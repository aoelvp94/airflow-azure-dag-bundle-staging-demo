#!/usr/bin/env python3
"""
Benchmark cold DAG-bundle staging: stock per-blob sync vs single archive.

Runs INSIDE an Airflow container (see `just bench`). Each round wipes the
bundle's local directory first, simulating a fresh worker pod. The archive
ETag marker lives inside that directory (``.archive_etag``), so the wipe
removes it and the next initialize stages again. A prefix refresh drops an
in-tree marker because it is not a remote blob. It does not unlink a sibling
``.{name}/etag``.
"""

from __future__ import annotations

import inspect
import json
import shutil
import statistics
import sys
import time
import urllib.request

from wasb_bundle import (
    WASB_ARCHIVE_DAG_BUNDLE_CLASSPATH,
    WASB_DAG_BUNDLE_CLASSPATH,
    WasbArchiveDagBundle,
    WasbDagBundle,
)

METRICS_URL = "http://wasb-proxy:10000/__metrics"
CONTAINER = "dag-bundles"
PREFIX = "dags/"
ARCHIVE_BLOB = "archive/dag-bundle.tar.gz"
ROUNDS = int(sys.argv[1]) if len(sys.argv) > 1 else 3
CLASSES = ("download", "list", "metadata", "container", "other")


def request_counts() -> dict[str, int]:
    with urllib.request.urlopen(METRICS_URL, timeout=5) as resp:
        payload = json.load(resp)
    return {key: int(payload.get(key, 0)) for key in ("total", *CLASSES)}


def diff(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    return {
        key: after.get(key, 0) - before.get(key, 0)
        for key in set(after) | set(before)
        if after.get(key, 0) != before.get(key, 0)
    }


def _wait_diff(before: dict[str, int]) -> dict[str, int]:
    deltas: dict[str, int] = {}
    for _ in range(15):
        deltas = diff(before, request_counts())
        if deltas.get("total"):
            break
        time.sleep(0.2)
    return deltas


def run_cold(make_bundle) -> tuple[float, dict[str, int], int]:
    bundle = make_bundle()
    shutil.rmtree(bundle.base_dir, ignore_errors=True)
    before = request_counts()
    t0 = time.perf_counter()
    bundle.initialize()
    elapsed = time.perf_counter() - t0
    deltas = _wait_diff(before)
    staged = sum(1 for path in bundle.path.rglob("*.py") if path.is_file())
    return elapsed, deltas, staged


def scenario(label: str, make_bundle) -> dict:
    timings, last_reqs, staged = [], {}, 0
    for i in range(ROUNDS):
        elapsed, reqs, staged = run_cold(make_bundle)
        timings.append(elapsed)
        last_reqs = reqs
        pretty = ", ".join(f"{key}={last_reqs.get(key, 0)}" for key in CLASSES if last_reqs.get(key))
        pretty = pretty or "no requests"
        print(f"  round {i + 1}: {elapsed:6.2f}s   ({pretty})")
    return {"label": label, "median": statistics.median(timings), "reqs": last_reqs, "staged": staged}


def assert_split() -> None:
    stock_params = inspect.signature(WasbDagBundle.__init__).parameters
    archive_params = inspect.signature(WasbArchiveDagBundle.__init__).parameters
    if "archive_blob" in stock_params or "prefix" not in stock_params:
        raise SystemExit(
            f"{WASB_DAG_BUNDLE_CLASSPATH} must be the prefix bundle, not an archive_blob flag"
        )
    if "prefix" in archive_params or "archive_blob" not in archive_params:
        raise SystemExit(
            f"{WASB_ARCHIVE_DAG_BUNDLE_CLASSPATH} must take archive_blob and not prefix"
        )
    try:
        WasbArchiveDagBundle(
            name="reject-prefix",
            container_name=CONTAINER,
            prefix=PREFIX,
            archive_blob=ARCHIVE_BLOB,
            wasb_conn_id="wasb_default",
        )
    except TypeError:
        pass
    else:
        raise SystemExit("WasbArchiveDagBundle accepted prefix")


def assert_archive_marker(bundle) -> None:
    marker = bundle.path / ".archive_etag"
    if not marker.is_file() or not marker.read_text():
        raise SystemExit(f"expected ETag marker inside the staged tree at {marker}")
    sibling = bundle.path.parent / f".{bundle.name}" / "etag"
    if sibling.exists():
        raise SystemExit(f"archive staging must not write a sibling ETag marker at {sibling}")


def assert_prefix_leaves_sibling_marker(make_bundle) -> None:
    """Prefix refresh drops the in-tree marker and leaves a sibling etag file."""
    bundle = make_bundle()
    sibling = bundle.path.parent / f".{bundle.name}" / "etag"
    sibling.parent.mkdir(parents=True, exist_ok=True)
    sibling.write_text("leftover")
    in_tree = bundle.path / ".archive_etag"
    in_tree.write_text("drop-me")
    bundle.refresh()
    if not sibling.is_file() or sibling.read_text() != "leftover":
        raise SystemExit(f"prefix refresh unlinked the sibling ETag marker at {sibling}")
    if in_tree.exists():
        raise SystemExit(f"prefix refresh left the in-tree marker at {in_tree}")
    sibling.unlink(missing_ok=True)
    if sibling.parent.is_dir() and not any(sibling.parent.iterdir()):
        sibling.parent.rmdir()
    print("  prefix refresh left the sibling etag in place and dropped .archive_etag")


def assert_warm_skips_download(make_bundle) -> None:
    bundle = make_bundle()
    before = request_counts()
    bundle.refresh()
    deltas = _wait_diff(before)
    pretty = ", ".join(f"{key}={deltas.get(key, 0)}" for key in CLASSES if deltas.get(key)) or "no requests"
    print(f"  warm refresh (ETag match, files present): download skipped ({pretty})")
    if deltas.get("download"):
        raise SystemExit(f"unchanged archive was downloaded again: {deltas}")
    if not deltas.get("metadata"):
        raise SystemExit(f"warm refresh did not read blob properties: {deltas}")


def main() -> None:
    print(f"stock classpath:   {WASB_DAG_BUNDLE_CLASSPATH}")
    print(f"archive classpath: {WASB_ARCHIVE_DAG_BUNDLE_CLASSPATH}")
    assert_split()
    print(f"\nCold-staging benchmark, {ROUNDS} rounds each (local dir wiped before every round)\n")

    print("[1/2] stock WasbDagBundle — serial per-blob sync")

    def make_stock():
        return WasbDagBundle(
            name="bench-per-object",
            container_name=CONTAINER,
            prefix=PREFIX,
            wasb_conn_id="wasb_default",
        )

    stock = scenario("per-object sync (stock WasbDagBundle)", make_stock)
    assert_prefix_leaves_sibling_marker(make_stock)

    print("\n[2/2] WasbArchiveDagBundle — single tar.gz blob")

    def make_archive():
        return WasbArchiveDagBundle(
            name="bench-archive",
            container_name=CONTAINER,
            archive_blob=ARCHIVE_BLOB,
            wasb_conn_id="wasb_default",
        )

    archive = scenario("single archive (WasbArchiveDagBundle)", make_archive)
    assert_archive_marker(make_archive())
    assert_warm_skips_download(make_archive)

    print(f"\n{'=' * 74}")
    print(f"{'scenario':<46}{'median':>9}{'blob reqs':>10}{'files':>8}")
    print(f"{'-' * 74}")
    for row in (stock, archive):
        reqs = row["reqs"].get("total", "n/a")
        print(f"{row['label']:<46}{row['median']:>8.2f}s{reqs!s:>10}{row['staged']:>8}")
    print(f"{'=' * 74}")
    if archive["median"] > 0:
        print(
            f"\nspeedup: {stock['median'] / archive['median']:.1f}x   "
            f"(every KubernetesExecutor task pod pays the cold-staging cost)"
        )
    if stock["staged"] != archive["staged"] or stock["staged"] == 0:
        raise SystemExit(
            f"staged file counts differ or are empty: stock={stock['staged']} archive={archive['staged']}"
        )


if __name__ == "__main__":
    main()
