"""
WasbArchiveDagBundle: stage an Airflow DAG bundle from ONE compressed archive
blob instead of downloading the container prefix one blob at a time.

The stock bundle refreshes with a serial per-blob loop. For a bundle of N
small files that costs one download per file on every fresh worker, which
under KubernetesExecutor means on every task start.

``archive_blob`` is mutually exclusive with ``prefix``. The archive is the
only source of the DAGs. A failed stage raises and leaves the previously
staged tree in place.
"""

from __future__ import annotations

import shutil
import tarfile
import tempfile
from pathlib import Path

from azure.core.exceptions import ResourceNotFoundError

from wasb_bundle.stock import WasbDagBundle

VERSION_MARKER = ".staged-archive-etag"


class WasbArchiveDagBundle(WasbDagBundle):
    """
    DAG bundle that stages from a single ``.tar.gz`` blob.

    :param archive_blob: Blob name of the archive
        (e.g. ``archive/dag-bundle.tar.gz``). Members are extracted relative
        to the bundle root, so pack the contents of the DAGs directory
        (``tar -C dags -czf dag-bundle.tar.gz .``).
    :param prefix: rejected. The stock bundle uses it as the per-blob
        staging source, which this bundle replaces rather than supplements.
    """

    def __init__(self, *, archive_blob: str, **kwargs) -> None:
        if kwargs.get("prefix"):
            raise ValueError(
                "WasbArchiveDagBundle takes either 'prefix' or 'archive_blob', not both: "
                "the archive is the only staging source."
            )
        super().__init__(**kwargs)
        self.archive_blob = archive_blob
        self._log = self._log.bind(archive_blob=self.archive_blob)

    def _initialize(self):
        with self.lock():
            if not self.wasb_dags_dir.exists():
                self.wasb_dags_dir.mkdir(parents=True, exist_ok=True)
            if not self.wasb_dags_dir.is_dir():
                raise NotADirectoryError(f"Local Dags path: {self.wasb_dags_dir} is not a directory.")
            try:
                exists = self._container_client().exists()
            except ResourceNotFoundError:
                exists = False
            if not exists:
                raise ValueError(f"WASB container '{self.container_name}' does not exist.")
            try:
                self._blob_client().get_blob_properties()
            except ResourceNotFoundError:
                raise ValueError(
                    f"WASB archive 'wasb://{self.container_name}/{self.archive_blob}' does not exist."
                )
            self.refresh()

    def refresh(self) -> None:
        if self.version:
            raise ValueError("Refreshing a specific version is not supported")
        with self.lock():
            self._stage_from_archive()

    def _blob_client(self):
        return self.wasb_hook.get_conn().get_blob_client(self.container_name, self.archive_blob)

    def _stage_from_archive(self) -> None:
        blob = self._blob_client()
        props = blob.get_blob_properties()
        token = props.etag or ""

        marker = self.wasb_dags_dir / VERSION_MARKER
        if token and marker.is_file() and marker.read_text() == token:
            self._log.debug("Archive unchanged (etag match), skipping staging")
            return

        parent = self.wasb_dags_dir.parent
        staging_dir = Path(tempfile.mkdtemp(dir=parent, prefix=".archive-staging-"))
        try:
            archive_path = staging_dir / "_bundle_archive"
            payload = blob.download_blob().readall()
            archive_path.write_bytes(payload)

            unpack_dir = staging_dir / "unpacked"
            unpack_dir.mkdir()
            with tarfile.open(archive_path, "r:*") as tar:
                tar.extractall(unpack_dir, filter="data")
            archive_path.unlink()

            if token:
                (unpack_dir / VERSION_MARKER).write_text(token)

            backup_dir = staging_dir / "previous"
            if self.wasb_dags_dir.exists():
                self.wasb_dags_dir.rename(backup_dir)
            try:
                unpack_dir.rename(self.wasb_dags_dir)
            except Exception:
                if backup_dir.exists() and not self.wasb_dags_dir.exists():
                    backup_dir.rename(self.wasb_dags_dir)
                raise

            self._log.info(
                "Staged DAG bundle from archive (1 GET instead of per-object sync)",
                archive_bytes=len(payload),
            )
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)
