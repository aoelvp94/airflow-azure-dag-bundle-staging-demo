# Airflow Azure Blob DAG bundle staging: per-blob sync vs single archive

A self-contained, fully local reproduction of the same gap as
[apache/airflow#73015](https://github.com/apache/airflow/pull/73015) and
[apache/airflow#75566](https://github.com/apache/airflow/pull/75566), on Azure
Blob Storage. **`WasbDagBundle` stages the bundle one blob at a time,
serially**, so a DAG project made of hundreds of small files pays one blob
round-trip per file on every fresh worker. Under KubernetesExecutor every
task instance is a fresh worker, which puts the full staging cost on the
critical path of every task start.

The other reproductions are
[aoelvp94/airflow-s3-dag-bundle-staging-demo](https://github.com/aoelvp94/airflow-s3-dag-bundle-staging-demo)
and
[aoelvp94/airflow-gcs-dag-bundle-staging-demo](https://github.com/aoelvp94/airflow-gcs-dag-bundle-staging-demo).

`WasbDagBundle` is on Airflow main
([#67016](https://github.com/apache/airflow/pull/67016)) and is **not** in the
latest provider release on PyPI (`apache-airflow-providers-microsoft-azure`
15.2.1). That release also has no `WasbHook.sync_to_local_dir`. This repo
includes the bundle class from main and the same serial loop (`list_blobs` +
one `get_file` per blob). `WasbArchiveDagBundle` changes only the staging
strategy. `archive_blob` is mutually exclusive with `prefix`. There is no
Airflow pull request for the archive option yet.

Everything runs on your machine: `astro dev` (Airflow 3.3, same runtime as
the other two reproductions) plus
[Azurite](https://github.com/Azure/Azurite), Microsoft's storage emulator.
**No Azure subscription, no cloud requests, nothing is uploaded anywhere.**
The account key in [`.env`](.env) is Azurite's published development account.

```
per-blob sync (stock)                    single archive (this PoC)

worker start                             worker start
  │                                        │
  ├─ GET  container                        ├─ GET  container
  ├─ LIST prefix                           ├─ HEAD archive etag
  ├─ LIST prefix                     ─┐    ├─ GET  archive.tar.gz     ─┐
  ├─ GET  file_001                    │    └─ unpack + rename swap    ─┘
  ├─ GET  file_002                    │slow
  ├─ ...                              │
  ├─ GET  file_400                   ─┘
  task code runs
```

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) (`brew install astro`)
- [just](https://github.com/casey/just) (`brew install just`)
- Python 3.10+ on the host (stdlib only, used to generate the bundle)

Host ports: Airflow UI `18081`, Postgres `15433`, Azurite `11000`, request
counter `11088`. Chosen so this project can run next to the S3 and GCS demos.

## Reproduce (one command)

```bash
just e2e
```

That runs, in order:

| step | recipe | what it does |
|---|---|---|
| 1 | `just generate` | builds `bundle_build/`: 400 small `.py` files (~3 MiB) **and** a `.tar.gz` of the same files |
| 2 | `just up` | `astro dev start`: Airflow 3 + Azurite + a counting proxy |
| 3 | `just seed` | creates container `dag-bundles` and uploads both layouts straight to Azurite, not through the counter |
| 4 | `just bench` | stops the DAG processor so it cannot add requests, then cold-stages both bundle classes 3× |

Each benchmark round deletes the bundle's local directory first. That is the
fresh-worker simulation. Measured on this machine (Airflow 3.3, provider
`apache-airflow-providers-microsoft-azure` 15.2.1, Azurite 3.35.0, everything
on localhost):

```
Cold-staging benchmark, 3 rounds each (local dir wiped before every round)

[1/2] stock WasbDagBundle — serial per-blob sync
  round 1:   1.57s   (download=400, list=2, container=1)
  round 2:   0.99s   (download=400, list=2, container=1)
  round 3:   1.08s   (download=400, list=2, container=1)

[2/2] WasbArchiveDagBundle — single tar.gz blob
  round 1:   0.29s   (download=1, metadata=2, container=1)
  round 2:   0.12s   (download=1, metadata=2, container=1)
  round 3:   0.09s   (download=1, metadata=2, container=1)

==========================================================================
scenario                                         median blob reqs   files
--------------------------------------------------------------------------
per-object sync (stock WasbDagBundle)             1.08s       403     400
single archive (WasbArchiveDagBundle)             0.12s         4     400
==========================================================================

speedup: 8.8x   (every KubernetesExecutor task pod pays the cold-staging cost)
```

The request mix did not move between rounds. Stock cold staging is 1
container GET, 2 lists (the prefix check, then the refresh), and 400 blob
downloads. The archive is 1 container GET, 2 metadata reads (blob exists,
then the etag), and 1 download.

Loopback Azurite makes each call a few milliseconds, so the wall-clock ratio
is a floor. Request count is the part that does not depend on the emulator.
Neither number is a cloud bill: nothing left the machine.

## Poke around

```bash
just logs    # how the DAG processor staged the two live bundles
just trace   # proxy log (errors only; counts are in the benchmark)
just bench 5 # more rounds
curl -s localhost:11088/__metrics
```

The Airflow UI is at http://localhost:18081. Both bundles (`wasb-per-object`
and `wasb-archive`) are configured in [`.env`](.env).

When you're done: `just down` (stop) or `just clean` (delete everything).

The DAG processor logs that the container or archive does not exist until
`just seed` has run. `just e2e` starts Airflow before seeding, so that window
is expected.

## Layout

```
bundle_pkg/            WasbDagBundle (from main) and WasbArchiveDagBundle
scripts/generate_bundle.py   synthetic bundle generator (host, stdlib only)
scripts/seed_wasb.py   upload both layouts straight to Azurite
scripts/wasb_proxy.py  counts blob requests, then forwards to Azurite
include/benchmark.py   in-container benchmark (timings + request counts)
docker-compose.override.yml  adds Azurite and the counting proxy
.env                   Azurite connection string and the two live bundles
justfile               all of the above, wired end to end
```

## Notes

- Members are extracted relative to the bundle root, so pack the *contents* of
  the DAGs directory (`tar -C dags .`) rather than the directory itself.
- The PoC extracts with `tarfile`'s `filter="data"` and swaps directories by
  rename. There is a short gap between the two renames where the directory
  does not exist. A failed swap puts the previous tree back.
- `prefix` and `archive_blob` are mutually exclusive. Delete the archive blob
  and rerun `just bench` to watch the archive bundle fail instead of staging
  from the per-blob prefix.
