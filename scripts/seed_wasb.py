#!/usr/bin/env python3
"""Create the container and upload both layouts straight to Azurite.

Runs inside the Airflow image, which has the blob SDK. Uploads use the
Azurite hostname, not the counting proxy, so seeding is not in the benchmark.
"""

from __future__ import annotations

from pathlib import Path

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

ROOT = Path("/usr/local/airflow")
BUILD = ROOT / "bundle_build"
CONTAINER = "dag-bundles"
# Microsoft's published Azurite development account. Not a real key.
CONNECTION = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://azurite:10000/devstoreaccount1;"
)


def main() -> None:
    dags = BUILD / "dags"
    archive = BUILD / "dag-bundle.tar.gz"
    if not dags.is_dir() or not archive.is_file():
        raise SystemExit("bundle_build/ is missing — run 'just generate' first")

    service = BlobServiceClient.from_connection_string(CONNECTION)
    try:
        service.create_container(CONTAINER)
    except ResourceExistsError:
        pass
    container = service.get_container_client(CONTAINER)

    files = sorted(path for path in dags.rglob("*") if path.is_file())
    for path in files:
        blob_name = str(path.relative_to(dags.parent)).replace("\\", "/")
        container.upload_blob(blob_name, path.read_bytes(), overwrite=True)
    container.upload_blob("archive/dag-bundle.tar.gz", archive.read_bytes(), overwrite=True)
    print(f"uploaded {len(files)} blobs plus archive/dag-bundle.tar.gz to container {CONTAINER}")


if __name__ == "__main__":
    main()
