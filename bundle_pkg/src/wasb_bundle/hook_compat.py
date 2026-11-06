"""
Hook methods the vendored bundle calls that provider 15.2.1 does not ship.

``WasbDagBundle`` at apache/airflow#75657 tip
``c853ba97b57118de4c18915a4337ea72bc939284`` calls
``WasbHook.check_for_container`` and ``WasbHook.sync_to_local_dir``. Those
methods are copied from ``hooks/wasb.py`` at that same commit and attached
only when the installed hook does not already define them. A later provider
that ships the methods keeps its own implementations.

The prefix sync deletes local files that are not in the remote listing. That
drops ``{wasb_dags_dir}/.archive_etag``. It does not unlink a sibling
``.{bundle name}/etag``.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from azure.storage.blob import ContainerClient

if TYPE_CHECKING:
    from azure.storage.blob._models import BlobProperties


def check_for_container(self, container_name: str) -> bool:
    """
    Check if a container exists on Azure Blob Storage.

    :param container_name: Name of the container.
    :return: True if the container exists, False otherwise.
    """
    container = self._get_container_client(container_name)
    self.check_for_variable_type("container", container, ContainerClient)
    return cast("ContainerClient", container).exists()


def _sync_to_local_dir_delete_stale_local_files(
    self, current_wasb_objects: list[Path], local_dir: Path
) -> None:
    current_wasb_keys = {key.resolve() for key in current_wasb_objects}

    for item in local_dir.rglob("*"):
        if item.is_file() and item.resolve() not in current_wasb_keys:
            self.log.debug("Deleting stale local file: %s", item)
            item.unlink()
    for root, dirs, _ in os.walk(local_dir, topdown=False):
        for d in dirs:
            dir_path = os.path.join(root, d)
            if not os.listdir(dir_path):
                self.log.debug("Deleting stale empty directory: %s", dir_path)
                os.rmdir(dir_path)


def _sync_to_local_dir_if_changed(
    self, container_name: str, blob: BlobProperties, local_target_path: Path
) -> None:
    should_download = False
    download_logs: list[str] = []
    download_log_params: list[Any] = []

    if not local_target_path.exists():
        should_download = True
        download_logs.append("Local file %s does not exist.")
        download_log_params.append(local_target_path)
    else:
        local_stats = local_target_path.stat()
        if blob.size != local_stats.st_size:
            should_download = True
            download_logs.append("Blob size (%s) and local file size (%s) differ.")
            download_log_params.extend([blob.size, local_stats.st_size])

        blob_last_modified = blob.last_modified
        if blob_last_modified and local_stats.st_mtime < blob_last_modified.timestamp():
            should_download = True
            download_logs.append("Blob last modified (%s) and local file last modified (%s) differ.")
            download_log_params.extend([blob_last_modified.timestamp(), local_stats.st_mtime])

    if should_download:
        self.get_file(
            file_path=str(local_target_path),
            container_name=container_name,
            blob_name=blob.name,
        )
        download_logs.append("Downloaded %s to %s")
        download_log_params.extend([blob.name, local_target_path.as_posix()])
        self.log.debug(" ".join(download_logs), *download_log_params)
    else:
        self.log.debug(
            "Local file %s is up-to-date with blob %s. Skipping download.",
            local_target_path.as_posix(),
            blob.name,
        )


def sync_to_local_dir(
    self,
    container_name: str,
    local_dir: Path,
    prefix: str | None = None,
    delete_stale: bool = True,
) -> None:
    """Download files from an Azure Blob Storage container to a local directory."""
    self.log.debug("Downloading data from wasb://%s/%s to %s", container_name, prefix, local_dir)

    local_wasb_objects: list[Path] = []
    container = self._get_container_client(container_name)
    self.check_for_variable_type("container", container, ContainerClient)
    container = cast("ContainerClient", container)

    local_dir_resolved = local_dir.resolve()
    list_prefix = f"{prefix.rstrip('/')}/" if prefix else prefix
    for blob in container.list_blobs(name_starts_with=list_prefix):
        if blob.name.endswith("/"):
            continue
        blob_path = Path(blob.name)
        if prefix:
            local_target_path = local_dir.joinpath(blob_path.relative_to(prefix))
        else:
            local_target_path = local_dir.joinpath(blob_path)
        if not local_target_path.resolve().is_relative_to(local_dir_resolved):
            raise ValueError(f"Blob name {blob.name!r} resolves outside local directory {local_dir}")
        if not local_target_path.parent.exists():
            local_target_path.parent.mkdir(parents=True, exist_ok=True)
            self.log.debug("Created local directory: %s", local_target_path.parent)
        self._sync_to_local_dir_if_changed(
            container_name=container_name, blob=blob, local_target_path=local_target_path
        )
        local_wasb_objects.append(local_target_path)

    if delete_stale:
        self._sync_to_local_dir_delete_stale_local_files(
            current_wasb_objects=local_wasb_objects, local_dir=local_dir
        )


def install() -> None:
    """Attach the missing hook methods. No-op when the provider already has them."""
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

    if not hasattr(WasbHook, "check_for_container"):
        WasbHook.check_for_container = check_for_container  # type: ignore[attr-defined]
    if not hasattr(WasbHook, "sync_to_local_dir"):
        WasbHook._sync_to_local_dir_delete_stale_local_files = (  # type: ignore[attr-defined]
            _sync_to_local_dir_delete_stale_local_files
        )
        WasbHook._sync_to_local_dir_if_changed = _sync_to_local_dir_if_changed  # type: ignore[attr-defined]
        WasbHook.sync_to_local_dir = sync_to_local_dir  # type: ignore[attr-defined]
