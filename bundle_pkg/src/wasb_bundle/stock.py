"""
Stock ``WasbDagBundle`` behavior from Airflow main.

``apache-airflow-providers-microsoft-azure`` 15.2.1 is the latest release on
PyPI and does not ship ``WasbDagBundle`` or ``WasbHook.sync_to_local_dir``
yet. Both landed on main in apache/airflow#67016, still unreleased (the
checkout's changelog lists them under 15.3.0).

This class is that bundle: validate the container and prefix, then stage by
listing the prefix and downloading each blob on the calling thread. The loop
is the body of ``WasbHook.sync_to_local_dir`` on main, called through the
released hook (``list_blobs`` + ``get_file``).
"""

from __future__ import annotations

import os
from pathlib import Path

import structlog
from azure.core.exceptions import ResourceNotFoundError

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


class WasbDagBundle(BaseDagBundle):
    """
    WASB Dag bundle - exposes a directory in Azure Blob Storage as a Dag bundle.

    :param wasb_conn_id: Airflow connection ID for Azure Blob Storage.
    :param container_name: The blob container that holds the Dag files.
    :param prefix: Optional subdirectory within the container. Mutually
        exclusive with an archive blob on :class:`WasbArchiveDagBundle`.
    """

    supports_versioning = False

    def __init__(
        self,
        *,
        wasb_conn_id: str = WasbHook.default_conn_name,
        container_name: str,
        prefix: str = "",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.wasb_conn_id = wasb_conn_id
        self.container_name = container_name
        self.prefix = prefix
        self.wasb_dags_dir: Path = self.base_dir

        log = structlog.get_logger(__name__)
        self._log = log.bind(
            bundle_name=self.name,
            version=self.version,
            container_name=self.container_name,
            prefix=self.prefix,
            wasb_conn_id=self.wasb_conn_id,
        )
        self._wasb_hook: WasbHook | None = None

    def _initialize(self):
        with self.lock():
            if not self.wasb_dags_dir.exists():
                self._log.info("Creating local Dags directory: %s", self.wasb_dags_dir)
                os.makedirs(self.wasb_dags_dir)

            if not self.wasb_dags_dir.is_dir():
                raise NotADirectoryError(f"Local Dags path: {self.wasb_dags_dir} is not a directory.")

            try:
                exists = self._container_client().exists()
            except ResourceNotFoundError:
                exists = False
            if not exists:
                raise ValueError(f"WASB container '{self.container_name}' does not exist.")

            if self.prefix:
                prefix = f"{self.prefix.rstrip('/')}/"
                if not self.wasb_hook.check_for_prefix(container_name=self.container_name, prefix=prefix):
                    raise ValueError(
                        f"WASB prefix 'wasb://{self.container_name}/{self.prefix}' does not exist."
                    )
            self.refresh()

    def initialize(self) -> None:
        self._initialize()
        super().initialize()

    @property
    def wasb_hook(self) -> WasbHook:
        if self._wasb_hook is None:
            self._wasb_hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        return self._wasb_hook

    def _container_client(self):
        return self.wasb_hook.get_conn().get_container_client(self.container_name)

    def __repr__(self):
        return (
            f"<WasbDagBundle("
            f"name={self.name!r}, "
            f"container_name={self.container_name!r}, "
            f"prefix={self.prefix!r}, "
            f"version={self.version!r}"
            f")>"
        )

    def get_current_version(self) -> str | None:
        return None

    @property
    def path(self) -> Path:
        return self.wasb_dags_dir

    def refresh(self) -> None:
        if self.version:
            raise ValueError("Refreshing a specific version is not supported")

        with self.lock():
            self._log.debug(
                "Downloading Dags from wasb://%s/%s to %s",
                self.container_name,
                self.prefix,
                self.wasb_dags_dir,
            )
            self._sync_to_local_dir()

    def _sync_to_local_dir(self) -> None:
        """Serial per-blob staging, matching ``WasbHook.sync_to_local_dir`` on main."""
        local_dir = self.wasb_dags_dir
        local_dir_resolved = local_dir.resolve()
        prefix = self.prefix
        list_prefix = f"{prefix.rstrip('/')}/" if prefix else prefix
        container = self._container_client()

        local_wasb_objects: list[Path] = []
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
            self.wasb_hook.get_file(
                file_path=str(local_target_path),
                container_name=self.container_name,
                blob_name=blob.name,
            )
            local_wasb_objects.append(local_target_path)

        current = {path.resolve() for path in local_wasb_objects}
        for item in local_dir.rglob("*"):
            if item.is_file() and item.resolve() not in current:
                item.unlink()
