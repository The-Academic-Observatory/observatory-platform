#  Copyright 2022 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# This is an ingredient file. It is not meant to be run directly. Check the samples/snippets
# folder for complete code samples that are ready to be used.

# Sources
# https://github.com/GoogleCloudPlatform/python-docs-samples

from __future__ import annotations

import sys
from typing import Any

from google.api_core.extended_operation import ExtendedOperation
from google.cloud import compute_v1


def gcp_create_disk(
    *, project_id: str, zone: str, disk_name: str, disk_size_gb: int = 10, disk_type: str = "pd-standard"
) -> compute_v1.Disk:
    """Creates a new empty disk in a project in given zone.

    Source: https://github.com/GoogleCloudPlatform/python-docs-samples/blob/main/compute/client_library/ingredients/disks/create_empty_disk.py

    :param project_id: project ID or project number of the Cloud project you want to use.
    :param zone: name of the zone in which you want to create the disk.
    :param disk_name: name of the disk you want to create.
    :param disk_size_gb: size of the new disk in gigabytes.
    :param disk_type: the type of disk you want to create. This value uses the following format:
    "zones/{zone}/diskTypes/(pd-standard|pd-ssd|pd-balanced|pd-extreme)".
    For example: "zones/us-west3-b/diskTypes/pd-ssd"
    :return: An unattached Disk instance.
    """

    disk = compute_v1.Disk()
    disk.name = disk_name
    disk.size_gb = disk_size_gb
    disk.zone = zone
    disk.type_ = f"zones/{zone}/diskTypes/{disk_type}"

    disk_client = compute_v1.DisksClient()
    operation = disk_client.insert(project=project_id, zone=zone, disk_resource=disk)

    wait_for_extended_operation(operation, "disk creation")

    return disk_client.get(project=project_id, zone=zone, disk=disk.name)


def gcp_delete_disk(*, project_id: str, zone: str, disk_name: str) -> None:
    """Deletes a disk from a project.

    Source: https://github.com/GoogleCloudPlatform/python-docs-samples/blob/main/compute/client_library/ingredients/disks/delete.py

    :param project_id: project ID or project number of the Cloud project you want to use.
    :param zone: name of the zone in which is the disk you want to delete.
    :param disk_name: name of the disk you want to delete.
    :return: None.
    """

    disk_client = compute_v1.DisksClient()
    operation = disk_client.delete(project=project_id, zone=zone, disk=disk_name)
    wait_for_extended_operation(operation, "disk deletion")


def wait_for_extended_operation(
    operation: ExtendedOperation, verbose_name: str = "operation", timeout: int = 300
) -> Any:
    """Waits for the extended (long-running) operation to complete.

    If the operation is successful, it will return its result.
    If the operation ends with an error, an exception will be raised.
    If there were any warnings during the execution of the operation
    they will be printed to sys.stderr.

    Source: https://github.com/GoogleCloudPlatform/python-docs-samples/blob/main/compute/client_library/snippets/operations/wait_for_extended_operation.py

    :param operation: a long-running operation you want to wait on.
    :param verbose_name: (optional) a more verbose name of the operation,
    used only during error and warning reporting.
    :param timeout: how long (in seconds) to wait for operation to finish. If None, wait indefinitely.
    :return: Whatever the operation.result() returns.
    :raises: concurrent.futures.TimeoutError: in the case of an operation taking longer than `timeout` seconds to complete.
    :raises: RuntimeError: if there is no exception set, but there is an `error_code` set for the `operation`.
    :raises: operation.exception(): will raise the exception received from operation.exception()
    """

    result = operation.result(timeout=timeout)

    if operation.error_code:
        print(
            f"Error during {verbose_name}: [Code: {operation.error_code}]: {operation.error_message}",
            file=sys.stderr,
            flush=True,
        )
        print(f"Operation ID: {operation.name}", file=sys.stderr, flush=True)
        raise operation.exception() or RuntimeError(operation.error_message)

    if operation.warnings:
        print(f"Warnings during {verbose_name}:\n", file=sys.stderr, flush=True)
        for warning in operation.warnings:
            print(f" - {warning.code}: {warning.message}", file=sys.stderr, flush=True)

    return result
