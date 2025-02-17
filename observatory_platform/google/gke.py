# Copyright 2023-2024 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import re
from dataclasses import dataclass
from typing import Optional

import kubernetes
import requests
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from kubernetes import client
from kubernetes.client import models as k8s
from kubernetes.client.models import V1ResourceRequirements

DEFAULT_GKE_IMAGE = "us-docker.pkg.dev/academic-observatory/academic-observatory/academic-observatory:latest"


@dataclass
class GkeParams:
    """Parameters describing the use of Google Kubernetes Engine"""

    def __init__(
        self,
        gke_namespace: str,
        gke_volume_name: str,
        gke_volume_size: str,
        gke_volume_path: str = "/data",
        gke_volume_storage_class: str = "standard",
        gke_image: str = DEFAULT_GKE_IMAGE,
        gke_zone: str = "us-central1",
        gke_startup_timeout_seconds: int = 300,
        gke_conn_id: str = "gke_cluster",
        docker_astro_uid: int = 50000,
        gke_resource_overrides: Optional[dict] = None,
    ):
        """
        :param gke_namespace: The cluster namespace to use.
        :param gke_volume_name: The name of the persistent volume to create
        :param gke_volume_size: The amount of storage to request for the persistent volume, units must be supplied, e.g. 500Mi, 1Gi etc.
        :param gke_volume_path: Where to mount the persistent volume.
        :param gke_volume_storage_class: what storage class to use when creating the volume. Valid options and how they
        map to Google Compute Engine (inside brackets) include: premium-rwo (pd-ssd), standard (pd-standard),
        standard-rwo (pd-balanced). See here for more details about the Compute Engine Disk types: https://cloud.google.com/kubernetes-engine/docs/concepts/storage-overview#why-pd
        :param gke_image: The image location to pull from.
        :param gke_zone: The zone containing the gke cluster
        :param gke_startup_timeout_seconds: How long to wait for the container to start in seconds.
        :param gke_conn_id: The name of the airlfow connection storing the gke cluster information.
        :param docker_astro_uuid: The uuid of the astro user
        :param gke_resource_overrides: Task resource overrides"""

        self.gke_namespace = gke_namespace
        self.gke_volume_name = gke_volume_name
        self.gke_volume_size = gke_volume_size
        self.gke_volume_path = gke_volume_path
        self.gke_volume_storage_class = gke_volume_storage_class
        self.gke_image = gke_image
        self.gke_zone = gke_zone
        self.gke_startup_timeout_seconds = gke_startup_timeout_seconds
        self.gke_conn_id = gke_conn_id
        self.docker_astro_uid = docker_astro_uid
        self.gke_resource_overrides = gke_resource_overrides
        if not gke_resource_overrides:
            self.gke_resource_overrides = {}

    @property
    def kubernetes_task_params(self):
        return gke_make_kubernetes_task_params(self)


def gke_make_kubernetes_task_params(gke_params: GkeParams):
    """Creates the kubernetes task parameters that are handed to each task k8s task

    :param gke_params: The gke_params object
    """

    volume_mounts = [k8s.V1VolumeMount(mount_path=gke_params.gke_volume_path, name=gke_params.gke_volume_name)]
    volumes = [
        k8s.V1Volume(
            name=gke_params.gke_volume_name,
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=gke_params.gke_volume_name),
        )
    ]
    return dict(
        image=gke_params.gke_image,
        security_context=k8s.V1PodSecurityContext(
            fs_group=gke_params.docker_astro_uid,
            fs_group_change_policy="OnRootMismatch",
            run_as_group=gke_params.docker_astro_uid,
            run_as_user=gke_params.docker_astro_uid,
        ),
        do_xcom_push=True,
        get_logs=True,
        in_cluster=False,
        kubernetes_conn_id=gke_params.gke_conn_id,
        log_events_on_failure=True,
        namespace=gke_params.gke_namespace,
        startup_timeout_seconds=gke_params.gke_startup_timeout_seconds,
        env_vars={"DATA_PATH": gke_params.gke_volume_path},
        volumes=volumes,
        volume_mounts=volume_mounts,
        init_containers=[
            k8s.V1Container(
                name="init-container",
                image="ubuntu",
                command=[
                    "sh",
                    "-c",
                    f"useradd -u {gke_params.docker_astro_uid} astro && chown -R astro:astro {gke_params.gke_volume_path}",
                ],
                volume_mounts=volume_mounts,
                security_context=k8s.V1PodSecurityContext(fs_group=0, run_as_group=0, run_as_user=0),
                resources=V1ResourceRequirements(
                    requests={"cpu": "1", "memory": "1G"}, limits={"cpu": "1", "memory": "1G"}
                ),
            )
        ],
    )


def gke_make_container_resources(default: dict, override: Optional[dict]) -> V1ResourceRequirements:
    """Creates the container resources object. Takes an optional override.

    :param default: The default dictionary for resources. e.g. {"memory": "2G", "cpu": "2"}
    :param override: If supplied, ignore the default and use this resource allocation instead
    """
    resource = default
    if override is not None:
        resource = override
    return V1ResourceRequirements(requests=resource, limits=resource)


def gke_create_volume(*, kubernetes_conn_id: str, volume_name: str, size: str, storage_class: str = "standard") -> None:
    """Creates a GKE volume

    :param kubernetes_conn_id:
    :param volume_name:
    :param size: size with units, e.g. 500Mi, 1Gi.
    """
    if not re.match(r"^\d+[KMGT]i$", size):
        raise ValueError("Size must be in format like '500Mi', '1Gi'")

    logging.info("Creating persistent storage volume to the following specification:")
    logging.info(f"Volume name: {volume_name}")
    logging.info(f"Volume size: {size}")
    logging.info(f"Storage class: {storage_class}")

    # Make Kubernetes API Client from Airflow Connection
    hook = KubernetesHook(conn_id=kubernetes_conn_id)
    api_client = hook.get_conn()
    v1 = client.CoreV1Api(api_client=api_client)

    # Create PersistentVolumeClaim
    namespace = hook.get_namespace()
    capacity = {"storage": size}
    pvc = client.V1PersistentVolumeClaim(
        api_version="v1",
        kind="PersistentVolumeClaim",
        metadata=client.V1ObjectMeta(name=volume_name),
        spec=client.V1PersistentVolumeClaimSpec(
            access_modes=["ReadWriteOnce"],
            resources=client.V1ResourceRequirements(requests=capacity),
            storage_class_name=storage_class,
        ),
    )
    try:
        v1.create_namespaced_persistent_volume_claim(namespace=namespace, body=pvc)
    except kubernetes.client.exceptions.ApiException as e:
        if e.status == 409:
            logging.info(
                f"gke_create_volume: PersistentVolumeClaim with name={volume_name}, namespace={namespace} already exists"
            )
        else:
            raise e


def gke_delete_volume(*, kubernetes_conn_id: str, volume_name: str) -> None:
    """Deletes a GKE volume

    :param kubernetes_conn_id:
    :param namespace:
    :param volume_name:
    """

    # Make Kubernetes API Client from Airflow Connection
    hook = KubernetesHook(conn_id=kubernetes_conn_id)
    api_client = hook.get_conn()
    v1 = client.CoreV1Api(api_client=api_client)

    # Delete VolumeClaim
    namespace = hook.get_namespace()
    try:
        v1.delete_namespaced_persistent_volume_claim(name=volume_name, namespace=namespace)
    except kubernetes.client.exceptions.ApiException as e:
        if e.status == 404:
            logging.info(
                f"gke_delete_volume: PersistentVolumeClaim with name={volume_name}, namespace={namespace} does not exist"
            )
        else:
            raise e


def gke_service_account_email() -> str | None:
    """Retrieves the service account email from the internal gke node

    :return: The service account email if it exists"""
    metadata_url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email"
    headers = {"Metadata-Flavor": "Google"}

    try:
        response = requests.get(metadata_url, headers=headers)
        if response.status_code == 200:
            return response.text
        else:
            print(f"Error: Received status code {response.status_code} from metadata server.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error accessing metadata server: {e}")
        return None
