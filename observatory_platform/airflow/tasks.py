# Copyright 2020-2023 Curtin University
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


from __future__ import annotations

import logging
from typing import Optional, List

import airflow
from airflow.decorators import task
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from observatory_platform.google.gcp import gcp_delete_disk, gcp_create_disk
from observatory_platform.google.gke import gke_create_volume, gke_delete_volume


@task
def check_dependencies(airflow_vars: Optional[List[str]] = None, airflow_conns: Optional[List[str]] = None, **context):
    """Checks if the given Airflow Variables and Connections exist.

    :param airflow_vars: the Airflow Variables to check exist.
    :param airflow_conns: the Airflow Connections to check exist.
    :return: None.
    """

    vars_valid = True
    conns_valid = True
    if airflow_vars:
        vars_valid = check_variables(*airflow_vars)
    if airflow_conns:
        conns_valid = check_connections(*airflow_conns)

    if not vars_valid or not conns_valid:
        raise AirflowNotFoundException("Required variables or connections are missing")


def check_variables(*variables):
    """Checks whether all given airflow variables exist.

    :param variables: name of airflow variable
    :return: True if all variables are valid
    """
    is_valid = True
    for name in variables:
        try:
            Variable.get(name)
        except AirflowNotFoundException:
            logging.error(f"Airflow variable '{name}' not set.")
            is_valid = False
    return is_valid


def check_connections(*connections):
    """Checks whether all given airflow connections exist.

    :param connections: name of airflow connection
    :return: True if all connections are valid
    """
    is_valid = True
    for name in connections:
        try:
            BaseHook.get_connection(name)
        except airflow.exceptions.AirflowNotFoundException:
            logging.error(f"Airflow connection '{name}' not set.")
            is_valid = False
    return is_valid


@task
def gke_create_storage(volume_name: str, volume_size: int, kubernetes_conn_id: str, **context):
    """Create storage on a GKE cluster.

    :param project_id: the Google Cloud project ID.
    :param zone: the Google Cloud zone.
    :param volume_name: the name of the volume.
    :param volume_size: the volume size.
    :param kubernetes_conn_id: the Kubernetes Airflow Connection ID.
    :param context: the Airflow context.
    :return: None.
    """

    # gcp_create_disk(project_id=project_id, zone=zone, disk_name=volume_name, disk_size_gb=volume_size)
    gke_create_volume(kubernetes_conn_id=kubernetes_conn_id, volume_name=volume_name, size_gi=volume_size)


@task
def gke_delete_storage(volume_name: str, kubernetes_conn_id: str, **context):
    """Delete storage on a GKE cluster.

    :param project_id: the Google Cloud project ID.
    :param zone: the Google Cloud zone.
    :param volume_name: the name of the volume.
    :param kubernetes_conn_id: the Kubernetes Airflow Connection ID.
    :param context: the Airflow context.
    :return: None.
    """

    gke_delete_volume(kubernetes_conn_id=kubernetes_conn_id, volume_name=volume_name)
    # gcp_delete_disk(project_id=project_id, zone=zone, disk_name=volume_name)
