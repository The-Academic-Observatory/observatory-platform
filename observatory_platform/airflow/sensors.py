# Copyright 2020, 2021 Curtin University
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

# Author: Tuan Chien, Keegan Smith, Jamie Diprose

from __future__ import annotations

from datetime import timedelta

import requests
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from airflow.sdk.bases.sensor import BaseSensorOperator


class DagCompleteSensor(BaseSensorOperator):
    """
    Waits until the most recent DAG run of `external_dag_id` reaches a terminal state.

    :param external_dag_id: the DAG ID of the external DAG to check.
    :param allowed_states: states that count as "complete" (default: ["success"]).
    :param failed_states: states that should immediately fail the sensor (default: ["failed"]).
    :param conn_id: Airflow connection pointing at this Deployment's Airflow API.
    """

    template_fields = ("external_dag_id",)

    def __init__(
        self,
        external_dag_id: str,
        allowed_states: list[str] | None = None,
        failed_states: list[str] | None = None,
        conn_id: str = "airflow_api",
        mode: str = "reschedule",
        poke_interval: int = 1200,
        timeout: int = int(timedelta(days=1).total_seconds()),
        **kwargs,
    ):
        super().__init__(mode=mode, poke_interval=poke_interval, timeout=timeout, **kwargs)
        self.external_dag_id = external_dag_id
        self.allowed_states = allowed_states or ["success"]
        self.failed_states = failed_states or ["failed"]
        self.conn_id = conn_id

    def poke(self, context) -> bool:
        conn = BaseHook.get_connection(self.conn_id)
        base_url = conn.host.rstrip("/")
        token = conn.password

        resp = requests.get(
            f"{base_url}/api/v2/dags/{self.external_dag_id}/dagRuns",
            headers={"Authorization": f"Bearer {token}"},
            params={"order_by": "-logical_date", "limit": 1},
            timeout=30,
        )
        resp.raise_for_status()
        dag_runs = resp.json()["dag_runs"]

        if not dag_runs:
            self.log.info("No dag runs found yet for %s", self.external_dag_id)
            return False

        latest = dag_runs[0]
        state = latest["state"]
        self.log.info(
            "Latest run of %s (logical_date=%s) is in state=%s",
            self.external_dag_id,
            latest["logical_date"],
            state,
        )

        if state in self.failed_states:
            raise AirflowException(f"Most recent run of {self.external_dag_id} failed (state={state})")

        return state in self.allowed_states
