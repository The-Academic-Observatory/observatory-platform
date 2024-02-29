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

# Author: Tuan Chien

from __future__ import annotations

from datetime import timedelta, datetime
from typing import List
import logging

from airflow.models import DagRun
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.db import provide_session
from airflow.utils.state import State
from sqlalchemy.orm.scoping import scoped_session

from observatory_platform.airflow.airflow import is_first_dag_run


class PreviousDagRunSensor(ExternalTaskSensor):
    def __init__(
        self,
        dag_id: str,
        task_id: str = "wait_for_prev_dag_run",
        external_task_id: str = "dag_run_complete",
        allowed_states: List[str] = None,
        *args,
        **kwargs,
    ):
        """Custom ExternalTaskSensor designed to wait for a previous DAG run of the same DAG. This sensor also
        skips on the first DAG run, as the DAG hasn't run before.

        Add the following as the last tag of your DAG:
            DummyOperator(
                task_id=external_task_id,
            )

        :param dag_id: the DAG id of the DAG to wait for.
        :param task_id: the task id for this sensor.
        :param external_task_id: the task id to wait for.
        :param allowed_states: to override allowed_states.
        :param args: args for ExternalTaskSensor.
        :param kwargs: kwargs for ExternalTaskSensor.
        """

        if allowed_states is None:
            # sensor can skip a run if previous dag run skipped for some reason
            allowed_states = [
                State.SUCCESS,
                State.SKIPPED,
            ]

        super().__init__(
            task_id=task_id,
            external_dag_id=dag_id,
            external_task_id=external_task_id,
            allowed_states=allowed_states,
            *args,
            **kwargs,
        )

    @provide_session
    def poke(self, context, session=None):
        # Custom poke to allow the sensor to skip on the first DAG run

        ti = context["task_instance"]
        dag_run = context["dag_run"]

        if is_first_dag_run(dag_run):
            self.log.info("Skipping the sensor check on the first DAG run")
            ti.set_state(State.SKIPPED)
            return True

        return super().poke(context, session=session)


class DagCompleteSensor(ExternalTaskSensor):
    """
    A sensor that awaits the completion of an external dag by default. Wait functionality can be customised by
    providing a different execution_date_fn.

    The sensor checks for completion of a dag with "external_dag_id" on the logical date returned by the
    execution_date_fn.
    """

    def __init__(
        self,
        task_id,
        external_dag_id,
        mode="reschedule",
        poke_interval=int(1200),  # Check if dag run is ready every 20 minutes
        timeout=int(timedelta(days=1).total_seconds()),  # Sensor will fail after 1 day of waiting
        check_existence=True,
        # Custom date retrieval fn. Airflow expects a callable with the execution_date as an argument only.
        execution_date_fn=None,
        **kwargs,
    ):
        """
        :param task_id: the id of the sensor task to create
        :param external_dag_id: the id of the external dag to check
        :param mode: The mode of the scheduler. Can be reschedule or poke.
        :param poke_interval: how often to check if the external dag run is complete
        :param timeout: how long to check before the sensor fails
        :param check_existence: Whether to check that the provided dag_id exists
        :param execution_date_fn: The callable that returns a logical date to check for the provided dag_id
        """

        if not execution_date_fn:
            execution_date_fn = lambda dt: latest_execution_timedelta(dt, external_dag_id)

        super().__init__(
            task_id=task_id,
            external_dag_id=external_dag_id,
            mode=mode,
            poke_interval=poke_interval,
            timeout=timeout,
            check_existence=check_existence,
            execution_date_fn=execution_date_fn,
            **kwargs,
        )


@provide_session
def latest_execution_timedelta(
    data_interval_start: datetime, ext_dag_id: str, session: scoped_session = None, **context
) -> int:
    """
    Get the latest execution for a given external dag and returns its data_interval_start (logical date)

    :param ext_dag_id: The dag_id to get the latest execution date for.
    :return: The latest execution date in the window.
    """
    dagruns = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == ext_dag_id,
        )
        .all()
    )
    dates = [d.data_interval_start for d in dagruns]  # data_interval start is what ExternalTaskSensor checks
    dates.sort(reverse=True)

    if not len(dates):  # If no execution is found return the logical date for the Workflow
        logging.warn(f"No Executions found for dag id: {ext_dag_id}")
        return data_interval_start

    return dates[0]
