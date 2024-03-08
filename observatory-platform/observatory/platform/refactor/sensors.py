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
from functools import partial
from typing import Callable, List, Optional

import pendulum
from airflow.models import DagRun
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.db import provide_session
from sqlalchemy.orm.scoping import scoped_session


class DagCompleteSensor(ExternalTaskSensor):
    """
    A sensor that awaits the completion of an external dag by default. Wait functionality can be customised by
    providing a different execution_date_fn.

    The sensor checks for completion of a dag with "external_dag_id" on the logical date returned by the
    execution_date_fn.
    """

    def __init__(
        self,
        task_id: str,
        external_dag_id: str,
        mode: str = "reschedule",
        poke_interval: int = 1200,  # Check if dag run is ready every 20 minutes
        timeout: int = int(timedelta(days=1).total_seconds()),  # Sensor will fail after 1 day of waiting
        check_existence: bool = True,
        execution_date_fn: Optional[Callable] = None,
        **kwargs,
    ):
        """
        :param task_id: the id of the sensor task to create
        :param external_dag_id: the id of the external dag to check
        :param mode: The mode of the scheduler. Can be reschedule or poke.
        :param poke_interval: how often to check if the external dag run is complete
        :param timeout: how long to check before the sensor fails
        :param check_existence: whether to check that the provided dag_id exists
        :param execution_date_fn: a function that returns the logical date(s) of the external DAG runs to query for,
        since you need a logical date and a DAG ID to find a particular DAG run to wait for.
        """

        if execution_date_fn is None:
            execution_date_fn = partial(get_logical_dates, external_dag_id)

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
def get_logical_dates(
    external_dag_id: str, logical_date: pendulum.DateTime, session: scoped_session = None, **context
) -> List[pendulum.DateTime]:
    """Get the logical dates for a given external dag that fall between and returns its data_interval_start (logical date)

    :param external_dag_id: the DAG ID of the external DAG we are waiting for.
    :param logical_date: the logic date of the waiting DAG.
    :param session: the SQL Alchemy session.
    :param context: the Airflow context.
    :return: the last logical date of the external DAG that falls before the data interval end of the waiting DAG.
    """

    data_interval_end = context["data_interval_end"]
    dag_runs = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == external_dag_id,
            DagRun.data_interval_end <= data_interval_end,
        )
        .all()
    )
    dates = [d.logical_date for d in dag_runs]
    dates.sort(reverse=True)

    # If more than 1 date return first date
    if len(dates) >= 2:
        dates = [dates[0]]

    return dates
