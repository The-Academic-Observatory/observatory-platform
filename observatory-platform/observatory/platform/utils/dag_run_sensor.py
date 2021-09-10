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


import datetime
import os
from time import sleep
from typing import Dict, List, Union

from airflow.exceptions import AirflowException
from airflow.models import DagModel, DagRun
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.session import provide_session
from airflow.utils.state import State
from sqlalchemy.orm.scoping import scoped_session


class DagRunSensor(BaseSensorOperator):
    """
    A sensor for monitoring dag runs from other DAGs.
    Behaviour:
    * If the DAG ID does not exist, throw an exception.
    * Monitors the time period [execution_date - duration, execution_date] for the DAG's dag runs.
      - If there are no runs, then return a success state.
      - If there are runs, look at the latest run in the time period:
        * If the latest run is successful, return a success state.
        * If the latest run is unsuccessful, return a retry state.
    Like other sensors, you can set the poke_interval and timeout in seconds, to control retry
    behaviour.
    """

    def __init__(
        self,
        *,
        external_dag_id: str,
        duration: datetime.timedelta,
        check_exists: bool = True,
        grace_period: datetime.timedelta = datetime.timedelta(minutes=10),
        **kwargs,
    ):
        """
        :param external_dag_id: The DAG ID to monitor.
        :param duration: Size of the window to look back from the current execution date.
        :param check_exists: Whether to perform check for dag existence.
        :param grace_period: If no dag run can be found, sleep for this short grace period and try again.
        :param kwargs: Pass the rest of the parameters to the ExternalTaskSensor.
        """
        super().__init__(**kwargs)

        self.duration = duration
        self.external_dag_id = external_dag_id
        self.check_exists = check_exists
        self.grace_period = grace_period

    @provide_session
    def poke(self, context: Dict, session: scoped_session = None):
        """Sensor interface for determining status of the sensor.

        :param context: Poke context.
        :param session: Database session.
        :return: Sensor poke response.
        """

        success_state = True
        retry_state = False

        if self.check_exists:
            self.check_dag_exists(session)
        execution_date = context["execution_date"]

        date = self.get_latest_execution_date(session=session, execution_date=execution_date)

        # If no execution_date could be found, sleep the grace period, and try again once.This will sleep the duration
        # of the grace period and try again. This is an alternative to scheduling the workflow at a slightly later time
        # to allow Airflow to record the dagrun in the database.
        # Note that this occupies a slot in execution queue.
        if date is None:
            sleep(self.grace_period.total_seconds())
            date = self.get_latest_execution_date(session=session, execution_date=execution_date)

        if not date:
            return success_state

        task_done = self.is_dagrun_done(date=date, session=session)

        if task_done:
            return success_state

        return retry_state

    def get_latest_execution_date(
        self, *, session: scoped_session, execution_date: datetime.datetime
    ) -> Union[datetime.datetime, None]:
        """Get the most recent execution date for a task in a given DAG.

        :param sesssion: Db session.
        :param execution_date: Current execution date.
        :return: Most recent execution date in the window.
        """

        end_date = execution_date
        start_date = end_date - self.duration

        response = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == self.external_dag_id,
                DagRun.execution_date.between(start_date, end_date),
            )
            .all()
        )

        dates = [r.execution_date for r in response]
        dates.sort(reverse=True)

        if len(dates) > 0:
            return dates[0]

        return None

    def check_dag_exists(self, session: scoped_session):
        """Check if the DAG exists in the DB.  If they do not, then throw an exception.

        :param session: Database session.
        """

        dag_model = session.query(DagModel).filter(DagModel.dag_id == self.external_dag_id).first()
        if dag_model is None or not os.path.exists(dag_model.fileloc):
            raise AirflowException(f"DAG: {self.external_dag_id} does not exist.")

    def is_dagrun_done(self, *, date: datetime.datetime, session: scoped_session) -> bool:
        """Check if the dagrun has finished executing at a given execution date.
        :param date: execution date.
        :return: True if finished, False otherwise.
        """

        success_states = [State.SUCCESS]
        response = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == self.external_dag_id,
                DagRun.execution_date.in_([date]),
                DagRun.state.in_(success_states),
            )
            .first()
        )

        if response is not None:
            return True

        return False
