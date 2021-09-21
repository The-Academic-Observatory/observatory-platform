# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from datetime import datetime
from typing import cast

from airflow.models.dag import create_timetable
from airflow.timetables.base import DataInterval
from airflow.timetables.interval import CronDataIntervalTimetable, DeltaDataIntervalTimetable
from airflow.timetables.simple import NullTimetable, OnceTimetable


def infer_automated_data_interval(execution_date: datetime, schedule_interval: str) -> DataInterval:
    """Infer a data interval for a run against this DAG.
    This method is used to bridge runs created prior to AIP-39
    implementation, which do not have an explicit data interval. Therefore,
    this method only considers ``schedule_interval`` values valid prior to
    Airflow 2.2.
    DO NOT use this method is there is a known data interval.
    """
    timezone = execution_date.timezone
    timetable = create_timetable(schedule_interval, timezone)
    timetable_type = type(timetable)
    if issubclass(timetable_type, (NullTimetable, OnceTimetable)):
        return DataInterval.exact(timezone.coerce_datetime(execution_date))
    #start = timezone.coerce_datetime(execution_date)
    if issubclass(timetable_type, CronDataIntervalTimetable):
        end = cast(CronDataIntervalTimetable, timetable)._get_next(execution_date)
    elif issubclass(timetable_type, DeltaDataIntervalTimetable):
        end = cast(DeltaDataIntervalTimetable, timetable)._get_next(execution_date)
    else:
        raise ValueError(f"Not a valid timetable: {timetable!r}")
    return DataInterval(execution_date, end)
