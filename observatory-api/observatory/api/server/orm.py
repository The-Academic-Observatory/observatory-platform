# Copyright 2021 Curtin University
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

# Author: James Diprose


from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, ClassVar, Dict, Union

import pendulum
from sqlalchemy import (
    Column,
    DateTime,
    JSON,
    Integer,
    String,
    create_engine,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

Base = declarative_base()
session_ = None  # Global session


def create_session(uri: str = os.environ.get("OBSERVATORY_DB_URI"), connect_args=None, poolclass=None):
    """Create an SQLAlchemy session.

    :param uri: the database URI.
    :param connect_args: connect arguments for SQLAlchemy.
    :param poolclass: what SQLAlchemy poolclass to use.
    :return: the SQLAlchemy session.
    """

    if uri is None:
        raise ValueError(
            "observatory.api.orm.create_session: please set the create_session `uri` parameter "
            "or the environment variable OBSERVATORY_DB_URI with a valid PostgreSQL workflow string"
        )

    if connect_args is None:
        connect_args = dict()

    engine = create_engine(uri, convert_unicode=True, connect_args=connect_args, poolclass=poolclass)
    s = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
    Base.query = s.query_property()
    Base.metadata.create_all(bind=engine)  # create all tables.

    return s


def set_session(session):
    """Set the SQLAlchemy session globally, within the orm module and the api module.

    :param session: the session to use.
    :return: None.
    """

    global session_
    session_ = session
    import observatory.api.server.api as api

    api.session_ = session


def fetch_db_object(cls: ClassVar, body: Any):
    """Fetch a database object via SQLAlchemy.

    :param cls: the class of object to fetch.
    :param body: the body of the object. If the body is None then None is returned (for the case where no object
    exists), if the body is already of type cls then the body is returned as the object and if the body is a dictionary
    with the key 'id' a query is made to fetch the given object.
    :return: the object.
    """

    if body is None:
        item = None
    elif isinstance(body, cls):
        item = body
    elif isinstance(body, Dict):
        if "id" not in body:
            raise AttributeError(f"id not found in {body}")

        id = body["id"]
        item = session_.query(cls).filter(cls.id == id).one_or_none()
        if item is None:
            raise ValueError(f"{item} with id {id} not found")
    else:
        raise ValueError(f"Unknown item type {body}")

    return item


def to_datetime_utc(obj: Union[None, pendulum.DateTime, str]) -> Union[pendulum.DateTime, None]:
    """Converts pendulum.DateTime into UTC object.

    :param obj: a pendulum.DateTime object (which will just be converted to UTC) or None which will be returned.
    :return: a DateTime object.
    """

    if isinstance(obj, pendulum.DateTime):
        return obj.in_tz(tz="UTC")
    elif isinstance(obj, str):
        dt = pendulum.parse(obj)
        return dt.in_tz(tz="UTC")
    elif obj is None:
        return None

    raise ValueError("body should be None or pendulum.DateTime")


@dataclass
class DatasetRelease(Base):
    __tablename__ = "dataset_release"

    id: int
    dag_id: str
    dataset_id: str
    dag_run_id: str
    data_interval_start: pendulum.DateTime
    data_interval_end: pendulum.DateTime
    snapshot_date: pendulum.DateTime
    changefile_start_date: pendulum.DateTime
    changefile_end_date: pendulum.DateTime
    sequence_start: int
    sequence_end: int
    extra: Dict
    created: pendulum.DateTime
    modified: pendulum.DateTime

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(250), nullable=False)
    dataset_id = Column(String(250), nullable=False)
    dag_run_id = Column(String(250), nullable=True)
    data_interval_start = Column(DateTime(), nullable=True)
    data_interval_end = Column(DateTime(), nullable=True)
    snapshot_date = Column(DateTime(), nullable=True)
    changefile_start_date = Column(DateTime(), nullable=True)
    changefile_end_date = Column(DateTime(), nullable=True)
    sequence_start = Column(Integer, nullable=True)
    sequence_end = Column(Integer, nullable=True)
    extra = Column(JSON(), nullable=True)
    created = Column(DateTime())
    modified = Column(DateTime())

    def __init__(
        self,
        id: int = None,
        dag_id: str = None,
        dataset_id: str = None,
        dag_run_id: str = None,
        data_interval_start: Union[pendulum.DateTime, str] = None,
        data_interval_end: Union[pendulum.DateTime, str] = None,
        snapshot_date: Union[pendulum.DateTime, str] = None,
        changefile_start_date: Union[pendulum.DateTime, str] = None,
        changefile_end_date: Union[pendulum.DateTime, str] = None,
        sequence_start: int = None,
        sequence_end: int = None,
        extra: dict = None,
        created: pendulum.DateTime = None,
        modified: pendulum.DateTime = None,
    ):
        """Construct a DatasetRelease object.

        :param id: unique id.
        :param dag_id: the DAG ID.
        :param dataset_id: the dataset ID.
        :param dag_run_id: the DAG's run ID.
        :param data_interval_start: the DAGs data interval start. Date is inclusive.
        :param data_interval_end: the DAGs data interval end. Date is exclusive.
        :param snapshot_date: the release date of the snapshot.
        :param changefile_start_date: the date of the first changefile processed in this release.
        :param changefile_end_date: the date of the last changefile processed in this release.
        :param sequence_start: the starting sequence number of files that make up this release.
        :param sequence_end: the end sequence number of files that make up this release.
        :param extra: optional extra field for storing any data.
        :param created: datetime created in UTC.
        :param modified: datetime modified in UTC.
        """

        self.id = id
        self.dag_id = dag_id
        self.dataset_id = dataset_id
        self.dag_run_id = dag_run_id
        self.data_interval_start = to_datetime_utc(data_interval_start)
        self.data_interval_end = to_datetime_utc(data_interval_end)
        self.snapshot_date = to_datetime_utc(snapshot_date)
        self.changefile_start_date = to_datetime_utc(changefile_start_date)
        self.changefile_end_date = to_datetime_utc(changefile_end_date)
        self.sequence_start = sequence_start
        self.sequence_end = sequence_end
        self.extra = extra
        self.created = to_datetime_utc(created)
        self.modified = to_datetime_utc(modified)

    def update(
        self,
        dag_id: str = None,
        dataset_id: str = None,
        dag_run_id: str = None,
        data_interval_start: Union[pendulum.DateTime, str] = None,
        data_interval_end: Union[pendulum.DateTime, str] = None,
        snapshot_date: Union[pendulum.DateTime, str] = None,
        changefile_start_date: Union[pendulum.DateTime, str] = None,
        changefile_end_date: Union[pendulum.DateTime, str] = None,
        sequence_start: int = None,
        sequence_end: int = None,
        extra: dict = None,
        modified: pendulum.DateTime = None,
    ):
        """Update the properties of an existing DatasetRelease object. This method is handy when you want to update
        the DatasetRelease from a dictionary, e.g. obj.update(**{'service': 'hello world'}).

        :param dag_id: the DAG ID.
        :param dataset_id: the dataset ID.
        :param dag_run_id: the DAG's run ID.
        :param data_interval_start: the DAGs data interval start. Date is inclusive.
        :param data_interval_end: the DAGs data interval end. Date is exclusive.
        :param snapshot_date: the release date of the snapshot.
        :param changefile_start_date: the date of the first changefile processed in this release.
        :param changefile_end_date: the date of the last changefile processed in this release.
        :param sequence_start: the starting sequence number of files that make up this release.
        :param sequence_end: the end sequence number of files that make up this release.
        :param extra: optional extra field for storing any data.
        :param modified: datetime modified in UTC.
        :return: None.
        """

        if dag_id is not None:
            self.dag_id = dag_id

        if dataset_id is not None:
            self.dataset_id = dataset_id

        if dag_run_id is not None:
            self.dag_run_id = dag_run_id

        if data_interval_start is not None:
            self.data_interval_start = to_datetime_utc(data_interval_start)

        if data_interval_end is not None:
            self.data_interval_end = to_datetime_utc(data_interval_end)

        if snapshot_date is not None:
            self.snapshot_date = to_datetime_utc(snapshot_date)

        if changefile_start_date is not None:
            self.changefile_start_date = to_datetime_utc(changefile_start_date)

        if changefile_end_date is not None:
            self.changefile_end_date = to_datetime_utc(changefile_end_date)

        if sequence_start is not None:
            self.sequence_start = sequence_start

        if sequence_end is not None:
            self.sequence_end = sequence_end

        if extra is not None:
            self.extra = extra

        if modified is not None:
            self.modified = to_datetime_utc(modified)
