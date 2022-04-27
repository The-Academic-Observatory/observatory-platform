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
import pendulum
from dataclasses import dataclass
from sqlalchemy import (
    JSON,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    BigInteger,
    String,
    create_engine,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relation, relationship, scoped_session, sessionmaker
from typing import Any, ClassVar, Dict, List, Union

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
class Organisation(Base):
    __tablename__ = "organisation"

    id: int
    name: str
    project_id: str
    download_bucket: str
    transform_bucket: str
    created: pendulum.DateTime
    modified: pendulum.DateTime

    id = Column(Integer, primary_key=True)
    name = Column(String(250))
    project_id = Column(String(30))
    download_bucket = Column(String(222))
    transform_bucket = Column(String(222))
    created = Column(DateTime())
    modified = Column(DateTime())
    workflows = relationship("Workflow", backref="organisation")

    def __init__(
        self,
        id: int = None,
        name: str = None,
        project_id: str = None,
        download_bucket: str = None,
        transform_bucket: str = None,
        created: pendulum.DateTime = None,
        modified: pendulum.DateTime = None,
    ):
        """Construct an Organisation object, which contains information about what Google Cloud project an
        organisation uses, what are it's download and transform buckets and what workflows does it have.

        The maximum lengths of the project_id, download_bucket and transform_bucket come from the following
        documentation:
        * https://cloud.google.com/resource-manager/docs/creating-managing-projects
        * https://cloud.google.com/storage/docs/naming-buckets


        :param id: unique id.
        :param name: the name.
        :param project_id: the Google Cloud project id.
        :param download_bucket: the Google Cloud Storage download bucket name for the above project.
        :param transform_bucket: the Google Cloud Storage transform bucket name for the above project.
        :param created: datetime created in UTC.
        :param modified: datetime modified in UTC.
        """

        self.id = id
        self.name = name
        self.project_id = project_id
        self.download_bucket = download_bucket
        self.transform_bucket = transform_bucket
        self.created = to_datetime_utc(created)
        self.modified = to_datetime_utc(modified)

    def update(
        self,
        name: str = None,
        project_id: str = None,
        download_bucket: str = None,
        transform_bucket: str = None,
        modified: pendulum.DateTime = None,
    ):
        """Update the properties of an existing Organisation object. This method is handy when you want to update
        the Organisation from a dictionary, e.g. obj.update(**{'name': 'hello world'}).

        :param name: the name.
        :param project_id: the Google Cloud project id.
        :param download_bucket: the Google Cloud Storage download bucket name for the above project.
        :param transform_bucket: the Google Cloud Storage transform bucket name for the above project.
        :param modified: datetime modified in UTC.
        :return: None.
        """

        if name is not None:
            self.name = name

        if project_id is not None:
            self.project_id = project_id

        if download_bucket is not None:
            self.download_bucket = download_bucket

        if transform_bucket is not None:
            self.transform_bucket = transform_bucket

        if modified is not None:
            self.modified = to_datetime_utc(modified)


@dataclass
class WorkflowType(Base):
    __tablename__ = "workflow_type"

    id: int
    type_id: str
    name: str
    created: pendulum.DateTime
    modified: pendulum.DateTime

    id = Column(Integer, primary_key=True)
    type_id = Column(String(250), unique=True, nullable=False)
    name = Column(String(250))
    created = Column(DateTime())
    modified = Column(DateTime())
    workflows = relationship("Workflow", backref="workflow_type")

    def __init__(
        self,
        id: int = None,
        type_id: str = None,
        name: str = None,
        created: pendulum.DateTime = None,
        modified: pendulum.DateTime = None,
    ):
        """Construct a WorkflowType object.

        :param id: unique id.
        :param type_id: a unique string id for the workflow type.
        :param name: the name.
        :param created: datetime created in UTC.
        :param modified: datetime modified in UTC.
        """

        self.id = id
        self.type_id = type_id
        self.name = name
        self.created = to_datetime_utc(created)
        self.modified = to_datetime_utc(modified)

    def update(self, type_id: str = None, name: str = None, modified: pendulum.DateTime = None):
        """Update the properties of an existing WorkflowType object. This method is handy when you want to update
        the WorkflowType from a dictionary, e.g. obj.update(**{'name': 'hello world'}).

        :param name: the name of the WorkflowType.
        :param type_id: a unique string id for the workflow type.
        :param modified: datetime modified in UTC.
        :return: None.
        """

        if type_id is not None:
            self.type_id = type_id

        if name is not None:
            self.name = name

        if modified is not None:
            self.modified = to_datetime_utc(modified)


@dataclass
class Workflow(Base):
    __tablename__ = "workflow"

    # Only include should be serialized to JSON as dataclass attributes
    id: int
    name: str
    extra: Dict
    tags: str
    created: pendulum.DateTime
    modified: pendulum.DateTime
    workflow_type: WorkflowType = None
    organisation: Organisation = None

    id = Column(Integer, primary_key=True)
    name = Column(String(250))
    extra = Column(JSON())
    tags = Column(String(250), nullable=True)
    created = Column(DateTime())
    modified = Column(DateTime())
    organisation_id = Column(Integer, ForeignKey(f"{Organisation.__tablename__}.id"), nullable=True)
    workflow_type_id = Column(Integer, ForeignKey(f"{WorkflowType.__tablename__}.id"), nullable=False)
    datasets = relationship("Dataset", backref=__tablename__)

    def __init__(
        self,
        id: int = None,
        name: str = None,
        extra: Dict = None,
        tags: str = None,
        created: pendulum.DateTime = None,
        modified: pendulum.DateTime = None,
        organisation: Union[Organisation, Dict] = None,
        workflow_type: Union[WorkflowType, Dict] = None,
    ):
        """Construct a Workflow object.

        :param id: unique id.
        :param name: the workflow name.
        :param extra: additional metadata for a workflow, stored as JSON.
        :param tags: List of tags, stored as JSON list string.
        :param created: datetime created in UTC.
        :param modified: datetime modified in UTC.
        :param organisation: the organisation associated with this workflow.
        :param workflow_type: the workflow type associated with this workflow.
        """

        self.id = id
        self.name = name
        self.extra = extra
        self.tags = tags
        self.created = to_datetime_utc(created)
        self.modified = to_datetime_utc(modified)

        # Fetch organisation and workflow type from database if it exists
        self.organisation = fetch_db_object(Organisation, organisation)
        self.workflow_type = fetch_db_object(WorkflowType, workflow_type)

    def update(
        self,
        name: str = None,
        extra: Dict = None,
        tags: str = None,
        modified: pendulum.DateTime = None,
        organisation: Union[Organisation, Dict] = None,
        workflow_type: Union[WorkflowType, Dict] = None,
    ):
        """Update the properties of an existing Workflow object. This method is handy when you want to update
        the Workflow from a dictionary, e.g. obj.update(**{'modified': datetime.utcnow()}).

        :param name: the workflow name.
        :param extra: additional metadata for a workflow, stored as JSON.
        :param tags: list of tags, stored as JSON list string.
        :param modified: datetime modified in UTC.
        :param organisation: the organisation associated with this workflow.
        :param workflow_type: the workflow type associated with this workflow.
        :return: None.
        """

        if name is not None:
            self.name = name

        if extra is not None:
            self.extra = extra

        if tags is not None:
            self.tags = tags

        if organisation is not None:
            self.organisation = fetch_db_object(Organisation, organisation)

        if workflow_type is not None:
            self.workflow_type = fetch_db_object(WorkflowType, workflow_type)

        if modified is not None:
            self.modified = to_datetime_utc(modified)


@dataclass
class TableType(Base):
    __tablename__ = "table_type"

    id: int
    type_id: str
    name: str
    created: pendulum.DateTime
    modified: pendulum.DateTime

    id = Column(Integer, primary_key=True)
    type_id = Column(String(250), unique=True, nullable=False)
    name = Column(String(250))
    created = Column(DateTime())
    modified = Column(DateTime())

    dataset_types = relationship("DatasetType", backref=__tablename__)

    def __init__(
        self,
        id: int = None,
        type_id: str = None,
        name: str = None,
        created: pendulum.DateTime = None,
        modified: pendulum.DateTime = None,
    ):
        """Construct a TableType object.

        :param id: unique id.
        :param type_id: the table type id.
        :param name: the table type name.
        :param created: datetime created in UTC.
        :param modified: datetime modified in UTC.
        """

        self.id = id
        self.name = name
        self.type_id = type_id
        self.created = to_datetime_utc(created)
        self.modified = to_datetime_utc(modified)

    def update(
        self,
        type_id: str = None,
        name: str = None,
        modified: pendulum.DateTime = None,
    ):
        """Update the properties of an existing TableType object. This method is handy when you want to update
        the Dataset from a dictionary, e.g. obj.update(**{'modified': datetime.utcnow()}).

        :param type_id: the table type id.
        :param name: the dataset name.
        :param modified: datetime modified in UTC.
        :return: None.
        """

        if type_id is not None:
            self.type_id = type_id

        if name is not None:
            self.name = name

        if modified is not None:
            self.modified = to_datetime_utc(modified)


@dataclass
class DatasetType(Base):
    __tablename__ = "dataset_type"

    # Only include should be serialized to JSON as dataclass attributes
    id: int
    type_id: str
    name: str
    extra: dict
    created: pendulum.DateTime
    modified: pendulum.DateTime
    table_type: TableType = None

    id = Column(Integer, primary_key=True)
    type_id = Column(String(250), unique=True, nullable=False)
    name = Column(String(250))
    extra = Column(JSON())
    created = Column(DateTime())
    modified = Column(DateTime())

    table_type_id = Column(Integer, ForeignKey(f"{TableType.__tablename__}.id"), nullable=False)
    datasets = relationship("Dataset", backref=__tablename__)

    def __init__(
        self,
        id: int = None,
        type_id: str = None,
        name: str = None,
        extra: dict = None,
        table_type: Union[TableType, dict] = None,
        created: pendulum.DateTime = None,
        modified: pendulum.DateTime = None,
    ):
        """Construct a Dataset object.

        :param id: unique id.
        :param type_id: a unique string id for the workflow type.
        :param name: the dataset name.
        :param extra: additional metadata for a dataset, stored as JSON.
        :param created: datetime created in UTC.
        :param modified: datetime modified in UTC.
        :param table_type: the Workflow associated with this dataset.
        """

        self.id = id
        self.type_id = type_id
        self.name = name
        self.extra = extra
        self.created = to_datetime_utc(created)
        self.modified = to_datetime_utc(modified)
        self.table_type = fetch_db_object(TableType, table_type)

    def update(
        self,
        type_id: str = None,
        name: str = None,
        extra: dict = None,
        table_type: Union[TableType, dict] = None,
        modified: pendulum.DateTime = None,
    ):
        """Update the properties of an existing Dataset object. This method is handy when you want to update
        the Dataset from a dictionary, e.g. obj.update(**{'modified': datetime.utcnow()}).

        :param type_id: a unique string id for the workflow type.
        :param name: the dataset name.
        :param extra: additional metadata for a dataset, stored as JSON.
        :param modified: datetime modified in UTC.
        :param workflow: the Workflow associated with this dataset.
        :return: None.
        """

        if type_id is not None:
            self.type_id = type_id

        if name is not None:
            self.name = name

        if extra is not None:
            self.extra = extra

        if table_type is not None:
            self.table_type = fetch_db_object(TableType, table_type)

        if modified is not None:
            self.modified = to_datetime_utc(modified)


@dataclass
class Dataset(Base):
    __tablename__ = "dataset"

    # Only include should be serialized to JSON as dataclass attributes
    id: int
    name: str
    service: str
    address: str
    created: pendulum.DateTime
    modified: pendulum.DateTime
    workflow: Workflow = None
    dataset_type: DatasetType = None

    id = Column(Integer, primary_key=True)
    name = Column(String(250))
    service = Column(String(250), nullable=False)
    address = Column(String(250), nullable=False)
    created = Column(DateTime())
    modified = Column(DateTime())

    workflow_id = Column(Integer, ForeignKey(f"{Workflow.__tablename__}.id"), nullable=False)
    dataset_type_id = Column(Integer, ForeignKey(f"{DatasetType.__tablename__}.id"), nullable=False)
    releases = relationship("DatasetRelease", backref=__tablename__)

    def __init__(
        self,
        id: int = None,
        name: str = None,
        service: str = None,
        address: str = None,
        workflow: Union[Workflow, dict] = None,
        dataset_type: Union[DatasetType, dict] = None,
        created: pendulum.DateTime = None,
        modified: pendulum.DateTime = None,
    ):
        """Construct a Dataset object.

        :param id: unique id.
        :param name: the dataset name.
        :param service: storage service name.
        :param address: storage resource address.
        :param created: datetime created in UTC.
        :param modified: datetime modified in UTC.
        :param workflow: the Workflow associated with this dataset.
        :param dataset_type: the DatasetType associated with this dataset.
        """

        self.id = id
        self.name = name
        self.service = service
        self.address = address
        self.created = to_datetime_utc(created)
        self.modified = to_datetime_utc(modified)

        # Fetch associated table info
        self.workflow = fetch_db_object(Workflow, workflow)
        self.dataset_type = fetch_db_object(DatasetType, dataset_type)

    def update(
        self,
        name: str = None,
        service: str = None,
        address: str = None,
        workflow: Union[Workflow, dict] = None,
        dataset_type: Union[DatasetType, dict] = None,
        modified: pendulum.DateTime = None,
    ):
        """Update the properties of an existing Dataset object. This method is handy when you want to update
        the Dataset from a dictionary, e.g. obj.update(**{'modified': datetime.utcnow()}).

        :param name: the dataset name.
        :param service: The storage service name, e.g., google.
        :param address: Storage resource address, e.g., project.dataset.tablename
        :param modified: datetime modified in UTC.
        :param workflow: the Workflow associated with this dataset.
        :param dataset_type: the DatasetType associated with this dataset.
        :return: None.
        """

        if name is not None:
            self.name = name

        if service is not None:
            self.service = service

        if address is not None:
            self.address = address

        if workflow is not None:
            self.workflow = fetch_db_object(Workflow, workflow)

        if dataset_type is not None:
            self.dataset_type = fetch_db_object(DatasetType, dataset_type)

        if modified is not None:
            self.modified = to_datetime_utc(modified)


@dataclass
class DatasetRelease(Base):
    __tablename__ = "dataset_release"

    id: int

    start_date: pendulum.DateTime
    end_date: pendulum.DateTime
    created: pendulum.DateTime
    modified: pendulum.DateTime
    dataset: Dataset = None

    id = Column(Integer, primary_key=True)
    start_date = Column(DateTime())
    end_date = Column(DateTime())
    dataset_id = Column(Integer, ForeignKey(f"{Dataset.__tablename__}.id"), nullable=False)
    created = Column(DateTime())
    modified = Column(DateTime())

    def __init__(
        self,
        id: int = None,
        start_date: Union[pendulum.DateTime, str] = None,
        end_date: Union[pendulum.DateTime, str] = None,
        dataset: Dataset = None,
        created: pendulum.DateTime = None,
        modified: pendulum.DateTime = None,
    ):
        """Construct a DatasetStorage object.

        :param id: unique id.
        :param service: storage service name.
        :param address: storage resource address.
        :param created: datetime created in UTC.
        :param modified: datetime modified in UTC.
        """

        self.id = id
        self.start_date = to_datetime_utc(start_date)
        self.end_date = to_datetime_utc(end_date)

        self.dataset = fetch_db_object(Dataset, dataset)

        self.created = to_datetime_utc(created)
        self.modified = to_datetime_utc(modified)

    def update(
        self,
        start_date: Union[pendulum.DateTime, str] = None,
        end_date: Union[pendulum.DateTime, str] = None,
        dataset: Dataset = None,
        modified: pendulum.DateTime = None,
    ):
        """Update the properties of an existing DatasetStorage object. This method is handy when you want to update
        the DatasetStorage from a dictionary, e.g. obj.update(**{'service': 'hello world'}).

        :param service: The storage service name, e.g., google.
        :param address: Storage resource address, e.g., project.dataset.tablename
        :param modified: Datetime modified in UTC.
        :return: None.
        """

        if start_date is not None:
            self.start_date = to_datetime_utc(start_date)

        if end_date is not None:
            self.end_date = to_datetime_utc(end_date)

        if modified is not None:
            self.modified = to_datetime_utc(modified)

        if dataset is not None:
            self.dataset = fetch_db_object(Dataset, dataset)


@dataclass
class BigQueryBytesProcessed(Base):
    __tablename__ = "bigquery_bytes_processed"

    id: int
    project: str
    total: int
    created: pendulum.DateTime
    modified: pendulum.DateTime

    id = Column(Integer, primary_key=True)
    project = Column(String(250))
    total = Column(BigInteger)
    created = Column(DateTime())
    modified = Column(DateTime())

    def __init__(
        self,
        id: int = None,
        project: str = None,
        total: int = None,
        created: pendulum.DateTime = None,
        modified: pendulum.DateTime = None,
    ):
        """Construct a BigQueryBytesProcessed object.

        :param id: unique id.
        :param project: GCP project.
        :param total: Total number of processed bytes.
        :param created: datetime created in UTC, i.e. the date that the query was made.
        :param modified: datetime modified in UTC.
        """

        self.id = id
        self.project = project
        self.total = total
        self.created = to_datetime_utc(created)
        self.modified = to_datetime_utc(modified)

    def update(self, project: str = None, date: str = None, total: int = None, modified: pendulum.DateTime = None):
        """Update the properties of an existing BigQueryBytesProcessed object. This method is handy when you want to
        update the BigQueryBytesProcessed from a dictionary, e.g. obj.update(**{'total': 100}).

        :param project: GCP project name.
        :param date: Date of queries in YYYY-MM-DD format.
        :param total: Total number of processed bytes.
        :param modified: datetime modified in UTC.
        :return: None.
        """

        if project is not None:
            self.project = project

        if total is not None:
            self.total = total

        if modified is not None:
            self.modified = to_datetime_utc(modified)
