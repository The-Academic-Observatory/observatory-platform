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
    JSON,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    create_engine,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, scoped_session, sessionmaker

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
            "or the environment variable OBSERVATORY_DB_URI with a valid PostgreSQL connection string"
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


def to_datetime_utc(obj: Union[None, pendulum.DateTime]) -> Union[pendulum.DateTime, None]:
    """Converts pendulum.DateTime into UTC object.

    :param obj: a pendulum.DateTime object (which will just be converted to UTC) or None which will be returned.
    :return: a DateTime object.
    """

    if isinstance(obj, pendulum.DateTime):
        return obj.in_tz(tz="UTC")
    elif obj is None:
        return None

    raise ValueError("body should be None or pendulum.DateTime")


@dataclass
class Organisation(Base):
    __tablename__ = "organisation"

    id: int
    name: str
    gcp_project_id: str
    gcp_download_bucket: str
    gcp_transform_bucket: str
    created: pendulum.DateTime
    modified: pendulum.DateTime

    id = Column(Integer, primary_key=True)
    name = Column(String(250))
    gcp_project_id = Column(String(30))
    gcp_download_bucket = Column(String(222))
    gcp_transform_bucket = Column(String(222))
    created = Column(DateTime())
    modified = Column(DateTime())
    telescopes = relationship("Telescope", backref="organisation")

    def __init__(
        self,
        id: int = None,
        name: str = None,
        gcp_project_id: str = None,
        gcp_download_bucket: str = None,
        gcp_transform_bucket: str = None,
        created: pendulum.DateTime = None,
        modified: pendulum.DateTime = None,
    ):
        """Construct an Organisation object, which contains information about what Google Cloud project an
        organisation uses, what are it's download and transform buckets and what telescopes does it have.

        The maximum lengths of the gcp_project_id, gcp_download_bucket and gcp_transform_bucket come from the following
        documentation:
        * https://cloud.google.com/resource-manager/docs/creating-managing-projects
        * https://cloud.google.com/storage/docs/naming-buckets


        :param id: unique id.
        :param name: the name.
        :param gcp_project_id: the Google Cloud project id.
        :param gcp_download_bucket: the Google Cloud Storage download bucket name for the above project.
        :param gcp_transform_bucket: the Google Cloud Storage transform bucket name for the above project.
        :param created: datetime created in UTC.
        :param modified: datetime modified in UTC.
        """

        self.id = id
        self.name = name
        self.gcp_project_id = gcp_project_id
        self.gcp_download_bucket = gcp_download_bucket
        self.gcp_transform_bucket = gcp_transform_bucket
        self.created = to_datetime_utc(created)
        self.modified = to_datetime_utc(modified)

    def update(
        self,
        name: str = None,
        gcp_project_id: str = None,
        gcp_download_bucket: str = None,
        gcp_transform_bucket: str = None,
        modified: pendulum.DateTime = None,
    ):
        """Update the properties of an existing Organisation object. This method is handy when you want to update
        the Organisation from a dictionary, e.g. obj.update(**{'name': 'hello world'}).

        :param name: the name.
        :param gcp_project_id: the Google Cloud project id.
        :param gcp_download_bucket: the Google Cloud Storage download bucket name for the above project.
        :param gcp_transform_bucket: the Google Cloud Storage transform bucket name for the above project.
        :param modified: datetime modified in UTC.
        :return: None.
        """

        if name is not None:
            self.name = name

        if gcp_project_id is not None:
            self.gcp_project_id = gcp_project_id

        if gcp_download_bucket is not None:
            self.gcp_download_bucket = gcp_download_bucket

        if gcp_transform_bucket is not None:
            self.gcp_transform_bucket = gcp_transform_bucket

        if modified is not None:
            self.modified = to_datetime_utc(modified)


@dataclass
class Telescope(Base):
    __tablename__ = "connection"

    # Only include should be serialized to JSON as dataclass attributes
    id: int
    name: str
    extra: Dict
    created: pendulum.DateTime
    modified: pendulum.DateTime
    telescope_type: TelescopeType = None
    organisation: Organisation = None

    id = Column(Integer, primary_key=True)
    name = Column(String(250))
    extra = Column(JSON())
    created = Column(DateTime())
    modified = Column(DateTime())
    organisation_id = Column(Integer, ForeignKey("organisation.id"), nullable=False)
    telescope_type_id = Column(Integer, ForeignKey("telescope_type.id"), nullable=False)

    def __init__(
        self,
        id: int = None,
        name: str = None,
        extra: Dict = None,
        created: pendulum.DateTime = None,
        modified: pendulum.DateTime = None,
        organisation: Union[Organisation, Dict] = None,
        telescope_type: Union[TelescopeType, Dict] = None,
    ):
        """Construct a Telescope object.

        :param id: unique id.
        :param name: the telescope name.
        :param extra: additional metadata for a telescope, stored as JSON.
        :param created: datetime created in UTC.
        :param modified: datetime modified in UTC.
        :param organisation: the organisation associated with this telescope.
        :param telescope_type: the telescope type associated with this telescope.
        """

        self.id = id
        self.name = name
        self.extra = extra
        self.created = to_datetime_utc(created)
        self.modified = to_datetime_utc(modified)

        # Fetch organisation and connection type from database if it exists
        self.organisation = fetch_db_object(Organisation, organisation)
        self.telescope_type = fetch_db_object(TelescopeType, telescope_type)

    def update(
        self,
        name: str = None,
        extra: Dict = None,
        modified: pendulum.DateTime = None,
        organisation: Union[Organisation, Dict] = None,
        telescope_type: Union[TelescopeType, Dict] = None,
    ):
        """Update the properties of an existing Telescope object. This method is handy when you want to update
        the Telescope from a dictionary, e.g. obj.update(**{'modified': datetime.utcnow()}).

        :param name: the telescope name.
        :param extra: additional metadata for a telescope, stored as JSON.
        :param modified: datetime modified in UTC.
        :param organisation: the organisation associated with this telescope.
        :param telescope_type: the telescope type associated with this telescope.
        :return: None.
        """

        if name is not None:
            self.name = name

        if extra is not None:
            self.extra = extra

        if organisation is not None:
            self.organisation = fetch_db_object(Organisation, organisation)

        if telescope_type is not None:
            self.telescope_type = fetch_db_object(TelescopeType, telescope_type)

        if modified is not None:
            self.modified = to_datetime_utc(modified)


@dataclass
class TelescopeType(Base):
    __tablename__ = "telescope_type"

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
    telescopes = relationship("Telescope", backref="telescope_type")

    def __init__(
        self,
        id: int = None,
        type_id: str = None,
        name: str = None,
        created: pendulum.DateTime = None,
        modified: pendulum.DateTime = None,
    ):
        """Construct a TelescopeType object.

        :param id: unique id.
        :param type_id: a unique string id for the telescope type.
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
        """Update the properties of an existing TelescopeType object. This method is handy when you want to update
        the TelescopeType from a dictionary, e.g. obj.update(**{'name': 'hello world'}).

        :param name: the name of the TelescopeType.
        :param type_id: a unique string id for the telescope type.
        :param modified: datetime modified in UTC.
        :return: None.
        """

        if type_id is not None:
            self.type_id = type_id

        if name is not None:
            self.name = name

        if modified is not None:
            self.modified = to_datetime_utc(modified)
