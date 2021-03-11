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

import datetime
import os
from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar, Dict, Union, Any

from sqlalchemy import String, create_engine, Integer, ForeignKey, Column, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, scoped_session, sessionmaker

Base = declarative_base()
session_ = None  # Global session


def create_session(uri: str = os.environ.get('OBSERVATORY_DB_URI'), connect_args=None, poolclass=None):
    if uri is None:
        raise ValueError('observatory.api.orm.create_session: please set the create_session `uri` parameter '
                         'or the environment variable OBSERVATORY_DB_URI with a valid PostgreSQL connection string')

    if connect_args is None:
        connect_args = dict()

    engine = create_engine(uri,
                           convert_unicode=True,
                           connect_args=connect_args,
                           poolclass=poolclass)
    s = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
    Base.query = s.query_property()
    Base.metadata.create_all(bind=engine)
    return s


def set_session(session):
    global session_
    session_ = session
    import observatory.api.api as api
    api.session_ = session


def fetch_db_item(cls: ClassVar, body: Any):
    if body is None:
        item = None
    elif isinstance(body, cls):
        item = body
    elif isinstance(body, Dict):
        if 'id' not in body:
            raise AttributeError(f'id not found in {body}')

        id = body['id']
        item = session_.query(cls).filter(cls.id == id).one_or_none()
        if item is None:
            raise ValueError(f'{item} with id {id} not found')
    else:
        raise ValueError(f'Unknown item type {body}')

    return item


def to_datetime(obj: Union[str, datetime]) -> datetime:
    if obj is None or isinstance(obj, datetime):
        return obj
    elif isinstance(obj, str):
        return datetime.strptime(obj, '%Y-%m-%d %H:%M:%S')

    raise ValueError('body should be a str or datetime')


@dataclass
class Organisation(Base):
    """
    https://cloud.google.com/resource-manager/docs/creating-managing-projects
    https://cloud.google.com/storage/docs/naming-buckets
    """

    __tablename__ = 'organisation'

    id: int
    name: str
    gcp_project_id: str
    gcp_download_bucket: str
    gcp_transform_bucket: str
    created: datetime
    modified: datetime

    id = Column(Integer, primary_key=True)
    name = Column(String(250))
    gcp_project_id = Column(String(30))
    gcp_download_bucket = Column(String(222))
    gcp_transform_bucket = Column(String(222))
    created = Column(DateTime())
    modified = Column(DateTime())
    telescopes = relationship("Telescope", backref='organisation')

    def __init__(self, id: int = None, name: str = None, gcp_project_id: str = None, gcp_download_bucket: str = None,
                 gcp_transform_bucket: str = None, created: datetime = None, modified: datetime = None):
        self.id = id
        self.name = name
        self.gcp_project_id = gcp_project_id
        self.gcp_download_bucket = gcp_download_bucket
        self.gcp_transform_bucket = gcp_transform_bucket
        self.created = to_datetime(created)
        self.modified = to_datetime(modified)

    def update(self, name: str = None, gcp_project_id: str = None, gcp_download_bucket: str = None,
               gcp_transform_bucket: str = None, modified: Union[datetime, str] = None):
        if name is not None:
            self.name = name

        if gcp_project_id is not None:
            self.gcp_project_id = gcp_project_id

        if gcp_download_bucket is not None:
            self.gcp_download_bucket = gcp_download_bucket

        if gcp_transform_bucket is not None:
            self.gcp_transform_bucket = gcp_transform_bucket

        if modified is not None:
            self.modified = to_datetime(modified)


@dataclass
class Telescope(Base):
    __tablename__ = 'connection'

    # Only include should be serialized to JSON as dataclass attributes
    id: int
    created: datetime
    modified: datetime
    telescope_type: TelescopeType = None

    id = Column(Integer, primary_key=True)
    created = Column(DateTime())
    modified = Column(DateTime())
    organisation_id = Column(Integer, ForeignKey('organisation.id'))
    telescope_type_id = Column(Integer, ForeignKey('telescope_type.id'))

    def __init__(self, id: int = None, created: datetime = None, modified: datetime = None,
                 organisation: Union[Organisation, Dict] = None, telescope_type: Union[TelescopeType, Dict] = None):
        self.id = id
        self.created = to_datetime(created)
        self.modified = to_datetime(modified)

        # Fetch organisation and connection type from database if it exists
        self.organisation = fetch_db_item(Organisation, organisation)
        self.telescope_type = fetch_db_item(TelescopeType, telescope_type)

    def update(self, modified: Union[datetime, str] = None, organisation: Union[Organisation, Dict] = None,
               telescope_type: Union[TelescopeType, Dict] = None):
        if organisation is not None:
            self.organisation = fetch_db_item(Organisation, organisation)

        if telescope_type is not None:
            self.telescope_type = fetch_db_item(TelescopeType, telescope_type)

        if modified is not None:
            self.modified = to_datetime(modified)


@dataclass
class TelescopeType(Base):
    __tablename__ = 'telescope_type'

    id: int
    name: str
    created: datetime
    modified: datetime

    id = Column(Integer, primary_key=True)
    name = Column(String(250))
    created = Column(DateTime())
    modified = Column(DateTime())
    telescopes = relationship("Telescope", backref='telescope_type')

    def __init__(self, id: int = None, name: str = None, created: Union[datetime, str] = None,
                 modified: Union[datetime, str] = None):
        self.id = id
        self.name = name
        self.created = to_datetime(created)
        self.modified = to_datetime(modified)

    def update(self, name: str = None, modified: Union[datetime, str] = None):
        if name is not None:
            self.name = name
        if modified is not None:
            self.modified = to_datetime(modified)
