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
from typing import Any
from typing import ClassVar
from typing import List

from sqlalchemy import String, create_engine, Integer, ForeignKey, Column, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, scoped_session, sessionmaker

Base = declarative_base()
session_ = None  # Global session


def create_session(uri: str = os.environ.get('OBSERVATORY_DB_URI'), connect_args=None, poolclass=None):
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


def fetch_db_item(item: Base):
    cls = item.__class__
    return_item = item

    if item is not None and item.id is not None:
        return_item = session_.query(cls).filter(cls.id == item.id).one_or_none()
        if return_item is None:
            raise ValueError(f'{return_item} with id {item.id} not found')

    return return_item


def dict_to_dataclass(cls: ClassVar, body: Any):
    if body is None or isinstance(body, cls):
        return body
    elif isinstance(body, dict):
        return cls(**body)

    raise ValueError('body should be a Dict')


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
    connections: List[Connection]

    id = Column(Integer, primary_key=True)
    name = Column(String(250))
    gcp_project_id = Column(String(30))
    gcp_download_bucket = Column(String(222))
    gcp_transform_bucket = Column(String(222))
    created = Column(DateTime())
    modified = Column(DateTime())
    connections = relationship("Connection", backref='organisation')

    def __init__(self, id: int = None, name: str = None, gcp_project_id: str = None, gcp_download_bucket: str = None,
                 gcp_transform_bucket: str = None, created: datetime = None, modified: datetime = None,
                 connections: List[Connection] = None):
        self.id = id
        self.name = name
        self.gcp_project_id = gcp_project_id
        self.gcp_download_bucket = gcp_download_bucket
        self.gcp_transform_bucket = gcp_transform_bucket
        self.created = created
        self.modified = modified

        # Fetch connections from database if they exist
        if connections is not None:
            self.connections = []
            for conn in connections:
                self.connections.append(fetch_db_item(dict_to_dataclass(Connection, conn)))


#
# # https://stackoverflow.com/questions/5022066/how-to-serialize-sqlalchemy-result-to-json


@dataclass
class Connection(Base):
    __tablename__ = 'connection'

    id: int
    name: str
    airflow_connection_id: int
    created: datetime
    modified: datetime
    organisation: Organisation = None
    connection_type: ConnectionType = None

    id = Column(Integer, primary_key=True)
    name = Column(String(250))
    airflow_connection_id = Column(Integer)
    created = Column(DateTime())
    modified = Column(DateTime())
    organisation_id = Column(Integer, ForeignKey('organisation.id'))
    connection_type_id = Column(Integer, ForeignKey('connection_type.id'))

    def __init__(self, id: int = None, name: str = None, airflow_connection_id: int = None,
                 created: datetime = None, modified: datetime = None,
                 organisation: Organisation = None, connection_type: ConnectionType = None, ):
        self.id = id
        self.name = name
        self.airflow_connection_id = airflow_connection_id
        self.created = created
        self.modified = modified

        # Fetch organisation and connection type from database if it exists
        self.organisation = fetch_db_item(dict_to_dataclass(Organisation, organisation))
        self.connection_type = fetch_db_item(dict_to_dataclass(ConnectionType, connection_type))

    def update(self, organisation: Organisation = None, connection_type: ConnectionType = None,
               name: str = None, airflow_connection_id: int = None):
        if organisation is not None:
            pass

        if connection_type is not None:
            pass

        if name is not None:
            pass

        if airflow_connection_id is not None:
            pass


@dataclass
class ConnectionType(Base):
    __tablename__ = 'connection_type'

    id: int
    name: str
    created: datetime
    modified: datetime

    id = Column(Integer, primary_key=True)
    name = Column(String(250))
    created = Column(DateTime())
    modified = Column(DateTime())
    connections = relationship("Connection", backref='connection_type')

    def update(self, name: str = None, modified: datetime = None):
        if name is not None:
            self.name = name
        if modified is not None:
            self.modified = modified
