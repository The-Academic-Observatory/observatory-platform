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

import unittest
from datetime import datetime
from typing import List

import sqlalchemy
from sqlalchemy.orm import scoped_session
from sqlalchemy.pool import StaticPool

from observatory.platform.api.app import set_session
from observatory.platform.api.orm import create_session, ConnectionType, Connection, Organisation


def create_conn_types(session: scoped_session, names: List[str], created: datetime):
    items = []

    for name in names:
        conn_type = ConnectionType(name=name, created=created)
        items.append(conn_type)
        session.add(conn_type)
        session.commit()

    return items


class TestOrm(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestOrm, self).__init__(*args, **kwargs)
        self.uri = 'sqlite://'
        self.conn_type_names = ['GCP', 'ONIX', 'Scopus']

    def setUp(self) -> None:
        """ Create the SQLAlchemy Session """

        self.session = create_session(uri=self.uri, connect_args={'check_same_thread': False}, poolclass=StaticPool)
        set_session(self.session)

    def test_create_session(self):
        """ Test that session is created """

        self.assertIsInstance(self.session, scoped_session)

    def test_connection_type(self):
        """ Test that ConnectionType can be created, fetched, updates and deleted """

        created = datetime.now()

        # Create and assert created
        conn_types_a = create_conn_types(self.session, self.conn_type_names, created)
        for i, conn_type in enumerate(conn_types_a):
            self.assertIsNotNone(conn_type.id)
            expected_id = i + 1
            self.assertEqual(conn_type.id, expected_id)

        # Fetch items
        conn_types_b = self.session.query(ConnectionType).order_by(ConnectionType.id).all()
        self.assertEqual(len(conn_types_b), len(self.conn_type_names))
        for conn_a, conn_b in zip(conn_types_a, conn_types_b):
            self.assertEqual(conn_a, conn_b)

        # Update items
        names_b = ['Hello', 'World', 'Scopus']
        for name, conn_type in zip(names_b, conn_types_b):
            conn_type.name = name
            self.session.commit()

        # Check that items updated
        conn_types_c = self.session.query(ConnectionType).order_by(ConnectionType.id).all()
        for name, conn_type in zip(names_b, conn_types_c):
            self.assertEqual(name, conn_type.name)

        # Delete items
        for conn_type in conn_types_c:
            self.session.query(ConnectionType).filter(ConnectionType.id == conn_type.id).delete()
            self.session.commit()

        # Check that items deleted
        conn_types_d = self.session.query(ConnectionType).order_by(ConnectionType.id).all()
        self.assertEqual(len(conn_types_d), 0)

    def test_connection_type_from_dict(self):
        """ Test that ConnectionType can be created from a dictionary """

        dict_ = {'name': 'My Connection Type', 'created': datetime.now()}
        obj = ConnectionType(**dict_)
        self.session.add(obj)
        self.session.commit()

        self.assertIsNotNone(obj.id)
        self.assertEqual(obj.id, 1)

    def test_connection_type_update(self):
        self.fail()

    def test_connection(self):
        """ Test that Connection can be created, fetched, updates and deleted """

        # Create ConnectionType instances
        created = datetime.now()
        create_conn_types(self.session, self.conn_type_names, created)

        # Create Connection
        connection_type = self.session.query(ConnectionType).filter(ConnectionType.id == 1).one()
        connection = Connection(name='My Connection',
                                connection_type=connection_type,
                                airflow_connection_id=1,
                                created=created)
        self.session.add(connection)
        self.session.commit()

        # Assert created object
        self.assertIsNotNone(connection.id)
        self.assertEqual(connection.id, 1)

        # Update Connection
        new_name = 'New Connection'
        new_airflow_connection_id = 3

        def get_connection():
            return self.session.query(ConnectionType).filter(ConnectionType.id == 1).one()

        connection = get_connection()
        connection.name = new_name
        connection.airflow_connection_id = new_airflow_connection_id
        self.session.commit()

        # Assert update
        connection = get_connection()
        self.assertEqual(connection.name, new_name)
        self.assertEqual(connection.airflow_connection_id, new_airflow_connection_id)

        # Delete items
        self.session.query(ConnectionType).filter(ConnectionType.id == 1).delete()
        with self.assertRaises(sqlalchemy.orm.exc.NoResultFound):
            get_connection()

    def test_connection_from_dict(self):
        """ Test that Connection can be created from a dictionary """

        # No existing organisation or connection type
        created = datetime.now()
        dict_ = {'name': 'My Connection Type',
                 'organisation': {
                     'name': 'My Organisation',
                     'gcp_project_id': 'project-id',
                     'gcp_download_bucket': 'download-bucket',
                     'gcp_transform_bucket': 'transform-bucket',
                     'created': created
                 },
                 'connection_type': {
                     'name': 'ONIX',
                     'created': created
                 },
                 'airflow_connection_id': 1,
                 'created': created}
        obj = Connection(**dict_)
        self.session.add(obj)
        self.session.commit()
        self.assertIsNotNone(obj.id)
        self.assertEqual(obj.id, 1)

        # Existing organisation and connection type
        expected_id = 1
        dict_ = {'name': 'My Connection Type 2',
                 'organisation': {
                     'id': expected_id
                 },
                 'connection_type': {
                     'id': expected_id
                 },
                 'airflow_connection_id': 2,
                 'created': created}
        obj = Connection(**dict_)
        self.session.add(obj)
        self.session.commit()
        self.assertIsNotNone(obj.id)
        self.assertEqual(obj.id, 2)

    def test_connection_update(self):
        self.fail()

    def test_organisation(self):
        """ Test that Organisation can be created, fetched, updates and deleted """

        # Create Organisation
        created = datetime.now()
        organisation = Organisation(name='My Organisation',
                                    gcp_project_id='project-id',
                                    gcp_download_bucket='download-bucket',
                                    gcp_transform_bucket='transform-bucket',
                                    created=created,
                                    connections=[Connection(name='My Connection',
                                                            connection_type=ConnectionType(name='ONIX'),
                                                            airflow_connection_id=2,
                                                            created=created)])
        self.session.add(organisation)
        self.session.commit()

        # Assert created object
        expected_id = 1
        self.assertIsNotNone(organisation.id)
        self.assertEqual(organisation.id, expected_id)

        # Update Connection
        new_name = 'New Organisation'
        organisation = self.session.query(Organisation).filter(Organisation.id == expected_id).one()
        organisation.name = new_name
        self.session.commit()

        # Assert update
        organisation = self.session.query(Organisation).filter(Organisation.id == expected_id).one()
        self.assertEqual(organisation.name, new_name)

        # Delete items
        self.session.query(Organisation).filter(Organisation.id == expected_id).delete()
        with self.assertRaises(sqlalchemy.orm.exc.NoResultFound):
            self.session.query(Organisation).filter(Organisation.id == expected_id).one()

    def test_organisation_from_dict(self):
        """ Test that Organisation can be created from a dictionary """

        # No existing organisation or connection type
        created = datetime.now()
        dict_ = {
            'name': 'My Organisation',
            'gcp_project_id': 'project-id',
            'gcp_download_bucket': 'download-bucket',
            'gcp_transform_bucket': 'transform-bucket',
            'created': created,
            'connections': [
                {
                    'name': 'Curtin ONIX',
                    'connection_type': {
                        'name': 'ONIX',
                        'created': created
                    },
                    'airflow_connection_id': 1,
                    'created': created},
                {
                    'name': 'Curtin GCP',
                    'connection_type': {
                        'name': 'GCP',
                        'created': created
                    },
                    'airflow_connection_id': 2,
                    'created': created}
            ]}
        obj = Organisation(**dict_)
        self.session.add(obj)
        self.session.commit()
        self.assertIsNotNone(obj.id)
        self.assertEqual(obj.id, 1)

        # Existing organisation and connection type
        dict_ = {
            'name': 'My Organisation 2',
            'gcp_project_id': 'project-id',
            'gcp_download_bucket': 'download-bucket',
            'gcp_transform_bucket': 'transform-bucket',
            'created': created,
            'connections': [
                {'id': 1},
                {'id': 2}
            ]}
        obj = Organisation(**dict_)
        self.session.add(obj)
        self.session.commit()
        self.assertIsNotNone(obj.id)
        self.assertEqual(obj.id, 2)

    def test_organisation_update(self):
        self.fail()
