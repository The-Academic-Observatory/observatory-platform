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

from observatory.api.orm import create_session, set_session, TelescopeType, Telescope, Organisation


def create_telescope_types(session: scoped_session, names: List[str], created: datetime):
    items = []

    for name in names:
        item = TelescopeType(name=name, created=created, modified=created)
        items.append(item)
        session.add(item)
        session.commit()

    return items


def datetime_to_str(dt: datetime):
    return dt.strftime('%Y-%m-%d %H:%M:%S')


class TestOrm(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestOrm, self).__init__(*args, **kwargs)
        self.uri = 'sqlite://'
        self.telescope_type_names = ['ONIX Telescope', 'Scopus Telescope', 'WoS Telescope']

    def setUp(self) -> None:
        """ Create the SQLAlchemy Session """

        self.session = create_session(uri=self.uri, connect_args={'check_same_thread': False}, poolclass=StaticPool)
        set_session(self.session)

    def test_create_session(self):
        """ Test that session is created """

        self.assertIsInstance(self.session, scoped_session)

    def test_telescope_type(self):
        """ Test that TelescopeType can be created, fetched, updated and deleted """

        created = datetime.utcnow()

        # Create and assert created
        telescope_types_a = create_telescope_types(self.session, self.telescope_type_names, created)
        for i, conn_type in enumerate(telescope_types_a):
            self.assertIsNotNone(conn_type.id)
            expected_id = i + 1
            self.assertEqual(conn_type.id, expected_id)

        # Fetch items
        telescope_types_b = self.session.query(TelescopeType).order_by(TelescopeType.id).all()
        self.assertEqual(len(telescope_types_b), len(self.telescope_type_names))
        for a, b in zip(telescope_types_a, telescope_types_b):
            self.assertEqual(a, b)

        # Update items
        names_b = ['Hello', 'World', 'Scopus']
        for name, conn_type in zip(names_b, telescope_types_b):
            conn_type.name = name
            self.session.commit()

        # Check that items updated
        telescope_types_c = self.session.query(TelescopeType).order_by(TelescopeType.id).all()
        for name, conn_type in zip(names_b, telescope_types_c):
            self.assertEqual(name, conn_type.name)

        # Delete items
        for conn_type in telescope_types_c:
            self.session.query(TelescopeType).filter(TelescopeType.id == conn_type.id).delete()
            self.session.commit()

        # Check that items deleted
        conn_types_d = self.session.query(TelescopeType).order_by(TelescopeType.id).all()
        self.assertEqual(len(conn_types_d), 0)

    def test_telescope_type_from_dict(self):
        """ Test that TelescopeType can be created and updated from a dictionary """

        # Create
        expected_id = 1
        dt_str = datetime_to_str(datetime.utcnow())
        dict_ = {'name': 'My Telescope Type', 'created': dt_str, 'modified': dt_str}

        obj = TelescopeType(**dict_)
        self.session.add(obj)
        self.session.commit()

        self.assertIsNotNone(obj.id)
        self.assertEqual(obj.id, 1)

        # Update
        dt_str = datetime_to_str(datetime.utcnow())
        dict_ = {'name': 'My Telescope Type 2', 'modified': dt_str}
        connection_type = self.session.query(TelescopeType).filter(TelescopeType.id == expected_id).one()
        connection_type.update(**dict_)
        self.session.commit()

    def test_telescope(self):
        """ Test that Telescope can be created, fetched, updated and deleted """

        # Create TelescopeType instances
        created = datetime.utcnow()
        create_telescope_types(self.session, self.telescope_type_names, created)

        # Create Telescope
        telescope_type = self.session.query(TelescopeType).filter(TelescopeType.id == 1).one()
        telescope = Telescope(telescope_type=telescope_type,
                              modified=created,
                              created=created)
        self.session.add(telescope)
        self.session.commit()

        # Assert created object
        self.assertIsNotNone(telescope.id)
        self.assertEqual(telescope.id, 1)

        # Update Telescope
        new_telescope_type_id = 2

        def get_telescope():
            return self.session.query(Telescope).filter(Telescope.id == 1).one()

        telescope = get_telescope()
        telescope.telescope_type_id = new_telescope_type_id
        self.session.commit()

        # Assert update
        telescope = get_telescope()
        self.assertEqual(telescope.telescope_type_id, new_telescope_type_id)

        # Delete items
        self.session.query(Telescope).filter(Telescope.id == 1).delete()
        with self.assertRaises(sqlalchemy.orm.exc.NoResultFound):
            get_telescope()

    def test_telescope_from_dict(self):
        """ Test that Telescope can be created and updated from a dictionary """

        # Create Organisation
        dt = datetime.utcnow()
        organisation = Organisation(name='My Organisation',
                                    gcp_project_id='project-id',
                                    gcp_download_bucket='download-bucket',
                                    gcp_transform_bucket='transform-bucket',
                                    created=dt,
                                    modified=dt)
        self.session.add(organisation)
        self.session.commit()

        # Create TelescopeType instances
        created = datetime.utcnow()
        create_telescope_types(self.session, self.telescope_type_names, created)

        # Create Telescope
        expected_id = 1
        dt_str = datetime_to_str(datetime.utcnow())
        dict_ = {'organisation': {
            'id': organisation.id
        },
            'telescope_type': {
                'id': expected_id
            },
            'created': dt_str,
            'modified': dt_str}
        obj = Telescope(**dict_)
        self.session.add(obj)
        self.session.commit()
        self.assertIsNotNone(obj.id)
        self.assertEqual(obj.id, expected_id)

        # Update Telescope
        expected_id = 2
        organisation2 = Organisation(name='My Organisation 2',
                                     gcp_project_id='project-id',
                                     gcp_download_bucket='download-bucket',
                                     gcp_transform_bucket='transform-bucket',
                                     created=dt,
                                     modified=dt)
        self.session.add(organisation2)
        self.session.commit()

        dict_ = {'organisation': {
            'id': expected_id
        }, 'telescope_type': {
            'id': expected_id
        }}
        obj.update(**dict_)
        self.session.commit()
        self.assertEqual(obj.telescope_type.id, expected_id)
        self.assertEqual(obj.organisation.id, expected_id)

    def test_organisation(self):
        """ Test that Organisation can be created, fetched, updates and deleted """

        # Create Organisation
        created = datetime.utcnow()
        organisation = Organisation(name='My Organisation',
                                    gcp_project_id='project-id',
                                    gcp_download_bucket='download-bucket',
                                    gcp_transform_bucket='transform-bucket',
                                    created=created)
        self.session.add(organisation)
        self.session.commit()

        # Assert created object
        expected_id = 1
        self.assertIsNotNone(organisation.id)
        self.assertEqual(organisation.id, expected_id)

        # Update Organisation
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

        # Create
        expected_id = 1
        dt_str = datetime_to_str(datetime.utcnow())
        dict_ = {
            'name': 'My Organisation',
            'gcp_project_id': 'project-id',
            'gcp_download_bucket': 'download-bucket',
            'gcp_transform_bucket': 'transform-bucket',
            'created': dt_str,
            'modified': dt_str
        }
        obj = Organisation(**dict_)
        self.session.add(obj)
        self.session.commit()
        self.assertIsNotNone(obj.id)
        self.assertEqual(obj.id, expected_id)

        # Update
        dt = datetime.utcnow()
        dt_str = datetime_to_str(dt)
        dict_ = {
            'name': 'My Organisation 2',
            'modified': dt_str
        }
        obj.update(**dict_)
        self.session.commit()
        self.assertEqual(dt_str, datetime_to_str(obj.modified))
        self.assertEqual('My Organisation 2', obj.name)
