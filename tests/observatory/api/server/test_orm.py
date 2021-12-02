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

import pendulum
import sqlalchemy
from observatory.api.server.orm import (
    Dataset,
    DatasetRelease,
    DatasetStorage,
    Organisation,
    Telescope,
    TelescopeType,
    create_session,
    fetch_db_object,
    set_session,
    to_datetime_utc,
)
from sqlalchemy.orm import scoped_session
from sqlalchemy.pool import StaticPool


def create_telescope_types(session: scoped_session, telescope_types: List, created: datetime):
    """Create a list of TelescopeType objects.

    :param session: the SQLAlchemy session.
    :param telescope_types: a list of tuples of telescope type id and names.
    :param created:the created datetime in UTC.
    :return: a list of TelescopeType objects.
    """

    items = []

    for type_id, name in telescope_types:
        item = TelescopeType(name=name, type_id=type_id, created=created, modified=created)
        items.append(item)
        session.add(item)
        session.commit()

    return items


class TestSession(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestSession, self).__init__(*args, **kwargs)
        self.uri = "sqlite://"

    def test_create_session(self):
        """Test create_session and init_db"""

        # Create session with seed_db set to True
        self.session = create_session(uri=self.uri, connect_args={"check_same_thread": False}, poolclass=StaticPool)
        self.assertTrue(self.session.connection())


class TestOrm(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestOrm, self).__init__(*args, **kwargs)
        self.uri = "sqlite://"
        self.telescope_types = [("onix", "ONIX Telescope"), ("scopus", "Scopus Telescope"), ("wos", "WoS Telescope")]

    def setUp(self) -> None:
        """Create the SQLAlchemy Session"""

        self.session = create_session(uri=self.uri, connect_args={"check_same_thread": False}, poolclass=StaticPool)
        set_session(self.session)

    def test_fetch_db_item(self):
        """Test fetch_db_object"""

        # Body is None
        self.assertEqual(None, fetch_db_object(TelescopeType, None))

        # Body is instance of cls
        dt = pendulum.now("UTC")
        dict_ = {"name": "My Telescope Type", "type_id": "onix", "created": dt, "modified": dt}
        obj = TelescopeType(**dict_)
        self.assertEqual(obj, fetch_db_object(TelescopeType, obj))

        # Body is Dict and no id
        with self.assertRaises(AttributeError):
            fetch_db_object(TelescopeType, {"name": "My Telescope Type"})

        # Body is Dict, has id and is not found
        with self.assertRaises(ValueError):
            fetch_db_object(TelescopeType, {"id": 1})

        # Body is Dict, has id and is found
        self.session.add(obj)
        self.session.commit()
        obj_fetched = fetch_db_object(TelescopeType, {"id": 1})
        self.assertEqual(obj, obj_fetched)

        # Body is wrong type
        with self.assertRaises(ValueError):
            fetch_db_object(TelescopeType, "hello")

    def test_to_datetime_utc(self):
        """Test to_datetime"""

        # From datetime
        dt_nz = pendulum.datetime(year=2020, month=12, day=31, tz=pendulum.timezone("Pacific/Auckland"))
        dt_utc = pendulum.datetime(year=2020, month=12, day=30, hour=11, tz=pendulum.timezone("UTC"))
        self.assertEqual(dt_utc, to_datetime_utc(dt_nz))
        self.assertEqual(dt_utc, to_datetime_utc(dt_utc))

        # From None
        self.assertIsNone(to_datetime_utc(None))

        # From another type
        with self.assertRaises(ValueError):
            to_datetime_utc(dt_nz.date())

    def test_create_session(self):
        """Test that session is created"""

        self.assertIsInstance(self.session, scoped_session)

        # Assert ValueError because uri=None
        with self.assertRaises(ValueError):
            create_session(uri=None)

        # connect_args=None
        create_session(uri=self.uri, connect_args=None, poolclass=StaticPool)

    def test_telescope_type(self):
        """Test that TelescopeType can be created, fetched, updated and deleted"""

        created = pendulum.now("UTC")

        # Create and assert created
        telescope_types_a = create_telescope_types(self.session, self.telescope_types, created)
        for i, conn_type in enumerate(telescope_types_a):
            self.assertIsNotNone(conn_type.id)
            expected_id = i + 1
            self.assertEqual(conn_type.id, expected_id)

        # Fetch items
        telescope_types_b = self.session.query(TelescopeType).order_by(TelescopeType.id).all()
        self.assertEqual(len(telescope_types_b), len(self.telescope_types))
        for a, b in zip(telescope_types_a, telescope_types_b):
            self.assertEqual(a, b)

        # Update items
        names_b = ["Hello", "World", "Scopus"]
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
        """Test that TelescopeType can be created and updated from a dictionary"""

        # Create
        expected_id = 1
        dt = pendulum.now("UTC")
        dict_ = {"name": "My Telescope Type", "type_id": "onix", "created": dt, "modified": dt}

        obj = TelescopeType(**dict_)
        self.session.add(obj)
        self.session.commit()

        self.assertIsNotNone(obj.id)
        self.assertEqual(obj.id, 1)

        # Update with no new values
        obj.update(**{})
        self.session.commit()
        self.assertEqual(expected_id, obj.id)
        self.assertEqual(dict_["name"], obj.name)
        self.assertEqual(dt, pendulum.instance(obj.created))
        self.assertEqual(dt, pendulum.instance(obj.modified))

        # Update
        dt = pendulum.now("UTC")
        dict_ = {"name": "My Telescope Type 2", "modified": dt}
        connection_type = self.session.query(TelescopeType).filter(TelescopeType.id == expected_id).one()
        connection_type.update(**dict_)
        self.session.commit()

    def test_telescope(self):
        """Test that Telescope can be created, fetched, updated and deleted"""

        # Create TelescopeType instances
        dt = pendulum.now("UTC")
        create_telescope_types(self.session, self.telescope_types, dt)

        organisation = Organisation(
            name="My Organisation",
            gcp_project_id="project-id",
            gcp_download_bucket="download-bucket",
            gcp_transform_bucket="transform-bucket",
            created=dt,
            modified=dt,
        )
        self.session.add(organisation)
        self.session.commit()

        telescope_type = self.session.query(TelescopeType).filter(TelescopeType.id == 1).one()
        telescope = Telescope(
            name="Curtin ONIX Telescope",
            extra={"view_id": 123456},
            telescope_type=telescope_type,
            organisation=organisation,
            modified=dt,
            created=dt,
        )
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
        """Test that Telescope can be created and updated from a dictionary"""

        # Create Organisation
        dt = pendulum.now("UTC")
        organisation = Organisation(
            name="My Organisation",
            gcp_project_id="project-id",
            gcp_download_bucket="download-bucket",
            gcp_transform_bucket="transform-bucket",
            created=dt,
            modified=dt,
        )
        self.session.add(organisation)
        self.session.commit()

        # Create TelescopeType instances
        created = pendulum.now("UTC")
        create_telescope_types(self.session, self.telescope_types, created)

        # Create Telescope
        expected_id = 1
        dt = pendulum.now("UTC")
        dict_ = {
            "organisation": {"id": organisation.id},
            "telescope_type": {"id": expected_id},
            "created": dt,
            "modified": dt,
        }
        obj = Telescope(**dict_)
        self.session.add(obj)
        self.session.commit()
        self.assertIsNotNone(obj.id)
        self.assertEqual(obj.id, expected_id)

        # Update with no new values
        obj.update(**{})
        self.session.commit()
        self.assertEqual(expected_id, obj.telescope_type.id)
        self.assertEqual(expected_id, obj.organisation.id)
        self.assertEqual(dt, pendulum.instance(obj.created))
        self.assertEqual(dt, pendulum.instance(obj.modified))

        # Update Telescope
        expected_id = 2
        organisation2 = Organisation(
            name="My Organisation 2",
            gcp_project_id="project-id",
            gcp_download_bucket="download-bucket",
            gcp_transform_bucket="transform-bucket",
            created=dt,
            modified=dt,
        )
        self.session.add(organisation2)
        self.session.commit()

        dict_ = {
            "name": "Curtin ONIX Telescope",
            "extra": {"view_id": 123456},
            "organisation": {"id": expected_id},
            "telescope_type": {"id": expected_id},
        }
        obj.update(**dict_)
        self.session.commit()
        self.assertEqual(expected_id, obj.telescope_type.id)
        self.assertEqual(expected_id, obj.organisation.id)
        self.assertEqual(dt, pendulum.instance(obj.created))
        self.assertEqual(dt, pendulum.instance(obj.modified))

    def test_organisation(self):
        """Test that Organisation can be created, fetched, updates and deleted"""

        # Create Organisation
        created = pendulum.now("UTC")
        organisation = Organisation(
            name="My Organisation",
            gcp_project_id="project-id",
            gcp_download_bucket="download-bucket",
            gcp_transform_bucket="transform-bucket",
            created=created,
        )
        self.session.add(organisation)
        self.session.commit()

        # Assert created object
        expected_id = 1
        self.assertIsNotNone(organisation.id)
        self.assertEqual(organisation.id, expected_id)

        # Update Organisation
        new_name = "New Organisation"
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
        """Test that Organisation can be created from a dictionary"""

        # Create
        expected_id = 1
        dt = pendulum.now("UTC")
        dict_ = {
            "name": "My Organisation",
            "gcp_project_id": "project-id",
            "gcp_download_bucket": "download-bucket",
            "gcp_transform_bucket": "transform-bucket",
            "created": dt,
            "modified": dt,
        }
        obj = Organisation(**dict_)
        self.session.add(obj)
        self.session.commit()
        self.assertIsNotNone(obj.id)
        self.assertEqual(obj.id, expected_id)

        # Update with no new values
        obj.update(**{})
        self.session.commit()
        self.assertEqual(dict_["name"], obj.name)
        self.assertEqual(dict_["gcp_project_id"], obj.gcp_project_id)
        self.assertEqual(dict_["gcp_download_bucket"], obj.gcp_download_bucket)
        self.assertEqual(dict_["gcp_transform_bucket"], obj.gcp_transform_bucket)
        self.assertEqual(dt, pendulum.instance(obj.created))
        self.assertEqual(dt, pendulum.instance(obj.modified))

        # Update
        dt = pendulum.now("UTC")
        dict_ = {
            "name": "My Organisation 2",
            "gcp_project_id": "project-id-2",
            "gcp_download_bucket": "download-bucket-2",
            "gcp_transform_bucket": "transform-bucket-2",
            "modified": dt,
        }
        obj.update(**dict_)
        self.session.commit()
        self.assertEqual(dict_["name"], obj.name)
        self.assertEqual(dict_["gcp_project_id"], obj.gcp_project_id)
        self.assertEqual(dict_["gcp_download_bucket"], obj.gcp_download_bucket)
        self.assertEqual(dict_["gcp_transform_bucket"], obj.gcp_transform_bucket)
        self.assertEqual(dt, pendulum.instance(obj.modified))

    def test_dataset_storage(self):
        """Test that DatasetStorage can be created, fetched, updates and deleted"""

        dt = pendulum.now("UTC")
        create_telescope_types(self.session, self.telescope_types, dt)

        organisation = Organisation(
            name="My Organisation",
            gcp_project_id="project-id",
            gcp_download_bucket="download-bucket",
            gcp_transform_bucket="transform-bucket",
            created=dt,
            modified=dt,
        )
        self.session.add(organisation)
        self.session.commit()

        telescope_type = self.session.query(TelescopeType).filter(TelescopeType.id == 1).one()
        telescope = Telescope(
            name="Curtin ONIX Telescope",
            extra={"view_id": 123456},
            telescope_type=telescope_type,
            organisation=organisation,
            modified=dt,
            created=dt,
        )
        self.session.add(telescope)
        self.session.commit()

        dataset = Dataset(
            name="dataset",
            extra={},
            connection=telescope,
            created=dt,
            modified=dt,
        )

        self.session.add(dataset)
        self.session.commit()

        # Create DatasetStorage
        created = pendulum.now("UTC")
        storage = DatasetStorage(
            service="bigquery",
            address="project_id.dataset_id.table_name",
            extra={"table_type": "sharded"},
            dataset=dataset,
            created=created,
        )
        self.session.add(storage)
        self.session.commit()

        # Assert created object
        expected_id = 1
        self.assertIsNotNone(storage.id)
        self.assertEqual(storage.id, expected_id)

        # Update DatasetStorage
        storage = self.session.query(DatasetStorage).filter(DatasetStorage.id == expected_id).one()
        new_address = "new_address"
        storage.address = new_address
        self.session.commit()

        # Assert update
        storage = self.session.query(DatasetStorage).filter(DatasetStorage.id == expected_id).one()
        self.assertEqual(storage.address, new_address)

        # Delete items
        self.session.query(DatasetStorage).filter(DatasetStorage.id == expected_id).delete()
        with self.assertRaises(sqlalchemy.orm.exc.NoResultFound):
            self.session.query(DatasetStorage).filter(DatasetStorage.id == expected_id).one()

    def test_dataset_storage_from_dict(self):
        """Test that DatasetStorage can be created from a dictionary"""

        dt = pendulum.now("UTC")
        create_telescope_types(self.session, self.telescope_types, dt)

        organisation = Organisation(
            name="My Organisation",
            gcp_project_id="project-id",
            gcp_download_bucket="download-bucket",
            gcp_transform_bucket="transform-bucket",
            created=dt,
            modified=dt,
        )
        self.session.add(organisation)
        self.session.commit()

        telescope_type = self.session.query(TelescopeType).filter(TelescopeType.id == 1).one()
        telescope = Telescope(
            name="Curtin ONIX Telescope",
            extra={"view_id": 123456},
            telescope_type=telescope_type,
            organisation=organisation,
            modified=dt,
            created=dt,
        )
        self.session.add(telescope)
        self.session.commit()

        dataset = Dataset(
            name="dataset",
            extra={},
            connection=telescope,
            created=dt,
            modified=dt,
        )

        self.session.add(dataset)
        self.session.commit()

        # Create
        expected_id = 1
        dt = pendulum.now("UTC")
        dict_ = {
            "dataset": {"id": 1},
            "service": "google",
            "address": "project.dataset.table",
            "extra": {"table_type": "sharded"},
            "created": dt,
            "modified": dt,
        }
        obj = DatasetStorage(**dict_)
        self.session.add(obj)
        self.session.commit()
        self.assertIsNotNone(obj.id)
        self.assertEqual(obj.id, expected_id)

        # Update with no new values
        obj.update(**{})
        self.session.commit()
        self.assertEqual(dict_["service"], obj.service)
        self.assertEqual(dict_["address"], obj.address)
        self.assertEqual(dict_["extra"], obj.extra)
        self.assertEqual(dt, pendulum.instance(obj.created))
        self.assertEqual(dt, pendulum.instance(obj.modified))

        # Update
        dt = pendulum.now("UTC")
        dict_ = {
            "service": "google",
            "address": "project.dataset.table",
            "extra": {"table_type": "sharded"},
            "dataset": {"id": 1},
            "modified": dt,
        }
        obj.update(**dict_)
        self.session.commit()
        self.assertEqual(dict_["service"], obj.service)
        self.assertEqual(dict_["address"], obj.address)
        self.assertEqual(dict_["extra"], obj.extra)
        self.assertEqual(dt, pendulum.instance(obj.modified))

    def test_dataset_release(self):
        """Test that DatasetRelease can be created, fetched, updates and deleted"""

        dt = pendulum.now("UTC")
        create_telescope_types(self.session, self.telescope_types, dt)

        organisation = Organisation(
            name="My Organisation",
            gcp_project_id="project-id",
            gcp_download_bucket="download-bucket",
            gcp_transform_bucket="transform-bucket",
            created=dt,
            modified=dt,
        )
        self.session.add(organisation)
        self.session.commit()

        telescope_type = self.session.query(TelescopeType).filter(TelescopeType.id == 1).one()
        telescope = Telescope(
            name="Curtin ONIX Telescope",
            extra={"view_id": 123456},
            telescope_type=telescope_type,
            organisation=organisation,
            modified=dt,
            created=dt,
        )
        self.session.add(telescope)
        self.session.commit()

        dataset = Dataset(
            name="dataset",
            extra={},
            connection=telescope,
            created=dt,
            modified=dt,
        )

        self.session.add(dataset)
        self.session.commit()

        # Create DatasetRelease
        created = pendulum.now("UTC")
        dt = pendulum.now("UTC")
        release = DatasetRelease(
            schema_version="schema",
            schema_version_alt="schema_alt",
            start_date=dt,
            end_date=dt,
            ingestion_start=dt,
            ingestion_end=dt,
            created=created,
            dataset=dataset,
        )
        self.session.add(release)
        self.session.commit()

        # Assert created object
        expected_id = 1
        self.assertIsNotNone(release.id)
        self.assertEqual(release.id, expected_id)

        # Update DatasetRelease
        release = self.session.query(DatasetRelease).filter(DatasetRelease.id == expected_id).one()
        new_schema = "new_schema"
        release.schema_version = new_schema
        self.session.commit()

        # Assert update
        release = self.session.query(DatasetRelease).filter(DatasetRelease.id == expected_id).one()
        self.assertEqual(release.schema_version, new_schema)

        # Delete items
        self.session.query(DatasetRelease).filter(DatasetRelease.id == expected_id).delete()
        with self.assertRaises(sqlalchemy.orm.exc.NoResultFound):
            self.session.query(DatasetRelease).filter(DatasetRelease.id == expected_id).one()

    def test_dataset_release_from_dict(self):
        """Test that DatasetRelease can be created from a dictionary"""

        dt = pendulum.now("UTC")
        create_telescope_types(self.session, self.telescope_types, dt)

        organisation = Organisation(
            name="My Organisation",
            gcp_project_id="project-id",
            gcp_download_bucket="download-bucket",
            gcp_transform_bucket="transform-bucket",
            created=dt,
            modified=dt,
        )
        self.session.add(organisation)
        self.session.commit()

        telescope_type = self.session.query(TelescopeType).filter(TelescopeType.id == 1).one()
        telescope = Telescope(
            name="Curtin ONIX Telescope",
            extra={"view_id": 123456},
            telescope_type=telescope_type,
            organisation=organisation,
            modified=dt,
            created=dt,
        )
        self.session.add(telescope)
        self.session.commit()

        dataset = Dataset(
            name="dataset",
            extra={},
            connection=telescope,
            created=dt,
            modified=dt,
        )

        self.session.add(dataset)
        self.session.commit()

        # Create
        expected_id = 1
        dt = pendulum.now("UTC")
        dict_ = {
            "schema_version": "schema",
            "schema_version_alt": "schema_alt",
            "start_date": dt,
            "end_date": dt,
            "ingestion_start": dt,
            "ingestion_end": dt,
            "modified": dt,
            "created": dt,
            "dataset": {"id": expected_id},
        }
        obj = DatasetRelease(**dict_)
        self.session.add(obj)
        self.session.commit()
        self.assertIsNotNone(obj.id)
        self.assertEqual(obj.id, expected_id)

        # Update with no new values
        obj.update(**{})
        self.session.commit()
        self.assertEqual(dict_["schema_version"], obj.schema_version)
        self.assertEqual(dict_["schema_version_alt"], obj.schema_version_alt)

        self.assertEqual(dt, pendulum.instance(obj.start_date))
        self.assertEqual(dt, pendulum.instance(obj.end_date))
        self.assertEqual(dt, pendulum.instance(obj.ingestion_start))
        self.assertEqual(dt, pendulum.instance(obj.ingestion_end))

        self.assertEqual(dt, pendulum.instance(obj.created))
        self.assertEqual(dt, pendulum.instance(obj.modified))

        # Update
        dt = pendulum.now("UTC")
        dict_ = {
            "schema_version": "schema",
            "schema_version_alt": "schema_alt",
            "start_date": dt,
            "end_date": dt,
            "ingestion_start": dt,
            "ingestion_end": dt,
            "modified": dt,
            "dataset": {"id": expected_id},
        }
        obj.update(**dict_)
        self.session.commit()
        self.assertEqual(dict_["schema_version"], obj.schema_version)
        self.assertEqual(dict_["schema_version_alt"], obj.schema_version_alt)
        self.assertEqual(dt, pendulum.instance(obj.start_date))
        self.assertEqual(dt, pendulum.instance(obj.end_date))
        self.assertEqual(dt, pendulum.instance(obj.ingestion_start))
        self.assertEqual(dt, pendulum.instance(obj.ingestion_end))
        self.assertEqual(dt, pendulum.instance(obj.modified))

    def test_dataset(self):
        """Test that Dataset can be created, fetched, updated and deleted"""

        dt = pendulum.now("UTC")
        create_telescope_types(self.session, self.telescope_types, dt)

        organisation = Organisation(
            name="My Organisation",
            gcp_project_id="project-id",
            gcp_download_bucket="download-bucket",
            gcp_transform_bucket="transform-bucket",
            created=dt,
            modified=dt,
        )
        self.session.add(organisation)
        self.session.commit()

        telescope_type = self.session.query(TelescopeType).filter(TelescopeType.id == 1).one()
        telescope = Telescope(
            name="Curtin ONIX Telescope",
            extra={"view_id": 123456},
            telescope_type=telescope_type,
            organisation=organisation,
            modified=dt,
            created=dt,
        )
        self.session.add(telescope)
        self.session.commit()

        dataset = Dataset(
            name="dataset",
            extra={},
            connection=telescope,
            created=dt,
            modified=dt,
        )

        self.session.add(dataset)
        self.session.commit()

        # Assert created object
        self.assertIsNotNone(dataset.id)
        self.assertEqual(dataset.id, 1)

        # Update Dataset
        newname = "newname"

        def get_dataset():
            return self.session.query(Dataset).filter(Dataset.id == 1).one()

        dataset = get_dataset()
        dataset.name = newname
        self.session.commit()

        # Assert update
        dataset = get_dataset()
        self.assertEqual(dataset.name, newname)

        # Delete items
        self.session.query(Dataset).filter(Dataset.id == 1).delete()
        with self.assertRaises(sqlalchemy.orm.exc.NoResultFound):
            get_dataset()

    def test_dataset_from_dict(self):
        """Test that Dataset can be created and updated from a dictionary"""

        dt = pendulum.now("UTC")
        create_telescope_types(self.session, self.telescope_types, dt)

        organisation = Organisation(
            name="My Organisation",
            gcp_project_id="project-id",
            gcp_download_bucket="download-bucket",
            gcp_transform_bucket="transform-bucket",
            created=dt,
            modified=dt,
        )
        self.session.add(organisation)
        self.session.commit()

        telescope_type = self.session.query(TelescopeType).filter(TelescopeType.id == 1).one()
        telescope = Telescope(
            name="Curtin ONIX Telescope",
            extra={"view_id": 123456},
            telescope_type=telescope_type,
            organisation=organisation,
            modified=dt,
            created=dt,
        )
        self.session.add(telescope)
        self.session.commit()

        # Create Dataset
        expected_id = 1
        dt = pendulum.now("UTC")
        dict_ = {
            "connection": {"id": telescope.id},
            "name": "name",
            "extra": {},
            "created": dt,
            "modified": dt,
        }

        obj = Dataset(**dict_)
        self.session.add(obj)
        self.session.commit()
        self.assertIsNotNone(obj.id)
        self.assertEqual(obj.id, expected_id)

        # Update with no new values
        obj.update(**{})
        self.session.commit()
        self.assertEqual(expected_id, obj.connection.id)
        self.assertEqual(dt, pendulum.instance(obj.created))
        self.assertEqual(dt, pendulum.instance(obj.modified))

        # Update Dataset
        expected_id = 2
        telescope = Telescope(
            name="Curtin ONIX Telescope",
            extra={"view_id": 123456},
            telescope_type=telescope_type,
            organisation=organisation,
            modified=dt,
            created=dt,
        )
        self.session.add(telescope)
        self.session.commit()

        dict_ = {
            "connection": {"id": expected_id},
            "name": "name",
            "extra": {},
            "modified": dt,
        }
        obj.update(**dict_)
        self.session.commit()
        self.assertEqual(expected_id, obj.connection.id)
        self.assertEqual(dt, pendulum.instance(obj.created))
        self.assertEqual(dt, pendulum.instance(obj.modified))
