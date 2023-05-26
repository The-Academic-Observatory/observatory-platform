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

import pendulum
import sqlalchemy
from sqlalchemy.orm import scoped_session
from sqlalchemy.pool import StaticPool

from observatory.api.server.orm import (
    DatasetRelease,
    create_session,
    fetch_db_object,
    set_session,
    to_datetime_utc,
)


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

    def setUp(self) -> None:
        """Create the SQLAlchemy Session"""

        self.session = create_session(uri=self.uri, connect_args={"check_same_thread": False}, poolclass=StaticPool)
        set_session(self.session)

    def test_fetch_db_item(self):
        """Test fetch_db_object"""

        # Body is None
        self.assertEqual(None, fetch_db_object(DatasetRelease, None))

        # Body is instance of cls
        dt = pendulum.now("UTC")
        dict_ = {
            "dag_id": "doi_workflow",
            "dataset_id": "doi",
            "dag_run_id": "scheduled__2023-03-26T00:00:00+00:00",
            "snapshot_date": dt,
            "extra": {"hello": "world"},
            "modified": dt,
        }
        obj = DatasetRelease(**dict_)
        self.assertEqual(obj, fetch_db_object(DatasetRelease, obj))

        # Body is Dict and no id
        with self.assertRaises(AttributeError):
            fetch_db_object(DatasetRelease, {"dag_id": "doi_workflow"})

        # Body is Dict, has id and is not found
        with self.assertRaises(ValueError):
            fetch_db_object(DatasetRelease, {"id": 1})

        # Body is Dict, has id and is found
        self.session.add(obj)
        self.session.commit()
        obj_fetched = fetch_db_object(DatasetRelease, {"id": 1})
        self.assertEqual(obj, obj_fetched)

        # Body is wrong type
        with self.assertRaises(ValueError):
            fetch_db_object(DatasetRelease, "hello")

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

    def test_dataset_release(self):
        """Test that DatasetRelease can be created, fetched, updates and deleted"""

        # Create DatasetRelease
        created = pendulum.now("UTC")
        dt = pendulum.now("UTC")
        release = DatasetRelease(
            dag_id="doi_workflow",
            dataset_id="doi",
            dag_run_id="scheduled__2023-03-26T00:00:00+00:00",
            data_interval_start=dt,
            data_interval_end=dt,
            snapshot_date=dt,
            changefile_start_date=dt,
            changefile_end_date=dt,
            sequence_start=1,
            sequence_end=10,
            extra={"hello": "world"},
            created=created,
        )
        self.session.add(release)
        self.session.commit()

        # Assert created object
        expected_id = 1
        self.assertIsNotNone(release.id)
        self.assertEqual(release.id, expected_id)

        # Update DatasetRelease
        release = self.session.query(DatasetRelease).filter(DatasetRelease.id == expected_id).one()
        release.snapshot_date = pendulum.datetime(1900, 1, 1)
        self.session.commit()

        # Assert update
        release = self.session.query(DatasetRelease).filter(DatasetRelease.id == expected_id).one()
        self.assertEqual(pendulum.instance(release.snapshot_date), pendulum.datetime(1900, 1, 1))

        # Delete items
        self.session.query(DatasetRelease).filter(DatasetRelease.id == expected_id).delete()
        with self.assertRaises(sqlalchemy.orm.exc.NoResultFound):
            self.session.query(DatasetRelease).filter(DatasetRelease.id == expected_id).one()

    def test_dataset_release_from_dict(self):
        """Test that DatasetRelease can be created from a dictionary"""

        # Create
        expected_id = 1
        dt = pendulum.now("UTC")
        dict_ = {
            "dag_id": "doi_workflow",
            "dataset_id": "doi",
            "dag_run_id": "scheduled__2023-03-26T00:00:00+00:00",
            "data_interval_start": dt,
            "data_interval_end": dt,
            "snapshot_date": dt,
            "changefile_start_date": dt,
            "changefile_end_date": dt,
            "sequence_start": 1,
            "sequence_end": 10,
            "extra": {"hello": "world"},
            "modified": dt,
            "created": dt,
        }
        obj = DatasetRelease(**dict_)
        self.session.add(obj)
        self.session.commit()
        self.assertIsNotNone(obj.id)
        self.assertEqual(obj.id, expected_id)

        # Update with no new values
        obj.update(**{})
        self.session.commit()
        self.assertEqual("doi_workflow", obj.dag_id)
        self.assertEqual(dt, pendulum.instance(obj.snapshot_date))

        # Update
        dt = pendulum.now("UTC")
        dict_ = {"dag_id": "doi_workflow_2", "snapshot_date": dt}
        obj.update(**dict_)
        self.session.commit()
        self.assertEqual("doi_workflow_2", obj.dag_id)
        self.assertEqual(dt, pendulum.instance(obj.snapshot_date))
