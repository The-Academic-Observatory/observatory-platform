"""
    Observatory API

    The REST API for managing and accessing data from the Observatory Platform.   # noqa: E501

    The version of the OpenAPI document: 1.0.0
    Contact: agent@observatory.academy
    Generated by: https://openapi-generator.tech
"""

import contextlib
import os
import threading
import unittest
from datetime import datetime

import vcr
from sqlalchemy.pool import StaticPool
from werkzeug.serving import make_server

import observatory.api.server.orm as orm
from observatory.api.client import Configuration, ApiClient
from observatory.api.client.api.observatory_api import ObservatoryApi  # noqa: E501
from observatory.api.client.exceptions import NotFoundException
# from observatory.api.client.model.organisation import Organisation
# from observatory.api.client.model.telescope import Telescope
from observatory.api.client.model.telescope_type import TelescopeType
from observatory.api.server.api import create_app
from observatory.api.server.orm import TelescopeType
from observatory.api.server.orm import create_session, set_session


class ObservatoryApiEnvironment:
    IMAGE_NAME = 'observatory-api-test-image'
    CONTAINER_NAME = 'observatory-api-container'
    DATABASE_NAME = 'observatory.db'

    def __init__(self, host: str = "localhost", port: int = 5000):
        self.host = host
        self.port = port
        self.db_uri = 'sqlite://'
        self.session = None
        self.server = None
        self.server_thread = None

    @contextlib.contextmanager
    def create(self):
        """ Make and destroy an Observatory API isolated environment, which involves:

        * Creating an in memory SQLite database for the API backend to connect to
        * Start the Connexion / Flask app

        :yield: None.
        """

        try:
            # Connect to in memory SQLite database with SQLAlchemy
            self.session = create_session(uri=self.db_uri,
                                          connect_args={'check_same_thread': False},
                                          poolclass=StaticPool)
            set_session(self.session)

            # Create the Connexion App and start the server
            app = create_app()
            self.server = make_server("localhost", 5000, app)
            self.server_thread = threading.Thread(target=self.server.serve_forever)
            self.server_thread.start()
            yield
        finally:
            # Stop server and wait for server thread to join
            self.server.shutdown()
            self.server_thread.join()


class TestObservatoryApi(unittest.TestCase):
    """ObservatoryApi unit test stubs"""

    def setUp(self):
        self.host = "localhost"
        self.port = 5000
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501

    def tearDown(self):
        pass

    def test_hello(self):
        env = ObservatoryApiEnvironment(host=self.host, port=self.port)
        with env.create() as temp_dir:
            print(temp_dir)
            a = 1

    def test_delete_organisation(self):
        """Test case for delete_organisation

        delete an Organisation  # noqa: E501
        """

        with vcr.use_cassette(os.path.join(self.vcr_cassettes_path, 'test_delete_organisation.yaml')):
            self.api.delete_organisation()

    def test_delete_telescope(self):
        """Test case for delete_telescope

        delete a Telescope  # noqa: E501
        """

        with vcr.use_cassette(os.path.join(self.vcr_cassettes_path, 'test_delete_telescope.yaml')):
            self.api.delete_telescope()

    def test_delete_telescope_type(self):
        """Test case for delete_telescope_type

        delete a TelescopeType  # noqa: E501
        """

        with vcr.use_cassette(os.path.join(self.vcr_cassettes_path, 'test_delete_telescope_type.yaml')):
            self.api.delete_telescope_type()

    def test_get_organisation(self):
        """Test case for get_organisation

        get an Organisation  # noqa: E501
        """

        with vcr.use_cassette(os.path.join(self.vcr_cassettes_path, 'test_get_organisation.yaml')):
            self.api.get_organisation()

    def test_get_organisations(self):
        """Test case for get_organisations

        Get a list of Organisations  # noqa: E501
        """

        with vcr.use_cassette(os.path.join(self.vcr_cassettes_path, 'test_get_organisations.yaml')):
            self.api.get_organisations()

    def test_get_telescope(self):
        """Test case for get_telescope

        get a Telescope  # noqa: E501
        """

        env = ObservatoryApiEnvironment()
        with env.create():
            with self.assertRaises(NotFoundException):
                self.api.get_telescope(expected_id)

            # Add Telescope

    def test_get_telescope_type(self):
        """Test case for get_telescope_type

        get a TelescopeType  # noqa: E501
        """

        env = ObservatoryApiEnvironment(host=self.host, port=self.port)
        with env.create():
            expected_id = 1

            # Assert that TelescopeType with given id does not exist
            with self.assertRaises(NotFoundException):
                self.api.get_telescope_type(expected_id)

            # Add TelescopeType
            name = 'ONIX Telescope'
            dt = datetime.utcnow()
            env.session.add(orm.TelescopeType(name=name, created=dt, modified=dt))
            env.session.commit()

            # Assert that TelescopeType with given id exists
            telescope_type = self.api.get_telescope_type(expected_id)
            self.assertIsInstance(telescope_type, TelescopeType)
            self.assertEqual(expected_id, telescope_type.id)
            self.assertEqual(name, telescope_type.name)
            self.assertEqual(dt, telescope_type.created)
            self.assertEqual(dt, telescope_type.modified)

    def test_get_telescope_types(self):
        """Test case for get_telescope_types

        Get a list of TelescopeType objects  # noqa: E501
        """

            self.api.get_telescope_types()

    def test_get_telescopes(self):
        """Test case for get_telescopes

        Get a list of Telescope objects  # noqa: E501
        """

            self.api.get_telescopes()

    def test_post_organisation(self):
        """Test case for post_organisation

        create an Organisation  # noqa: E501
        """

            self.api.post_organisation()

    def test_post_telescope(self):
        """Test case for post_telescope

        create a Telescope  # noqa: E501
        """


            self.api.post_telescope()

    def test_post_telescope_type(self):
        """Test case for post_telescope_type

        create a TelescopeType  # noqa: E501
        """
            self.api.post_telescope_type()

    def test_put_organisation(self):
        """Test case for put_organisation

        create or update an Organisation  # noqa: E501
        """

            self.api.put_organisation()

    def test_put_telescope(self):
        """Test case for put_telescope

        create or update a Telescope  # noqa: E501
        """

            self.api.put_telescope()

    def test_put_telescope_type(self):
        """Test case for put_telescope_type

        create or update a TelescopeType  # noqa: E501
        """

            self.api.put_telescope_type()

    def test_queryv1(self):
        """Test case for queryv1

        Search the Observatory API  # noqa: E501
        """

            self.api.queryv1()


if __name__ == '__main__':
    unittest.main()
