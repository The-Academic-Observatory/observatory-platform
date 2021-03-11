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

import json
import unittest
from typing import Dict, ClassVar

from sqlalchemy.pool import StaticPool

from observatory.api.api import make_response, create_app
from observatory.api.orm import create_session, set_session, TelescopeType, Telescope, Organisation


class TestApp(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestApp, self).__init__(*args, **kwargs)
        self.uri = 'sqlite://'
        self.content_type = 'application/json'
        self.version = 'v1'

    def setUp(self) -> None:
        self.session = create_session(uri=self.uri, connect_args={'check_same_thread': False}, poolclass=StaticPool)
        set_session(self.session)

    def test_endpoints(self):
        # TelescopeType
        endpoint_telescope_type = f'/{self.version}/telescope_type'
        post_expected_id = 1
        put_create_expected_id = 2
        post_data = {'name': 'ONIX Telescope'}
        put_create_data = {'name': 'Scopus Telescope'}
        put_update_data = {'id': put_create_expected_id, 'name': 'WoS Telescope'}
        self.post_test(TelescopeType, endpoint_telescope_type, post_data, post_expected_id)
        self.get_test(TelescopeType, endpoint_telescope_type, post_expected_id)
        self.put_create_test(TelescopeType, endpoint_telescope_type, put_create_data, put_create_expected_id)
        self.put_update_test(TelescopeType, endpoint_telescope_type, put_update_data, put_create_expected_id)
        self.put_gets_test(endpoint_telescope_type, 2)

        # Organisation
        endpoint_organisation = f'/{self.version}/organisation'
        post_data = {
            'name': 'Curtin',
            'gcp_project_id': 'project-id',
            'gcp_download_bucket': 'download-bucket',
            'gcp_transform_bucket': 'transform-bucket'
        }
        put_create_data = {
            'name': 'UoA',
            'gcp_project_id': 'project-id',
            'gcp_download_bucket': 'download-bucket',
            'gcp_transform_bucket': 'transform-bucket'
        }
        put_update_data = {
            'id': put_create_expected_id,
            'name': 'Sydney'
        }
        self.post_test(Organisation, endpoint_organisation, post_data, post_expected_id)
        self.get_test(Organisation, endpoint_organisation, post_expected_id)
        self.put_create_test(Organisation, endpoint_organisation, put_create_data, put_create_expected_id)
        self.put_update_test(Organisation, endpoint_organisation, put_update_data, put_create_expected_id)
        self.put_gets_test(endpoint_organisation, 2)

        # Connection
        endpoint_telescope = f'/{self.version}/telescope'
        post_data = {
            'organisation': {'id': post_expected_id},
            'telescope_type': {'id': post_expected_id}
        }
        put_create_data = {
            'organisation': {'id': post_expected_id},
            'telescope_type': {'id': post_expected_id}
        }
        put_update_data = {
            'id': put_create_expected_id,
            'organisation': {'id': put_create_expected_id},
            'telescope_type': {'id': put_create_expected_id}
        }
        self.post_test(Telescope, endpoint_telescope, post_data, post_expected_id)
        self.get_test(Telescope, endpoint_telescope, post_expected_id)
        self.put_create_test(Telescope, endpoint_telescope, put_create_data, put_create_expected_id)
        self.put_update_test(Telescope, endpoint_telescope, put_update_data, put_create_expected_id)
        self.put_gets_test(endpoint_telescope, 2, query_string={'limit': 10})
        self.put_gets_test(endpoint_telescope, 1, query_string={'limit': 10,
                                                                'telescope_type_id': post_expected_id})
        self.put_gets_test(endpoint_telescope, 1, query_string={'limit': 10,
                                                                'organisation_id': post_expected_id})
        self.put_gets_test(endpoint_telescope, 1, query_string={'limit': 10,
                                                                'telescope_type_id': post_expected_id,
                                                                'organisation_id': post_expected_id})

        # Test delete methods
        self.delete_test(TelescopeType, endpoint_telescope_type, put_create_expected_id)
        self.delete_test(Organisation, endpoint_organisation, put_create_expected_id)
        self.delete_test(Telescope, endpoint_telescope, put_create_expected_id)

    def get_test(self, cls: ClassVar, endpoint: str, expected_id: int):
        flask_app = create_app()
        with flask_app.app.test_client() as test_client:
            # GET
            response = test_client.get(endpoint,
                                       query_string={'id': expected_id},
                                       content_type=self.content_type)

            status_code = 200
            description = f'Found: {cls.__name__} with id {expected_id}'
            expected = make_response(status_code, description, json=False)[0]
            actual = json.loads(response.data)
            self.assertEqual(status_code, response.status_code)
            self.assertDictEqual(expected['response'], actual['response'])

    def post_test(self, cls: ClassVar, endpoint: str, data: Dict, expected_id: int):
        flask_app = create_app()
        with flask_app.app.test_client() as test_client:
            # POST
            response = test_client.post(endpoint,
                                        data=json.dumps(data),
                                        content_type=self.content_type)
            status_code = 201
            description = f'Created: {cls.__name__} with id {expected_id}'
            self.assertEqual(response.status_code, 201)

            expected = make_response(status_code, description, data={'id': expected_id}, json=False)[0]
            actual = json.loads(response.data)
            self.assertDictEqual(expected, actual)

    def put_create_test(self, cls: ClassVar, endpoint: str, data: Dict, expected_id: int):
        flask_app = create_app()
        with flask_app.app.test_client() as test_client:
            response = test_client.put(endpoint,
                                       data=json.dumps(data),
                                       content_type=self.content_type)
            status_code = 201
            description = f'Created: {cls.__name__} with id {expected_id}'
            expected = make_response(status_code, description, data={'id': expected_id}, json=False)[0]
            actual = json.loads(response.data)
            self.assertEqual(status_code, response.status_code)
            self.assertDictEqual(expected, actual)

    def put_update_test(self, cls: ClassVar, endpoint: str, data: Dict, expected_id: int):
        flask_app = create_app()
        with flask_app.app.test_client() as test_client:
            # PUT: update
            response = test_client.put(endpoint,
                                       data=json.dumps(data),
                                       content_type=self.content_type)
            status_code = 200
            description = f'Updated: {cls.__name__} with id {expected_id}'
            expected = make_response(status_code, description, json=False)[0]
            actual = json.loads(response.data)
            self.assertEqual(status_code, response.status_code)
            self.assertDictEqual(expected, actual)

    def put_gets_test(self, endpoint: str, expected_num: int, query_string: Dict = None):
        if query_string is None:
            query_string = {'limit': 10}

        flask_app = create_app()
        with flask_app.app.test_client() as test_client:
            # GET: many
            response = test_client.get(f'{endpoint}s',
                                       query_string=query_string,
                                       content_type=self.content_type)

            status_code = 200
            items = json.loads(response.data)['data']
            self.assertIsInstance(items, list)
            self.assertEqual(status_code, response.status_code)
            self.assertEqual(expected_num, len(items))

    def delete_test(self, cls: ClassVar, endpoint: str, expected_id: int):
        flask_app = create_app()
        with flask_app.app.test_client() as test_client:
            # DELETE
            response = test_client.delete(endpoint,
                                          query_string={'id': expected_id},
                                          content_type=self.content_type)
            status_code = 200
            description = f'Deleted: {cls.__name__} with id {expected_id}'
            expected = make_response(status_code, description, json=False)[0]
            actual = json.loads(response.data)
            self.assertEqual(status_code, response.status_code)
            self.assertDictEqual(expected, actual)

            # DELETE: test that not found when try to delete again
            response = test_client.delete(endpoint,
                                          query_string={'id': expected_id},
                                          content_type=self.content_type)
            status_code = 404
            description = f'Not found: {cls.__name__} with id {expected_id}'
            expected = make_response(status_code, description, json=False)[0]
            actual = json.loads(response.data)
            self.assertEqual(status_code, response.status_code)
            self.assertDictEqual(expected, actual)
