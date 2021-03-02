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
from typing import Dict

from sqlalchemy.pool import StaticPool

from observatory.platform.api.api import make_response
from observatory.platform.api.app import create_app, set_session
from observatory.platform.api.orm import create_session, ConnectionType, Connection, Organisation


class TestApp(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestApp, self).__init__(*args, **kwargs)
        self.uri = 'sqlite://'
        self.content_type = 'application/json'

    def setUp(self) -> None:
        self.session = create_session(uri=self.uri, connect_args={'check_same_thread': False}, poolclass=StaticPool)
        set_session(self.session)

    def endpoint_test(self, endpoint: str, post_data: Dict, put_create_data: Dict, cls):
        flask_app = create_app()
        with flask_app.app.test_client() as test_client:
            # POST
            post_expected_id = 1
            response = test_client.post(endpoint,
                                        data=json.dumps(post_data),
                                        content_type=self.content_type)
            status_code = 201
            description = f'Created: {cls.__name__} with id {post_expected_id}'
            self.assertEqual(response.status_code, 201)

            expected = make_response(status_code, description, data={'id': post_expected_id}, json=False)[0]
            actual = json.loads(response.data)
            self.assertDictEqual(expected, actual)

            # GET
            response = test_client.get(endpoint,
                                       query_string={'id': post_expected_id},
                                       content_type=self.content_type)

            status_code = 200
            description = f'Found: {cls.__name__} with id {post_expected_id}'
            subset = post_data.copy()
            subset['id'] = post_expected_id
            expected = make_response(status_code, description, data=subset, json=False)[0]
            actual = json.loads(response.data)
            self.assertEqual(status_code, response.status_code)
            self.assertDictContainsSubset(expected['data'], actual['data'])
            self.assertDictEqual(expected['response'], actual['response'])

            # PUT: update
            # response = test_client.put(endpoint,
            #                            data=json.dumps({'id': expected_id, 'name': new_name}),
            #                            content_type=self.content_type)
            # self.assertEqual(response.status_code, 200)

            # PUT: create
            response = test_client.put(endpoint,
                                       data=json.dumps(put_create_data),
                                       content_type=self.content_type)

            put_expected_id = 2
            status_code = 201
            description = f'Created: {cls.__name__} with id {put_expected_id}'
            expected = make_response(status_code, description, data={'id': put_expected_id}, json=False)[0]
            actual = json.loads(response.data)
            self.assertEqual(status_code, response.status_code)
            self.assertDictEqual(expected, actual)

            # GET: many
            response = test_client.get(f'{endpoint}s',
                                       query_string={'limit': 10},
                                       content_type=self.content_type)

            status_code = 200
            items = json.loads(response.data)['data']
            self.assertIsInstance(items, list)
            self.assertEqual(status_code, response.status_code)
            self.assertEqual(2, len(items))

            # DELETE
            response = test_client.delete(endpoint,
                                          query_string={'id': post_expected_id},
                                          content_type=self.content_type)
            status_code = 200
            description = f'Deleted: {cls.__name__} with id {post_expected_id}'
            expected = make_response(status_code, description, json=False)[0]
            actual = json.loads(response.data)
            self.assertEqual(status_code, response.status_code)
            self.assertDictEqual(expected, actual)

            # DELETE: test that not found when try to delete again
            response = test_client.delete(endpoint,
                                          query_string={'id': post_expected_id},
                                          content_type=self.content_type)
            status_code = 404
            description = f'Not found: {cls.__name__} with id {post_expected_id}'
            expected = make_response(status_code, description, json=False)[0]
            actual = json.loads(response.data)
            self.assertEqual(status_code, response.status_code)
            self.assertDictEqual(expected, actual)

    def test_connection_type(self):
        endpoint = '/connection_type'
        post_data = {'name': 'GCP'}
        put_create_data = {'name': 'Scopus'}
        self.endpoint_test(endpoint, post_data, put_create_data, ConnectionType)

    def test_connection(self):
        endpoint = '/connection'
        post_data = {
            'name': 'My Connection 1',
            'connection_type': {'name': 'GCP'},
            'airflow_connection_id': 1
        }
        put_create_data = {
            'name': 'My Connection 2',
            'connection_type': {'name': 'Scopus'},
            'airflow_connection_id': 2
        }
        self.endpoint_test(endpoint, post_data, put_create_data, Connection)

    def test_organisation(self):
        endpoint = '/organisation'
        post_data = {
            'name': 'Curtin',
            'gcp_project_id': 'project-id',
            'gcp_download_bucket': 'download-bucket',
            'gcp_transform_bucket': 'transform-bucket',
            'connections': [
                {
                    'name': 'Curtin ONIX',
                    'connection_type': {
                        'name': 'ONIX'
                    },
                    'airflow_connection_id': 1
                },
                {
                    'name': 'Curtin GCP',
                    'connection_type': {
                        'name': 'GCP'
                    },
                    'airflow_connection_id': 2
                }
            ]}
        put_create_data = {
            'name': 'UoA',
            'gcp_project_id': 'project-id',
            'gcp_download_bucket': 'download-bucket',
            'gcp_transform_bucket': 'transform-bucket',
            'connections': [
                {
                    'name': 'UoA ONIX',
                    'connection_type': {
                        'id': 1
                    },
                    'airflow_connection_id': 3
                },
                {
                    'name': 'UoA GCP',
                    'connection_type': {
                        'id': 2
                    },
                    'airflow_connection_id': 4
                }
            ]}
        self.endpoint_test(endpoint, post_data, put_create_data, Organisation)
