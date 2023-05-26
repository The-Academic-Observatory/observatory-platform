# Copyright 2020 Curtin University
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
#
#
# Author: Tuan Chien


import os
from urllib.parse import urlparse

from observatory.api.client import ApiClient, Configuration
from observatory.api.client.api.observatory_api import ObservatoryApi


def get_api_client(host: str = "localhost", port: int = 5002, api_key: dict = None) -> ObservatoryApi:
    """Get an API client.
    :param host: URI for api server.
    :param port: Server port.
    :param api_key: API key.
    :return: ObservatoryApi object.
    """

    if "API_URI" in os.environ:
        fields = urlparse(os.environ["API_URI"])
        uri = f"{fields.scheme}://{fields.hostname}:{fields.port}"
        api_key = {"api_key": fields.password}
    else:
        uri = f"http://{host}:{port}"

    configuration = Configuration(host=uri, api_key=api_key)
    api_client = ApiClient(configuration)
    api = ObservatoryApi(api_client=api_client)
    return api
