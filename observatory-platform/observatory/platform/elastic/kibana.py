# Copyright 2018 Elasticsearch BV
# Copyright 2020, 2021 Curtin University
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

# Some of the docstrings are based on the https://github.com/elastic/kibana/tree/7.10/docs/api documentation which is
# licensed under the Apache 2.0 license according to the license notice:
# https://github.com/elastic/kibana/blob/7.10/LICENSE.txt

# Author: James Diprose

import json
import logging
from enum import Enum
from posixpath import join as urljoin
from typing import Dict, List
from urllib.parse import urlparse, urljoin

import requests


def parse_kibana_url(url):
    """ Parse a Kibana URL into host + post and username and password.

    :param url: the full url.
    :return: the host + port, username and password.
    """

    parts = urlparse(url)
    username = parts.username
    password = parts.password
    parts = parts._replace(netloc=parts.hostname)
    kibana_host = parts.geturl()

    return kibana_host, username, password


class ObjectType(Enum):
    """ Valid Kibana saved object types """

    visualization = "visualization"
    dashboard = "dashboard"
    search = "search"
    index_pattern = "index-pattern"
    config = "config"
    timelion_sheet = "timelion-sheet"


class Kibana:
    headers = {
        "Content-Type": "application/json",
        "kbn-xsrf": "true",
    }

    def __init__(self, host: str = "http://kibana:5601/", username: str = None, password: str = None):
        """ Create a Kibana API client.

        :param host: the host including the hostname and port.
        :param username: the Kibana username.
        :param password: the Kibana password.
        """

        self.host = host
        self.username = username
        self.password = password

        if self.username is not None and self.password is not None:
            self.auth = (self.username, self.password)

    def create_space(
        self,
        space_id: str,
        name: str,
        description: str = None,
        disabled_features: List = None,
        initials: str = None,
        color: str = None,
        image_url: str = None,
    ) -> bool:
        """ Create a Kibana space.

        :param space_id: the space ID.
        :param name: the display name for the space.
        :param description: the description for the space.
        :param disabled_features: the list of disabled features for the space.
        :param initials: the initials shown in the space avatar. By default, the initials are automatically generated
        from the space name. Initials must be 1 or 2 characters.
        :param color: the hexadecimal color code used in the space avatar. By default, the color is automatically
        generated from the space name.
        :param image_url: The data-URL encoded image to display in the space avatar. If specified, initials will not
        be displayed, and the color will be visible as the background color for transparent images. For best results,
        your image should be 64x64. Images will not be optimized by this API call, so care should be taken when using
        custom images.
        :return: whether the Kibana space was created successfully or not.
        """

        # Construct body
        body = {"id": space_id, "name": name}
        if description is not None:
            body["description"] = description
        if disabled_features is not None:
            body["disabledFeatures"] = disabled_features
        if initials is not None:
            body["initials"] = initials
        if color is not None:
            body["color"] = color
        if image_url is not None:
            body["imageUrl"] = image_url

        url = self._make_spaces_url()
        response = requests.post(url, headers=self.headers, data=json.dumps(body), auth=self.auth)

        success = response.status_code == 200
        if not success:
            logging.error(response.text)

        return success

    def delete_space(self, space_id: str) -> bool:
        """ Delete a Kibana space.

        :param space_id: the space ID.
        :return: whether the Kibana space was deleted successfully or not.
        """

        url = self._make_spaces_url(space_id=space_id)
        response = requests.delete(url, headers=self.headers, auth=self.auth)

        success = response.status_code == 200
        if not success:
            logging.error(response.text)

        return success

    def create_object(
        self,
        object_type: ObjectType,
        object_id: str,
        attributes: Dict,
        space_id: str = None,
        overwrite: bool = False,
        initial_namespaces: List[str] = None,
        exists_ok: bool = False,
    ) -> bool:
        """ Create a Kibana saved object.

        :param object_type: the type of object to create.
        :param object_id: the object ID.
        :param attributes: the data that you want to create.
        :param space_id: the space ID. If space_id is not provided the default space is used.
        :param overwrite: when true, overwrites the document with the same ID.
        :param initial_namespaces: identifiers for the spaces in which this object is created. If this is provided, the
        object is created only in the explicitly defined spaces. If this is not provided, the object is created in the
        current space (default behavior).
        :param exists_ok: if the object already exists then consider creation successful.
        :return: whether the Kibana saved object was created successfully or not.
        """

        body = {"attributes": attributes}

        if initial_namespaces is not None:
            body["initialNamespaces"] = initial_namespaces

        params = (("overwrite", overwrite),)

        url = self._make_saved_object_url(object_type, object_id, space_id=space_id)
        response = requests.post(url, headers=self.headers, params=params, data=json.dumps(body), auth=self.auth)

        success = response.status_code == 200 or (exists_ok and response.status_code == 409)
        if not success:
            logging.error(response.text)

        return success

    def delete_object(self, object_type: ObjectType, object_id: str, space_id: str = None, force: bool = False) -> bool:
        """ Delete a Kibana saved object.

        :param object_type: the object type.
        :param object_id: The object ID that you want to delete.
        :param space_id: the space ID. If not provided the default space is used.
        :param force: when true, forces an object to be deleted if it exists in multiple namespaces.
        :return: whether Kibana object was deleted successfully or not.
        """

        url = self._make_saved_object_url(object_type, object_id, space_id=space_id)

        params = (("force", force),)

        response = requests.delete(url, headers=self.headers, params=params, auth=self.auth)

        success = response.status_code == 200
        if not success:
            logging.error(response.text)

        return success

    def _make_spaces_url(self, space_id: str = None) -> str:
        """ Make a URL for using the Kibana spaces REST API.

        :param space_id: the space ID.
        :return: the URL.
        """

        parts = ["api", "spaces", "space"]
        if space_id is not None:
            parts.append(space_id)
        return urljoin(self.host, *parts)

    def _make_saved_object_url(self, object_type: ObjectType, object_id: str, space_id: str = None) -> str:
        """ Make a URL for using the Kibana saved object API.

        :param object_type: the Kibana saved object type.
        :param object_id: the object ID.
        :param space_id: the space ID.
        :return: the URL.
        """

        # Make URL
        parts = []
        if space_id is not None:
            parts += ["s", space_id]
        parts += ["api", "saved_objects", object_type.value, object_id]
        return urljoin(self.host, *parts)
