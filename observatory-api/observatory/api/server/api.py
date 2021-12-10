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

# Author: Aniek Roelofs, James Diprose

from __future__ import annotations

import logging
import os
from typing import Any, ClassVar, Dict, Tuple

import connexion
import pendulum
from connexion import NoContent
from flask import jsonify
from observatory.api.cli.openapi_renderer import OpenApiRenderer
from observatory.api.server.orm import (
    Dataset,
    DatasetRelease,
    DatasetStorage,
    Organisation,
    Telescope,
    TelescopeType,
)
from sqlalchemy import and_

Response = Tuple[Any, int]
session_ = None  # Global session


def get_item(cls: ClassVar, item_id: int):
    """Get an item.

    :param cls: the SQLAlchemy Table metadata class.
    :param item_id: the id of the item.
    :return: a Response object.
    """

    item = session_.query(cls).filter(cls.id == item_id).one_or_none()
    if item is not None:
        logging.info(f"Found: {cls.__name__} with id {item_id}")
        return jsonify(item)

    body = f"Not found: {cls.__name__} with id {item_id}"
    logging.info(body)
    return body, 404


def post_item(cls: ClassVar, body: Dict) -> Response:
    """Create an item.

    :param cls: the SQLAlchemy Table metadata class.
    :param body: the item in the form of a dictionary.
    :return: a Response object.
    """

    logging.info(f"Creating item: {cls.__name__}")

    # Automatically set created and modified datetime
    now = pendulum.now("UTC")
    body["created"] = now
    body["modified"] = now

    create_item = cls(**body)
    session_.add(create_item)
    session_.flush()
    session_.commit()

    logging.info(f"Created: {cls.__name__} with id {create_item.id}")
    return jsonify(create_item), 201


def put_item(cls: ClassVar, body: Dict) -> Response:
    """Create or update an item. If the item has an id it will be updated, else it will be created.

    :param cls: the SQLAlchemy Table metadata class.
    :param body: the item in the form of a dictionary.
    :return: a Response object.
    """

    item_id = body.get("id")
    if item_id is not None:
        item = session_.query(cls).filter(cls.id == item_id).one_or_none()

        if item is not None:
            logging.info(f"Updating {cls.__name__} {item_id}")
            # Remove id and automatically set modified time
            body.pop("id")
            body["modified"] = pendulum.now("UTC")
            item.update(**body)
            session_.commit()

            logging.info(f"Updated: {cls.__name__} with id {item_id}")
            return jsonify(item), 200
        else:
            body = f"Not found: {cls.__name__} with id {item_id}"
            logging.info(body)
            return body, 404
    else:
        return post_item(cls, body)


def delete_item(cls: ClassVar, item_id: int) -> Response:
    """Delete an item.

    :param cls: the SQLAlchemy Table metadata class.
    :param id: the id of the item.
    :return: a Response object.
    """

    org = session_.query(cls).filter(cls.id == item_id).one_or_none()
    if org is not None:
        logging.info(f"Deleting {cls.__name__} {item_id}")
        session_.query(cls).filter(cls.id == item_id).delete()
        session_.commit()

        logging.info(f"Deleted: {cls.__name__} with id {item_id}")
        return NoContent, 200
    else:
        body = f"Not found: {cls.__name__} with id {item_id}"
        logging.info(body)
        return body, 404


def get_items(cls: ClassVar, limit: int) -> Response:
    """Get a list of items.

    :param cls: the SQLAlchemy Table metadata class.
    :param limit: the maximum number of items to return.
    :return: a Response object.
    """

    items = session_.query(cls).limit(limit).all()

    logging.info(f"Found items: {cls.__name__} {items}")
    return jsonify(items)


def get_telescope_type(id: int = None, type_id: str = None) -> Response:
    """Get a TelescopeType.

    :param id: the TelescopeType id.
    :param type_id: the TelescopeType type_id.
    :return: a Response object.
    """

    if (id is not None and type_id is not None) or (id is None and type_id is None):
        body = "At least one and only one of id or type_id must be specified"
        logging.error(body)
        return body, 400
    elif id is not None:
        return get_item(TelescopeType, id)
    else:
        item = session_.query(TelescopeType).filter(TelescopeType.type_id == type_id).one_or_none()
        if item is not None:
            logging.info(f"Found: TelescopeType with type_id {type_id}")
            return jsonify(item)

        body = f"Not found: TelescopeType with type_id {type_id}"
        logging.info(body)
        return body, 404


def post_telescope_type(body: Dict) -> Response:
    """Create a TelescopeType.

    :param body: the TelescopeType in the form of a dictionary.
    :return: a Response object.
    """

    return post_item(TelescopeType, body)


def put_telescope_type(body: Dict) -> Response:
    """Create or update a TelescopeType.

    :param body: the TelescopeType in the form of a dictionary.
    :return: a Response object.
    """

    return put_item(TelescopeType, body)


def delete_telescope_type(id: int) -> Response:
    """Delete a TelescopeType.

    :param id: the TelescopeType id.
    :return: a Response object.
    """

    return delete_item(TelescopeType, id)


def get_telescope_types(limit: int) -> Response:
    """Get a list of TelescopeType objects.

    :param limit: the maximum number of TelescopeType objects to return.
    :return: a Response object.
    """

    return get_items(TelescopeType, limit)


def get_telescope(id: int) -> Response:
    """Get a Telescope.

    :param id: the Telescope id.
    :return: a Response object.
    """

    return get_item(Telescope, id)


def post_telescope(body: Dict) -> Response:
    """Create a Telescope.

    :param body: the Connection in the form of a dictionary.
    :return: a Response object.
    """

    return post_item(Telescope, body)


def put_telescope(body: Dict) -> Response:
    """Create or update a Telescope.

    :param body: the Telescope in the form of a dictionary.
    :return: a Response object.
    """

    return put_item(Telescope, body)


def delete_telescope(id: int) -> Response:
    """Delete a Telescope.

    :param id: the Telescope id.
    :return: a Response object.
    """

    return delete_item(Telescope, id)


def get_telescopes(limit: int, telescope_type_id=None, organisation_id: int = None) -> Response:
    """Get a list of Telescope objects.

    :param organisation_id: the Organisation id to filter by.
    :param telescope_type_id: the TelescopeType id to filter by.
    :param limit: the maximum number of Telescope objects to return.
    :return: a Response object.
    """

    q = session_.query(Telescope)

    # Create filters based on parameters
    filters = []
    if telescope_type_id is not None:
        filters.append(Telescope.telescope_type_id == telescope_type_id)
    if organisation_id is not None:
        filters.append(Telescope.organisation_id == organisation_id)
    if len(filters):
        q = q.filter(and_(*filters))

    # Return items that match with a limit
    return q.limit(limit).all()


def get_organisation(id: int) -> Response:
    """Get an Organisation.

    :param id: the organisation id.
    :return: a Response object.
    """

    return get_item(Organisation, id)


def post_organisation(body: Dict) -> Response:
    """Create an Organisation.

    :param body:
    :return: a Response object.
    """

    return post_item(Organisation, body)


def put_organisation(body: Dict) -> Response:
    """Create or update an Organisation.

    :param body: the Organisation in the form of a dictionary.
    :return: a Response object.
    """

    return put_item(Organisation, body)


def delete_organisation(id: int) -> Response:
    """Delete an Organisation.

    :param id: the Organisation id.
    :return: a Response object.
    """

    return delete_item(Organisation, id)


def get_organisations(limit: int) -> Response:
    """Get a list of organisations.

    :param limit: the maximum number of items to return.
    :return: a Response object.
    """

    return get_items(Organisation, limit)


def get_dataset_storage(id: int) -> Response:
    """Get a DatasetStorage.

    :param id: the DatasetStorage id.
    :return: a Response object.
    """

    return get_item(DatasetStorage, id)


def post_dataset_storage(body: Dict) -> Response:
    """Create a DatasetStorage.

    :param body: the DatasetStorage in the form of a dictionary.
    :return: a Response object.
    """

    return post_item(DatasetStorage, body)


def put_dataset_storage(body: Dict) -> Response:
    """Create or update a DatasetStorage.

    :param body: the DatasetStorage in the form of a dictionary.
    :return: a Response object.
    """

    return put_item(DatasetStorage, body)


def delete_dataset_storage(id: int) -> Response:
    """Delete a DatasetStorage.

    :param id: the DatasetStorage id.
    :return: a Response object.
    """

    return delete_item(DatasetStorage, id)


def get_dataset_storages(limit: int, dataset_id: int = None) -> Response:
    """Get a list of DatasetStorage objects.

    :param limit: the maximum number of DatasetStorage objects to return.
    :param dataset_id: the dataset id to filter for.
    :return: a Response object.
    """

    q = session_.query(DatasetStorage)

    # Create filters based on parameters
    filters = []
    if dataset_id is not None:
        filters.append(DatasetStorage.dataset_id == dataset_id)
    if len(filters):
        q = q.filter(and_(*filters))

    # Return items that match with a limit
    return q.limit(limit).all()


def get_dataset_release(id: int) -> Response:
    """Get a DatasetRelease.

    :param id: the DatasetRelease id.
    :return: a Response object.
    """

    return get_item(DatasetRelease, id)


def post_dataset_release(body: Dict) -> Response:
    """Create a DatasetRelease.

    :param body: the DatasetRelease in the form of a dictionary.
    :return: a Response object.
    """

    return post_item(DatasetRelease, body)


def put_dataset_release(body: Dict) -> Response:
    """Create or update a DatasetRelease.

    :param body: the DatasetRelease in the form of a dictionary.
    :return: a Response object.
    """

    return put_item(DatasetRelease, body)


def delete_dataset_release(id: int) -> Response:
    """Delete a DatasetRelease.

    :param id: the DatasetRelease id.
    :return: a Response object.
    """

    return delete_item(DatasetRelease, id)


def get_dataset_releases(limit: int, dataset_id: int = None) -> Response:
    """Get a list of DatasetRelease objects.

    :param limit: the maximum number of DatasetRelease objects to return.
    :param dataset_id: the dataset_id to query
    :return: a Response object.
    """

    q = session_.query(DatasetRelease)

    # Create filters based on parameters
    filters = []
    if dataset_id is not None:
        filters.append(DatasetRelease.dataset_id == dataset_id)
    if len(filters):
        q = q.filter(and_(*filters))

    # Return items that match with a limit
    return q.limit(limit).all()


def get_dataset(id: int) -> Response:
    """Get a Dataset.

    :param id: the Dataset id.
    :return: a Response object.
    """

    return get_item(Dataset, id)


def post_dataset(body: Dict) -> Response:
    """Create a Dataset.

    :param body: the Dataset in the form of a dictionary.
    :return: a Response object.
    """

    return post_item(Dataset, body)


def put_dataset(body: Dict) -> Response:
    """Create or update a Dataset.

    :param body: the Dataset in the form of a dictionary.
    :return: a Response object.
    """

    return put_item(Dataset, body)


def delete_dataset(id: int) -> Response:
    """Delete a Dataset.

    :param id: the Dataset id.
    :return: a Response object.
    """

    return delete_item(Dataset, id)


def get_datasets(limit: int, telescope_id: int = None) -> Response:
    """Get a list of Dataset objects.

    :param limit: the maximum number of Dataset objects to return.
    :param telescope_id: Telescope id to filter by.
    :return: a Response object.
    """

    q = session_.query(Dataset)
    filters = []

    if telescope_id is not None:
        filters.append(Dataset.connection_id == telescope_id)
    if len(filters):
        q = q.filter(and_(*filters))

    # Return items that match with a limit
    return q.limit(limit).all()


# def searchv2():
#     """ Example of having a different function for an upgraded version of the API.
#
#     :return:
#     """
#
#     return "Hello World"


def create_app() -> connexion.App:
    """Create a Connexion App.

    :return: the Connexion App.
    """

    logging.info("Creating app")

    # Create the application instance and don't sort JSON output alphabetically
    conn_app = connexion.App(__name__)
    conn_app.app.config["JSON_SORT_KEYS"] = False

    # Add the OpenAPI specification
    specification_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "openapi.yaml.jinja2")
    builder = OpenApiRenderer(specification_path, usage_type="backend")
    specification = builder.to_dict()
    conn_app.add_api(specification)

    return conn_app
