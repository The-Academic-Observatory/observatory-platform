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
from sqlalchemy import and_

from observatory.api.server.openapi_renderer import OpenApiRenderer
from observatory.api.server.orm import (
    DatasetRelease,
)

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


def get_dataset_releases(dag_id: str = None, dataset_id: str = None) -> Response:
    """Get a list of DatasetRelease objects.

    :param dag_id: the dag_id to query
    :param dataset_id: the dataset_id to query
    :return: a Response object.
    """

    q = session_.query(DatasetRelease)

    # Create filters based on parameters
    filters = []
    if dag_id is not None:
        filters.append(DatasetRelease.dag_id == dag_id)
    if dataset_id is not None:
        filters.append(DatasetRelease.dataset_id == dataset_id)
    if len(filters):
        q = q.filter(and_(*filters))

    # Return items that match
    return q.all()


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
    builder = OpenApiRenderer(specification_path)
    specification = builder.to_dict()
    conn_app.add_api(specification)

    return conn_app
