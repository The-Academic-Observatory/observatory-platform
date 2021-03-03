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

from __future__ import annotations

import datetime
import logging
from datetime import datetime
from typing import ClassVar
from typing import Dict, Tuple, Union

from flask import jsonify

from observatory.platform.api.orm import (Connection, ConnectionType, Organisation)

Response = Tuple[Union[Dict, str], int]
session_ = None  # Global session


def make_response(status_code: int, description: str, data: Dict = None, json: bool = True) -> Response:
    data_response = {
        'data': data,
        'response': {'status_code': status_code, 'description': description}
    }

    if json:
        data_response = jsonify(data_response)

    return data_response, status_code


def get_item(cls: ClassVar, item_id: int) -> Response:
    """ Get an item.

    :param cls: the SQLAlchemy Table metadata class.
    :param item_id: the id of the item.
    :return: a Response object.
    """

    item = session_.query(cls).filter(cls.id == item_id).one_or_none()
    if item is not None:
        status_code = 200
        description = f'Found: {cls.__name__} with id {item_id}'
        logging.info(description)
        return make_response(status_code, description, data=item)

    status_code = 404
    description = f'Not found: {cls.__name__} with id {item_id}'
    logging.info(description)
    return make_response(status_code, description)


def post_item(cls: ClassVar, body: Dict) -> Response:
    """ Create an item.

    :param cls: the SQLAlchemy Table metadata class.
    :param body: the item in the form of a dictionary.
    :return: a Response object.
    """

    logging.info(f'Creating item: {cls.__name__}')

    # Automatically set created and modified datetime
    now = datetime.utcnow()
    body['created'] = now
    body['modified'] = now

    create_item = cls(**body)
    session_.add(create_item)
    session_.flush()
    session_.commit()

    item_id = create_item.id
    status_code = 201
    description = f'Created: {cls.__name__} with id {item_id}'
    data = {'id': create_item.id}
    logging.info(description)

    return make_response(status_code, description, data=data)


def put_item(cls: ClassVar, body: Dict) -> Response:
    """ Create or update an item. If the item has an id it will be updated, else it will be created.

    :param cls: the SQLAlchemy Table metadata class.
    :param body: the item in the form of a dictionary.
    :return: a Response object.
    """

    item_id = body.get('id')
    if item_id is not None:
        item = session_.query(cls).filter(cls.id == item_id).one_or_none()

        if item is not None:
            logging.info(f'Updating {cls.__name__} {item_id}')
            # Remove id and automatically set modified time
            body.pop('id')
            body['modified'] = datetime.utcnow()
            item.update(**body)
            session_.commit()

            status_code = 200
            description = f'Updated: {cls.__name__} with id {item_id}'
            logging.info(description)
            return make_response(status_code, description)
        else:
            status_code = 404
            description = f'Not found: {cls.__name__} with id {item_id}'
            logging.info(description)
            return make_response(status_code, description)
    else:
        return post_item(cls, body)


def delete_item(cls: ClassVar, item_id: int) -> Response:
    """ Delete an item.

    :param cls: the SQLAlchemy Table metadata class.
    :param id: the id of the item.
    :return: a Response object.
    """

    org = session_.query(cls).filter(cls.id == item_id).one_or_none()
    if org is not None:
        logging.info(f'Deleting {cls.__name__} {item_id}')
        session_.query(cls).filter(cls.id == item_id).delete()
        session_.commit()

        status_code = 200
        description = f'Deleted: {cls.__name__} with id {item_id}'
        logging.info(description)
        return make_response(status_code, description)
    else:
        status_code = 404
        description = f'Not found: {cls.__name__} with id {item_id}'
        logging.info(description)
        return make_response(status_code, description)


def get_items(cls: ClassVar, limit: int) -> Response:
    """ Get a list of items.

    :param cls: the SQLAlchemy Table metadata class.
    :param limit: the maximum number of items to return.
    :return: a Response object.
    """

    items = session_.query(cls).limit(limit).all()

    status_code = 200
    description = f'Found items: {cls.__name__}'
    logging.info(description)
    return make_response(status_code, description, data=items)


def get_connection_type(id: int) -> Response:
    """ Get a ConnectionType.

    :param id: the ConnectionType id.
    :return: a Response object.
    """

    return get_item(ConnectionType, id)


def post_connection_type(body: Dict) -> Response:
    """ Create a ConnectionType.

    :param body: the ConnectionType in the form of a dictionary.
    :return: a Response object.
    """

    return post_item(ConnectionType, body)


def put_connection_type(body: Dict) -> Response:
    """ Create or update a ConnectionType.

    :param body: the ConnectionType in the form of a dictionary.
    :return: a Response object.
    """

    return put_item(ConnectionType, body)


def delete_connection_type(id: int) -> Response:
    """ Delete a ConnectionType.

    :param id: the ConnectionType id.
    :return: a Response object.
    """

    return delete_item(ConnectionType, id)


def get_connection_types(limit: int) -> Response:
    """ Get a list of ConnectionType objects.

    :param limit: the maximum number of ConnectionType objects to return.
    :return: a Response object.
    """

    return get_items(ConnectionType, limit)


def get_connection(id: int) -> Response:
    """ Get a Connection.

    :param id: the Connection id.
    :return: a Response object.
    """

    return get_item(Connection, id)


def post_connection(body: Dict) -> Response:
    """ Create a Connection.

    :param body: the Connection in the form of a dictionary.
    :return: a Response object.
    """

    return post_item(Connection, body)


def put_connection(body: Dict) -> Response:
    """ Create or update a Connection.

    :param body: the Connection in the form of a dictionary.
    :return: a Response object.
    """

    return put_item(Connection, body)


def delete_connection(id: int) -> Response:
    """ Delete a Connection.

    :param id: the Connection id.
    :return: a Response object.
    """

    return delete_item(Connection, id)


def get_connections(organisation_id: int, limit: int) -> Response:
    """ Get a list of Connection objects.

    :param organisation_id: the Organisation id to filter by.
    :param limit: the maximum number of Connection objects to return.
    :return: a Response object.
    """

    items = session_.query(Connection).filter(Connection.organisation_id == organisation_id).limit(limit).all()
    status_code = 200
    description = f'Found items: {Organisation.__class__}'
    return make_response(status_code, description, data=items)


def get_organisation(id: int) -> Response:
    """ Get an Organisation.

    :param id: the organisation id.
    :return: a Response object.
    """

    return get_item(Organisation, id)


def post_organisation(body: Dict) -> Response:
    """ Create a ConnectionType.

    :param body:
    :return: a Response object.
    """

    return post_item(Organisation, body)


def put_organisation(body: Dict) -> Response:
    """ Create or update an Organisation.

    :param body: the Organisation in the form of a dictionary.
    :return: a Response object.
    """

    return put_item(Organisation, body)


def delete_organisation(id: int) -> Response:
    """ Delete an Organisation.

    :param id: the Organisation id.
    :return: a Response object.
    """

    return delete_item(Organisation, id)


def get_organisations(limit: int) -> Response:
    """ Get a list of organisations.

    :param limit: the maximum number of items to return.
    :return: a Response object.
    """

    return get_items(Organisation, limit)
