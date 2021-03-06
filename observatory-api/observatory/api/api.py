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

import datetime
import logging
import os
import time
from datetime import datetime
from typing import ClassVar, Dict, Tuple, Union

import connexion
from flask import jsonify
from sqlalchemy import and_

from observatory.api.elastic import (create_schema, process_response, list_available_index_dates, create_search_body,
                                     create_es_connection, parse_args)
from observatory.api.openapi_renderer import OpenApiRenderer
from observatory.api.orm import Telescope, TelescopeType, Organisation

Response = Tuple[Union[Dict, str], int]
session_ = None  # Global session


def make_response(status_code: int, description: str, data: Dict = None, json: bool = True) -> Response:
    """ Make an API response.

    :param status_code: the status code.
    :param description: the description for the status code.
    :param data: the data.
    :param json: whether to jsonify the data.
    :return: a Response.
    """

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


def get_telescope_type(id: int) -> Response:
    """ Get a TelescopeType.

    :param id: the TelescopeType id.
    :return: a Response object.
    """

    return get_item(TelescopeType, id)


def post_telescope_type(body: Dict) -> Response:
    """ Create a TelescopeType.

    :param body: the TelescopeType in the form of a dictionary.
    :return: a Response object.
    """

    return post_item(TelescopeType, body)


def put_telescope_type(body: Dict) -> Response:
    """ Create or update a TelescopeType.

    :param body: the TelescopeType in the form of a dictionary.
    :return: a Response object.
    """

    return put_item(TelescopeType, body)


def delete_telescope_type(id: int) -> Response:
    """ Delete a TelescopeType.

    :param id: the TelescopeType id.
    :return: a Response object.
    """

    return delete_item(TelescopeType, id)


def get_telescope_types(limit: int) -> Response:
    """ Get a list of TelescopeType objects.

    :param limit: the maximum number of TelescopeType objects to return.
    :return: a Response object.
    """

    return get_items(TelescopeType, limit)


def get_telescope(id: int) -> Response:
    """ Get a Telescope.

    :param id: the Telescope id.
    :return: a Response object.
    """

    return get_item(Telescope, id)


def post_telescope(body: Dict) -> Response:
    """ Create a Telescope.

    :param body: the Connection in the form of a dictionary.
    :return: a Response object.
    """

    return post_item(Telescope, body)


def put_telescope(body: Dict) -> Response:
    """ Create or update a Telescope.

    :param body: the Telescope in the form of a dictionary.
    :return: a Response object.
    """

    return put_item(Telescope, body)


def delete_telescope(id: int) -> Response:
    """ Delete a Telescope.

    :param id: the Telescope id.
    :return: a Response object.
    """

    return delete_item(Telescope, id)


def get_telescopes(limit: int, telescope_type_id=None, organisation_id: int = None) -> Response:
    """ Get a list of Telescope objects.

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
    items = q.limit(limit).all()
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
    """ Create an Organisation.

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


def queryv1() -> Union[Tuple[str, int], dict]:
    """ Search the Observatory Platform.

    :return: results dictionary or error response
    """

    start = time.time()

    alias, index_date, from_date, to_date, filter_fields, size, scroll_id = parse_args()

    es_api_key = os.environ.get('ES_API_KEY')
    es_address = os.environ.get('ES_HOST')
    es = create_es_connection(es_address, es_api_key)
    if es is None:
        return "Elasticsearch environment variable for host or api key is empty", 400

    # use scroll id
    if scroll_id:
        res = es.scroll(scroll_id=scroll_id, scroll='1m')
        index = 'N/A'
    # use search body
    else:
        if alias == '':
            return "Invalid combination of aggregation (publisher) and subset (collaborations)", 400

        search_body = create_search_body(from_date, to_date, filter_fields, size)

        # use specific index if date is given, otherwise use alias which points to latest date
        if index_date:
            index = alias + f"-{index_date}"
            index_exists = es.indices.exists(index)
            if not index_exists:
                available_dates = list_available_index_dates(es, alias)
                return f"Index does not exist: {index}\n Available dates for this agg & subset:\n" \
                       f"{chr(10).join(available_dates)}", 400
        else:
            index = es.cat.aliases(alias, format='json')[0]['index']

        res = es.search(index=index, body=search_body, scroll='1m')
    scroll_id, results_data = process_response(res)

    number_total_results = res['hits']['total']['value']

    end = time.time()
    print(end - start)
    results = {
        'version': 'v1',
        'index': index,
        'scroll_id': scroll_id,
        'returned_hits': len(results_data),
        'total_hits': number_total_results,
        'schema': create_schema(),
        'results': results_data
    }
    return results


# def searchv2():
#     """ Example of having a different function for an upgraded version of the API.
#
#     :return:
#     """
#
#     return "Hello World"


def create_app() -> connexion.App:
    """ Create a Connexion App.

    :return: the Connexion App.
    """

    logging.info('Creating app')

    # Create the application instance and don't sort JSON output alphabetically
    conn_app = connexion.App(__name__)
    conn_app.app.config['JSON_SORT_KEYS'] = False

    # Add the OpenAPI specification
    specification_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'openapi.yaml.jinja2')
    builder = OpenApiRenderer(specification_path, cloud_endpoints=False)
    specification = builder.to_dict()
    conn_app.add_api(specification)

    return conn_app
