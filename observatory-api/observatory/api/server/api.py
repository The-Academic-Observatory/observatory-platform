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

import connexion
import logging
import os
import pendulum
import time
from connexion import NoContent
from flask import jsonify
from observatory.api.server.elastic import (
    create_es_connection,
    create_schema,
    create_search_body,
    list_available_index_dates,
    parse_args,
    process_response,
)
from observatory.api.server.openapi_renderer import OpenApiRenderer
from observatory.api.server.orm import (
    BigQueryBytesProcessed,
    Dataset,
    DatasetRelease,
    Organisation,
    Workflow,
    WorkflowType,
    TableType,
    DatasetType,
)
from sqlalchemy import and_
from sqlalchemy import func
from typing import Any, ClassVar, Dict, Tuple, Union

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


def get_workflow_type(id: int = None, type_id: str = None) -> Response:
    """Get a WorkflowType.

    :param id: the WorkflowType id.
    :param type_id: the WorkflowType type_id.
    :return: a Response object.
    """

    if (id is not None and type_id is not None) or (id is None and type_id is None):
        body = "At least one and only one of id or type_id must be specified"
        logging.error(body)
        return body, 400
    elif id is not None:
        return get_item(WorkflowType, id)
    else:
        item = session_.query(WorkflowType).filter(WorkflowType.type_id == type_id).one_or_none()
        if item is not None:
            logging.info(f"Found: WorkflowType with type_id {type_id}")
            return jsonify(item)

        body = f"Not found: WorkflowType with type_id {type_id}"
        logging.info(body)
        return body, 404


def post_workflow_type(body: Dict) -> Response:
    """Create a WorkflowType.

    :param body: the WorkflowType in the form of a dictionary.
    :return: a Response object.
    """

    return post_item(WorkflowType, body)


def put_workflow_type(body: Dict) -> Response:
    """Create or update a WorkflowType.

    :param body: the WorkflowType in the form of a dictionary.
    :return: a Response object.
    """

    return put_item(WorkflowType, body)


def delete_workflow_type(id: int) -> Response:
    """Delete a WorkflowType.

    :param id: the WorkflowType id.
    :return: a Response object.
    """

    return delete_item(WorkflowType, id)


def get_workflow_types(limit: int) -> Response:
    """Get a list of WorkflowType objects.

    :param limit: the maximum number of WorkflowType objects to return.
    :return: a Response object.
    """

    return get_items(WorkflowType, limit)


def get_workflow(id: int) -> Response:
    """Get a Workflow.

    :param id: the Workflow id.
    :return: a Response object.
    """

    return get_item(Workflow, id)


def post_workflow(body: Dict) -> Response:
    """Create a Workflow.

    :param body: the Connection in the form of a dictionary.
    :return: a Response object.
    """

    return post_item(Workflow, body)


def put_workflow(body: Dict) -> Response:
    """Create or update a Workflow.

    :param body: the Workflow in the form of a dictionary.
    :return: a Response object.
    """

    return put_item(Workflow, body)


def delete_workflow(id: int) -> Response:
    """Delete a Workflow.

    :param id: the Workflow id.
    :return: a Response object.
    """

    return delete_item(Workflow, id)


def get_workflows(limit: int, workflow_type_id=None, organisation_id: int = None) -> Response:
    """Get a list of Workflow objects.

    :param organisation_id: the Organisation id to filter by.
    :param workflow_type_id: the WorkflowType id to filter by.
    :param limit: the maximum number of Workflow objects to return.
    :return: a Response object.
    """

    q = session_.query(Workflow)

    # Create filters based on parameters
    filters = []
    if workflow_type_id is not None:
        filters.append(Workflow.workflow_type_id == workflow_type_id)
    if organisation_id is not None:
        filters.append(Workflow.organisation_id == organisation_id)
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


def get_table_type(id: int = None, type_id: str = None) -> Response:
    """Get a TableType.

    :param id: the TableType id.
    :param type_id: the TableType type_id.
    :return: a Response object.
    """

    if (id is not None and type_id is not None) or (id is None and type_id is None):
        body = "At least one and only one of id or type_id must be specified"
        logging.error(body)
        return body, 400
    elif id is not None:
        return get_item(TableType, id)
    else:
        item = session_.query(TableType).filter(TableType.type_id == type_id).one_or_none()
        if item is not None:
            logging.info(f"Found: TableType with type_id {type_id}")
            return jsonify(item)

        body = f"Not found: TableType with type_id {type_id}"
        logging.info(body)
        return body, 404


def post_table_type(body: Dict) -> Response:
    """Create a TableType.

    :param body: the TableType in the form of a dictionary.
    :return: a Response object.
    """

    return post_item(TableType, body)


def put_table_type(body: Dict) -> Response:
    """Create or update a TableType.

    :param body: the TableType in the form of a dictionary.
    :return: a Response object.
    """

    return put_item(TableType, body)


def delete_table_type(id: int) -> Response:
    """Delete a TableType.

    :param id: the TableType id.
    :return: a Response object.
    """

    return delete_item(TableType, id)


def get_table_types(limit: int) -> Response:
    """Get a list of TableType objects.

    :param limit: the maximum number of TableType objects to return.
    :return: a Response object.
    """

    return get_items(TableType, limit)


def get_dataset_type(id: int = None, type_id: str = None) -> Response:
    """Get a DatasetType.

    :param id: the DatasetType id.
    :param type_id: the DatasetType type_id.
    :return: a Response object.
    """

    if (id is not None and type_id is not None) or (id is None and type_id is None):
        body = "At least one and only one of id or type_id must be specified"
        logging.error(body)
        return body, 400
    elif id is not None:
        return get_item(DatasetType, id)
    else:
        item = session_.query(DatasetType).filter(DatasetType.type_id == type_id).one_or_none()
        if item is not None:
            logging.info(f"Found: DatasetType with type_id {type_id}")
            return jsonify(item)

        body = f"Not found: DatasetType with type_id {type_id}"
        logging.info(body)
        return body, 404


def post_dataset_type(body: Dict) -> Response:
    """Create a DatasetType.

    :param body: the DatasetType in the form of a dictionary.
    :return: a Response object.
    """

    return post_item(DatasetType, body)


def put_dataset_type(body: Dict) -> Response:
    """Create or update a DatasetType.

    :param body: the DatasetType in the form of a dictionary.
    :return: a Response object.
    """

    return put_item(DatasetType, body)


def delete_dataset_type(id: int) -> Response:
    """Delete a DatasetType.

    :param id: the DatasetType id.
    :return: a Response object.
    """

    return delete_item(DatasetType, id)


def get_dataset_types(limit: int) -> Response:
    """Get a list of DatasetType objects.

    :param limit: the maximum number of DatasetType objects to return.
    :return: a Response object.
    """

    return get_items(DatasetType, limit)


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


def get_datasets(limit: int, workflow_id: int = None) -> Response:
    """Get a list of Dataset objects.

    :param limit: the maximum number of Dataset objects to return.
    :param workflow_id: Workflow id to filter by.
    :return: a Response object.
    """

    q = session_.query(Dataset)
    filters = []

    if workflow_id is not None:
        filters.append(Dataset.workflow_id == workflow_id)
    if len(filters):
        q = q.filter(and_(*filters))

    # Return items that match with a limit
    return q.limit(limit).all()


def get_bigquery_bytes_processed(project: str) -> Response:
    """Get the bytes processed by BigQuery project for the past 24 hours

    :param project: GCP project ID.
    """

    date_24_hrs = pendulum.now("UTC").subtract(hours=24)

    result = (
        session_.query(func.sum(BigQueryBytesProcessed.total).label("total"))
        .filter(
            and_(
                BigQueryBytesProcessed.project == project,
                BigQueryBytesProcessed.created >= date_24_hrs,
            )
        )
        .first()
    )

    # Get total
    total = result.total

    # Set total to zero when no results returned
    if total is None:
        total = 0

    return int(total), 200


def post_bigquery_bytes_processed(body: Dict) -> Response:
    """Create a BigQueryBytesProcessed.

    :param body: the BigQueryBytesProcessed in the form of a dictionary.
    :return: a Response object.
    """

    return post_item(BigQueryBytesProcessed, body)


def queryv1() -> Union[Tuple[str, int], dict]:
    """Search the Observatory Platform.

    :return: results dictionary or error response
    """

    start = time.time()

    alias, index_date, from_date, to_date, filter_fields, size, scroll_id = parse_args()

    es_api_key = os.environ.get("ES_API_KEY")
    es_address = os.environ.get("ES_HOST")
    es = create_es_connection(es_address, es_api_key)
    if es is None:
        return "Elasticsearch environment variable for host or api key is empty", 400

    # use scroll id
    if scroll_id:
        res = es.scroll(scroll_id=scroll_id, scroll="1m")
        index = "N/A"
    # use search body
    else:
        search_body = create_search_body(from_date, to_date, filter_fields, size)

        # use specific index if date is given, otherwise use alias which points to latest date
        if index_date:
            index = alias + f"-{index_date}"
            index_exists = es.indices.exists(index)
            if not index_exists:
                available_dates = list_available_index_dates(es, alias)
                return (
                    f"Index does not exist: {index}\n Available dates for this agg & subset:\n"
                    f"{chr(10).join(available_dates)}",
                    400,
                )
        else:
            index = es.cat.aliases(alias, format="json")[0]["index"]

        res = es.search(index=index, body=search_body, scroll="1m")
    scroll_id, results_data = process_response(res)

    number_total_results = res["hits"]["total"]["value"]

    end = time.time()
    print(end - start)
    results = {
        "version": "v1",
        "index": index,
        "scroll_id": scroll_id,
        "returned_hits": len(results_data),
        "total_hits": number_total_results,
        "schema": create_schema(),
        "results": results_data,
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
    """Create a Connexion App.

    :return: the Connexion App.
    """

    logging.info("Creating app")

    # Create the application instance and don't sort JSON output alphabetically
    conn_app = connexion.App(__name__)
    conn_app.app.config["JSON_SORT_KEYS"] = False

    # Add the OpenAPI specification
    specification_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "openapi.yaml.jinja2")
    builder = OpenApiRenderer(specification_path, cloud_endpoints=False)
    specification = builder.to_dict()
    conn_app.add_api(specification)

    return conn_app
