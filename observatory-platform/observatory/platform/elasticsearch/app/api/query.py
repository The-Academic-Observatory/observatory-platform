"""
example queries:
http://0.0.0.0:5000/query?agg=institution&subset=journals&from=2018-01-15&to=2019-01-01&journal=Molecular
%20Pharmaceutics&api_key=158f9f48b674b0fdd18e5b465d7ae37e 447 records, ~2 seconds
http://0.0.0.0:5000/query?agg=country&subset=oa-metrics&api_key=158f9f48b674b0fdd18e5b465d7ae37e 11435 records, ~6 sec
http://0.0.0.0:5000/query?agg=institution&subset=journals&id=grid.4691.a,
grid.469280.1&api_key=158f9f48b674b0fdd18e5b465d7ae37e 28479 records, ~9 sec
http://0.0.0.0:5000/query?agg=country&subset=collaborations&api_key=158f9f48b674b0fdd18e5b465d7ae37e 268162 records,
~80 sec
"""
import os
import time
from datetime import datetime
from typing import Tuple

from elasticsearch import Elasticsearch
from flask import current_app
from flask import request


def create_es_connection():
    es_username = os.environ.get('ES_USERNAME')
    es_password = os.environ.get('ES_PASSWORD')
    es_address = os.environ.get('ES_ADDRESS')

    es = Elasticsearch(es_address, http_auth=(es_username, es_password))
    if not es.ping():
        raise ConnectionError("Could not connect to elasticsearch server")
    return es


def create_search_body(from_year: str, to_year: str, filter_fields: dict) -> dict:
    filter_list = []
    for field in filter_fields:
        if filter_fields[field]:
            filter_list.append({
                "terms": {
                    f"{field}.keyword": filter_fields[field]
                }
            })
    if from_year or to_year:
        range_dict = {
            "range": {
                "published_year": {
                    "format": "yyyy-MM-dd"
                }
            }
        }
        if from_year:
            range_dict["range"]["published_year"]["gte"] = from_year
        if to_year:
            range_dict["range"]["published_year"]["lte"] = to_year
        filter_list.append(range_dict)
    query_body = {
        "bool": {
            "filter": filter_list
        }
    }

    search_body = {
        "size": 10000,
        "query": query_body,
    }
    return search_body


def validate_date(date_text: str):
    try:
        datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")


def process_response(res: dict) -> Tuple[str, list]:
    scroll_id = res['_scroll_id']
    # flatten nested dictionary '_source'
    for hit in res['hits']['hits']:
        source = hit.pop('_source')
        for k, v in source.items():
            hit[k] = v
    hits = res['hits']['hits']
    return scroll_id, hits


def create_schema():
    return {
        'schema': 'to_be_created'
    }


def search():
    start = time.time()
    all_results = []

    agg = request.args.get('agg')
    subset = request.args.get('subset')
    from_date = request.args.get('from')
    to_date = request.args.get('to')
    limit = request.args.get('limit')

    filter_fields = {}
    with current_app.app_context():
        query_filter_parameters = current_app.query_filter_parameters
    for field in query_filter_parameters:
        value = request.args.get(field)
        if value:
            value = value.split(',')
        filter_fields[field] = value

    for date_text in [from_date, to_date]:
        if date_text:
            validate_date(date_text)

    # TODO determine which combinations/indices we can use
    if agg == 'author' or agg == 'funder':
        agg += '_test'
    index = f"{subset}-{agg}-20201205"

    limit = int(limit) if limit else None

    search_body = create_search_body(from_date, to_date, filter_fields)

    es = create_es_connection()
    res = es.search(index=index, body=search_body, scroll='3m')
    scroll_id, hits = process_response(res)

    number_total_results = res['hits']['total']['value']
    all_results += hits

    while hits:
        if limit:
            if len(all_results) > limit:
                break
        res = es.scroll(scroll_id=scroll_id, scroll='1s')
        scroll_id, hits = process_response(res)
        all_results += hits

    end = time.time()
    print(end - start)
    all_results = all_results[:limit] if limit else all_results
    schema = create_schema()
    results = {
        'returned_results': len(all_results),
        'total_results': number_total_results,
        'schema': schema,
        'results': all_results
    }
    # results = OrderedDict(
    #     [('returned_hits', len(all_hits)), ('total_hits', total_hits), ('schema', schema), ('hits', all_hits)])
    return results
