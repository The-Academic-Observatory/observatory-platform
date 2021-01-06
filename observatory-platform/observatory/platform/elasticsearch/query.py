"""
example queries:
http://0.0.0.0:5000/query?agg=institution&subset=journals&from=2018-01-15&to=2019-01-01&journal=Molecular
%20Pharmaceutics 447 records, ~2 seconds
http://0.0.0.0:5000/query?agg=country&subset=oa-metrics 11435 records, ~6 sec
http://0.0.0.0:5000/query?agg=institution&subset=journals&id=grid.4691.a,grid.469280.1 28479 records, ~9 sec
http://0.0.0.0:5000/query?agg=country&subset=collaborations 268162 records, ~80 sec

possible filtering fields:
- id
- country
- country_code
- journal
- name
- region
- subregion
"""
from flask import make_response, abort, request
from datetime import datetime
from elasticsearch import Elasticsearch
import os
import pprint
import pandas as pd
from pandas import DataFrame
from typing import Tuple
import time


def create_es_connection():
    script_directory = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(script_directory, 'elasticsearch_auth.txt')) as auth_file:
        lines = auth_file.read().splitlines()
        address = lines[0]
        username = lines[1]
        password = lines[2]

    es = Elasticsearch(address, http_auth=(username, password))
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
        range_dict = {"range": {"published_year": {"format": "yyyy-MM-dd"}}}
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
    # agg_body = {}
    # for agg_type in aggregations.keys():
    #     fields = aggregations[agg_type]
    #     for field in fields:
    #         agg_body[agg_type + "_" + field] = {
    #             agg_type: {
    #                 "field": field
    #             }
    #         }
    search_body = {
        "size": 10000,
        "query": query_body,
        # "aggs": agg_body
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


def search():
    start = time.time()
    results = []

    agg = request.args.get('agg')
    subset = request.args.get('subset')
    from_date = request.args.get('from')
    to_date = request.args.get('to')

    filter_field_ids = ['id', 'country', 'country_code', 'journal', 'name', 'region', 'subregion']
    filter_fields = {}
    for field in filter_field_ids:
        value = request.args.get(field)
        if value:
            value = value.split(',')
        filter_fields[field] = value

    for date_text in [from_date, to_date]:
        if date_text:
            validate_date(date_text)

    # TODO determine which combinations/indices we can use
    index = f"{subset}-{agg}-20201205"

    # aggregations = {"sum": ["num_not_oa_outputs", "num_oa_outputs", "total_outputs"]}
    search_body = create_search_body(from_date, to_date, filter_fields)

    es = create_es_connection()
    res = es.search(index=index, body=search_body, scroll='3m')
    scroll_id, hits = process_response(res)
    print(res['hits']['total'])
    results += hits

    while hits:
        res = es.scroll(scroll_id=scroll_id, scroll='1s')
        scroll_id, hits = process_response(res)
        results += hits

    # Turn into pandas dataframe like this
    # df = pd.DataFrame(results)
    print('done')
    end = time.time()
    print(end-start)
    return results

