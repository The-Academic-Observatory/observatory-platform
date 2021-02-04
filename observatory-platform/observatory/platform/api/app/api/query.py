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
from typing import Tuple, Union

from elasticsearch import Elasticsearch
from flask import current_app
from flask import request


def create_es_connection(address: str, username: str, password: str) -> Union[Elasticsearch, None]:
    for value in [address, username, password]:
        if value is None or value == '':
            return None
    es = Elasticsearch(address, http_auth=(username, password))
    if not es.ping():
        raise ConnectionError("Could not connect to elasticsearch server. Url, username or password might be invalid.")
    return es


def create_search_body_search_after(from_year: str, to_year: str, filter_fields: dict, size: int, pit_id: str,
                                    search_after: list) -> dict:
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
        "size": size,
        "query": query_body,
        "sort": [
            {"published_year": "asc"},
            {"_id": "asc"}
        ]
    }
    if pit_id:
        search_body["pit"] = {
            "id": pit_id,
            "keep_alive": "1m"
        }
    if search_after:
        search_body['search_after'] = search_after
    return search_body


def process_response_after_search(res: dict) -> Tuple[str, int, str, list]:
    pit_id = res['pit_id']
    # flatten nested dictionary '_source'
    for hit in res['hits']['hits']:
        source = hit.pop('_source')
        for k, v in source.items():
            hit[k] = v
    hits = res['hits']['hits']

    if hits:
        search_after = hits[-1]['sort']
        search_after_no = search_after[0]
        search_after_text = search_after[1]
    else:
        search_after_no = None
        search_after_text = None

    return pit_id, search_after_no, search_after_text, hits


def create_search_body(from_year: Union[str, None], to_year: Union[str, None], filter_fields: dict, size: int) -> dict:
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
            range_dict["range"]["published_year"]["lt"] = to_year
        filter_list.append(range_dict)
    query_body = {
        "bool": {
            "filter": filter_list
        }
    }

    search_body = {
        "size": size,
        "query": query_body,
        "sort": ["_doc"]
    }
    return search_body


def validate_dates(from_date: str, to_date: str) -> Tuple[str, str]:
    dates = []
    for date_text in [from_date, to_date]:
        if date_text:
            try:
                datetime.strptime(date_text, '%Y')
            except ValueError:
                raise ValueError("Incorrect data format, should be YYYY")
            dates.append(f"{date_text}-12-31")
        else:
            dates.append(None)
    return dates[0], dates[1]


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


# def search():
#     start = time.time()
#     max_size = 10000
#     # all_results = []
#
#     agg = request.args.get('agg')
#     subset = request.args.get('subset')
#     from_date = request.args.get('from')
#     to_date = request.args.get('to')
#     limit = request.args.get('limit')
#     pit_id = request.args.get('pit_id')
#     search_after_no = request.args.get('search_after_no')
#     search_after_text = request.args.get('search_after_text')
#
#     filter_fields = {}
#     with current_app.app_context():
#         query_filter_parameters = current_app.query_filter_parameters
#     for field in query_filter_parameters:
#         value = request.args.get(field)
#         if value:
#             value = value.split(',')
#         filter_fields[field] = value
#
#     from_date, to_date = validate_dates(from_date, to_date)
#
#     # TODO determine which combinations/indices we can use
#     if agg == 'author' or agg == 'funder':
#         agg += '_test'
#     if agg == 'publisher' and subset == 'collaborations':
#         return "Invalid combination of aggregation (publisher) and subset (collaborations)", 400
#     index = f"{subset}-{agg}-20201205"
#
#     if search_after_no and search_after_text:
#         search_after = [search_after_no, search_after_text]
#     else:
#         search_after = None
#
#     es_username = os.environ.get('ES_USERNAME')
#     es_password = os.environ.get('ES_PASSWORD')
#     es_address = os.environ.get('ES_ADDRESS')
#
#     if not pit_id:
#         res = requests.post(f"{es_address}/{index}/_pit", auth=(es_username, es_password),
#                             params=(('keep_alive', '1m'),))
#         pit_id = json.loads(res.text)['id']
#
#     if limit:
#         limit = int(limit)
#         size = min(max_size, limit)
#     else:
#         size = max_size
#     search_body = create_search_body_search_after(from_date, to_date, filter_fields, size, pit_id, search_after)
#
#     es = create_es_connection(es_address, es_username, es_password)
#     res = es.search(body=search_body)
#     pit_id, search_after_no, search_after_text, all_results = process_response_after_search(res)
#
#     number_total_results = res['hits']['total']['value']
#
#     end = time.time()
#     print(end - start)
#     schema = create_schema()
#     results = {
#         'pit_id': pit_id,
#         'search_after_no': search_after_no,
#         'search_after_text': search_after_text,
#         'returned_hits': len(all_results),
#         'total_hits': number_total_results,
#         'schema': schema,
#         'results': all_results
#     }
#
#     return results

def parse_args() -> Tuple[str, str, str, dict, int, str]:
    agg = request.args.get('agg')
    subset = request.args.get('subset')
    from_date = request.args.get('from')
    to_date = request.args.get('to')
    limit = request.args.get('limit')
    scroll_id = request.args.get('scroll_id')

    # get filter keys/values from list of filter parameters
    filter_fields = {}
    with current_app.app_context():
        query_filter_parameters = current_app.query_filter_parameters
    for field in query_filter_parameters:
        value = request.args.get(field)
        if value:
            value = value.split(',')
        filter_fields[field] = value

    from_date, to_date = validate_dates(from_date, to_date)

    # TODO determine which combinations/indices we can use
    if agg == 'author' or agg == 'funder':
        agg += '_test'
    if agg == 'publisher' and subset == 'collaborations':
        return '', '', '', {}, 0, ''
    index = f"{subset}-{agg}-20201205"

    max_size = 10000
    if limit:
        limit = int(limit)
        size = min(max_size, limit)
    else:
        size = max_size

    return index, from_date, to_date, filter_fields, size, scroll_id


def search():
    start = time.time()

    index, from_date, to_date, filter_fields, size, scroll_id = parse_args()
    if index == '':
        return "Invalid combination of aggregation (publisher) and subset (collaborations)", 400
    search_body = create_search_body(from_date, to_date, filter_fields, size)

    es_username = os.environ.get('ES_USERNAME')
    es_password = os.environ.get('ES_PASSWORD')
    es_address = os.environ.get('ES_ADDRESS')
    es = create_es_connection(es_address, es_username, es_password)
    if es is None:
        return "Elasticsearch environment variable for address, username and/or password is empty", 400
    if scroll_id:
        res = es.scroll(scroll_id=scroll_id, scroll='1m')
    else:
        res = es.search(index=index, body=search_body, scroll='1m')
    scroll_id, results_data = process_response(res)

    number_total_results = res['hits']['total']['value']

    # while hits:
    #     if len(all_results) > limit:
    #         break
    #     res = es.scroll(scroll_id=scroll_id, scroll='1s')
    #     scroll_id, hits = process_response(res)
    #     all_results += hits

    end = time.time()
    print(end - start)
    schema = create_schema()
    results = {
        'scroll_id': scroll_id,
        'returned_hits': len(results_data),
        'total_hits': number_total_results,
        'schema': schema,
        'results': results_data
    }
    return results
