from datetime import datetime
from elasticsearch import Elasticsearch
import os
import pprint


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


def create_search_body(from_year: datetime, to_year: datetime, ids: list, aggregations: dict) -> dict:
    query_body = {
        "bool": {
            "must": [{
                "terms": {
                    "id.keyword": ids
                }
            }, {
                "range": {
                    "published_year": {
                        "gte": from_year.strftime("%Y-%m-%d"),
                        "lte": to_year.strftime("%Y-%m-%d"),
                        "format": "yyyy-MM-dd"
                    }
                }
            }]
        }
    }
    agg_body = {}
    for agg_type in aggregations.keys():
        fields = aggregations[agg_type]
        for field in fields:
            agg_body[agg_type + "_" + field] = {
                agg_type: {
                    "field": field
                }
            }
    search_body = {
        "query": query_body,
        "aggs": agg_body
    }
    return search_body


def main():
    from_year = datetime(1992, 12, 30)
    to_year = datetime(1993, 12, 31)
    # ids = ["SSD", "NLD"]
    # index = 'oa-metrics-country-20201205'
    ids = ["us_btaa_pstate"]
    index = 'oa-metrics-group'
    aggregations = {"sum": ["num_not_oa_outputs", "num_oa_outputs", "total_outputs"]}
    search_body = create_search_body(from_year, to_year, ids, aggregations)

    es = create_es_connection()
    res = es.search(index=index, body=search_body)
    pprint.pprint(res)
    print('done')


if __name__ == "__main__":
    main()
