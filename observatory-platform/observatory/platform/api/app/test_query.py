import copy
import unittest
from unittest.mock import patch

from elasticsearch import Elasticsearch
from observatory.platform.api.app.app import app
from observatory.platform.api.app.elastic import (create_es_connection,
                                                  create_search_body,
                                                  list_available_index_dates,
                                                  parse_args,
                                                  process_response,
                                                  searchv1)


class TestQuery(unittest.TestCase):
    """ Tests for the 'query' endpoint of the API. """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestQuery, self).__init__(*args, **kwargs)
        # from enum in openapi.yml
        self.aggregations = ['author', 'country', 'funder', 'group', 'institution', 'publisher']
        self.subsets = ['citations', 'collaborations', 'disciplines', 'events', 'funders', 'journals', 'oa-metrics',
                        'output-types', 'publishers']
        self.scroll_id = \
            'FGluY2x1ZGVfY29udGV4dF91dWlkDXF1ZXJ5QW5kRmV0Y2gBFlVrbkJuSmNzVFMtSUh3MkcxVmxXRVEAAAAAAJDtdhZkejJRLUUwZ1N0T05nUTV6ZUlVNGRn '
        self.res_example = {
            '_scroll_id': self.scroll_id,
            'hits': {
                'total': {
                    'value': 452
                },
                'hits': [{
                             '_index': 'journals-institution-20201205',
                             '_type': '_doc',
                             '_id': 'bQ88QXYBGinIh2YA4IaO',
                             '_score': None,
                             '_source': {
                                 'id': 'example_id',
                                 'name': 'Example Name',
                                 'published_year': '2018-12-31'
                             },
                             'sort': [77250]
                         }, {
                             '_index': 'journals-institution-20201205',
                             '_type': '_doc',
                             '_id': 'bQ88QXYBGinIh2YA4Ia1',
                             '_score': None,
                             '_source': {
                                 'id': 'example_id2',
                                 'name': 'Example Name2',
                                 'published_year': '2018-12-31'
                             },
                             'sort': [77251]
                         }]
            }
        }

    def setUp(self):
        """ Set-up the app and app_context for the unit tests.

        :return: None.
        """
        self.app = app.app.test_client()
        with app.app.app_context():
            self.query_filter_parameters = app.app.query_filter_parameters

    def test_main_page(self):
        """ Test that the main page can be accessed succesfully.

        :return: None.
        """
        response = self.app.get('/', follow_redirects=True)
        self.assertEqual(response.status_code, 200)

    @patch('observatory.platform.api.app.elastic.Elasticsearch.ping')
    def test_create_es_connection(self, mock_elasticsearch_ping):
        """ Test creating elasticsearch connection with (in)valid address and api_key

        :return:
        """
        # test elasticsearch instance is returned
        mock_elasticsearch_ping.return_value = True
        es = create_es_connection('address', 'api_key')
        self.assertIsInstance(es, Elasticsearch)

        # test connection error is raised
        mock_elasticsearch_ping.return_value = False
        with self.assertRaises(ConnectionError):
            create_es_connection('address', 'api_key')

        # test None is returned when address or api_key is empty
        es = create_es_connection('address', '')
        self.assertIsNone(es)

        es = create_es_connection(None, 'api_key')
        self.assertIsNone(es)

    def test_parse_args(self):
        """ Test that parse_args returns values as expected when using different query parameters.

        :return: None.
        """
        with self.app as c:
            # test minimal parameters
            parameters = {
                'agg': 'author',
                'subset': 'citations'
            }
            c.get('/query', query_string=parameters)
            alias, index_date, from_date, to_date, filter_fields, size, scroll_id = parse_args()
            self.assertIsNone(from_date)
            self.assertIsNone(to_date)
            for field in filter_fields:
                self.assertIsNone(filter_fields[field])
            self.assertEqual(10000, size)
            self.assertIsNone(scroll_id)

            # test filter parameters
            parameters = {}
            for field in self.query_filter_parameters:
                parameters[field] = 'test1,test2'
            c.get('/query', query_string=parameters)
            alias, index_date, from_date, to_date, filter_fields, size, scroll_id = parse_args()
            for field in filter_fields:
                self.assertEqual(['test1', 'test2'], filter_fields[field])

            # test from & to dates
            parameters = {
                'from': '2000',
                'to': '2010'
            }
            c.get('/query', query_string=parameters)
            alias, index_date, from_date, to_date, filter_fields, size, scroll_id = parse_args()
            self.assertEqual('2000-12-31', from_date)
            self.assertEqual('2010-12-31', to_date)

            # TODO use openapi config so that this will not be allowed
            parameters = {
                'from': '2020-01-01',
                'to': '2010'
            }
            c.get('/query', query_string=parameters)
            alias, index_date, from_date, to_date, filter_fields, size, scroll_id = parse_args()

            # test aliases
            parameters = {}
            for agg in self.aggregations:
                for subset in self.subsets:
                    parameters['agg'] = agg
                    parameters['subset'] = subset
                    c.get('/query', query_string=parameters)
                    alias, index_date, from_date, to_date, filter_fields, size, scroll_id = parse_args()

                    if agg == 'publisher' and subset == 'collaborations':
                        self.assertEqual('', alias)
                    else:
                        if agg == 'author' or agg == 'funder':
                            self.assertEqual(f"{subset}-{agg}_test", alias)
                        else:
                            self.assertEqual(f"{subset}-{agg}", alias)

            # test index date
            parameters = {
                'index_date': '2020-01-01'
            }
            c.get('/query', query_string=parameters)
            alias, index_date, from_date, to_date, filter_fields, size, scroll_id = parse_args()
            self.assertEqual('2020-01-01', index_date)

            c.get('/query')
            alias, index_date, from_date, to_date, filter_fields, size, scroll_id = parse_args()
            self.assertEqual(None, index_date)

            # test limit
            parameters = {
                'limit': 500
            }
            c.get('/query', query_string=parameters)
            alias, index_date, from_date, to_date, filter_fields, size, scroll_id = parse_args()
            self.assertEqual(500, size)

            c.get('/query')
            alias, index_date, from_date, to_date, filter_fields, size, scroll_id = parse_args()
            self.assertEqual(10000, size)

            # test scroll_id
            parameters = {
                'scroll_id': self.scroll_id
            }
            c.get('/query', query_string=parameters)
            alias, index_date, from_date, to_date, filter_fields, size, scroll_id = parse_args()
            self.assertEqual(self.scroll_id, scroll_id)

    def test_create_search_body(self):
        """ Test that resulting search_body is as expected with different arguments.

        :return: None.
        """
        # test dates
        from_year = '2000-12-31'
        to_year = '2010-12-31'
        filter_fields = dict.fromkeys(self.query_filter_parameters)
        size = 500
        search_body = create_search_body(from_year, to_year, filter_fields, size)
        expected_range_dict = {
            'range': {
                'published_year': {
                    'format': 'yyyy-MM-dd',
                    'gte': from_year,
                    'lt': to_year
                }
            }
        }
        self.assertIn(expected_range_dict, search_body['query']['bool']['filter'])

        from_year = None
        to_year = '2010-12-31'
        search_body = create_search_body(from_year, to_year, filter_fields, size)
        expected_range_dict = {
            'range': {
                'published_year': {
                    'format': 'yyyy-MM-dd',
                    'lt': to_year
                }
            }
        }
        self.assertIn(expected_range_dict, search_body['query']['bool']['filter'])

        from_year = '2000-12-31'
        to_year = None
        search_body = create_search_body(from_year, to_year, filter_fields, size)
        expected_range_dict = {
            'range': {
                'published_year': {
                    'format': 'yyyy-MM-dd',
                    'gte': from_year
                }
            }
        }
        self.assertIn(expected_range_dict, search_body['query']['bool']['filter'])

        # test empty dates and empty filter fields
        from_year = None
        to_year = None
        search_body = create_search_body(from_year, to_year, filter_fields, size)
        self.assertEqual([], search_body['query']['bool']['filter'])

        # test filter fields
        from_year = '2000-12-31'
        to_year = '2010-12-31'
        filter_fields = {}
        for field in self.query_filter_parameters:
            filter_fields[field] = ['test1', 'test2']
        search_body = create_search_body(from_year, to_year, filter_fields, size)
        for field in filter_fields:
            list_entry = {
                "terms": {
                    f"{field}.keyword": filter_fields[field]
                }
            }
            self.assertIn(list_entry, search_body['query']['bool']['filter'])

        # test size
        size = 500
        search_body = create_search_body(from_year, to_year, filter_fields, size)
        self.assertEqual(500, search_body['size'])

    def test_process_response(self):
        """ Test that the response from the elasticsearch search/scroll query is as expected.

        :return:
        """
        scroll_id, hits = process_response(copy.deepcopy(self.res_example))
        self.assertEqual(self.scroll_id, scroll_id)
        expected_hits = [{
                             '_index': 'journals-institution-20201205',
                             '_type': '_doc',
                             '_id': 'bQ88QXYBGinIh2YA4IaO',
                             '_score': None,
                             'sort': [77250],
                             'id': 'example_id',
                             'name': 'Example Name',
                             'published_year': '2018-12-31'
                         }, {
                             '_index': 'journals-institution-20201205',
                             '_type': '_doc',
                             '_id': 'bQ88QXYBGinIh2YA4Ia1',
                             '_score': None,
                             'sort': [77251],
                             'id': 'example_id2',
                             'name': 'Example Name2',
                             'published_year': '2018-12-31'
                         }]
        self.assertEqual(expected_hits, hits)

    @patch('elasticsearch.client.CatClient.indices')
    def test_list_available_index_dates(self, mock_es_indices):
        """ Test parsing of available dates from elasticsearch's cat.indices() response.

        :return: None.
        """
        dates = ['20201212', '20201201']
        alias = 'subset-agg'
        mock_es_indices.return_value = [{
                                            'index': f'{alias}-{dates[0]}'
                                        }, {
                                            'index': f'{alias}-{dates[1]}'
                                        }]

        available_dates = list_available_index_dates(Elasticsearch(), alias)
        self.assertEqual(dates, available_dates)

    def test_create_schema(self):
        """ To be created. 'create_schema' function is not implemented yet.

        :return: None.
        """
        pass

    @patch('observatory.platform.api.app.elastic.Elasticsearch.scroll')
    @patch('observatory.platform.api.app.elastic.Elasticsearch.search')
    @patch('observatory.platform.api.app.elastic.create_es_connection')
    @patch('observatory.platform.api.app.elastic.parse_args')
    def test_search(self, mock_parse_args, mock_create_connection, mock_es_search, mock_es_scroll):
        """ Test elasticsearch search query with different args.
        :return: None.
        """
        # set parse_args return values
        alias = 'subset-agg'
        index_date = None
        from_date = None
        to_date = None
        filter_fields = {}
        size = 10000
        scroll_id = None

        # test es connection is None
        mock_parse_args.return_value = (alias, index_date, from_date, to_date, filter_fields, size, scroll_id)
        mock_create_connection.return_value = None

        error, return_status = searchv1()
        self.assertEqual(400, return_status)

        # mock es connection
        mock_create_connection.return_value = Elasticsearch()

        # test with scroll id
        mock_parse_args.return_value = (alias, index_date, from_date, to_date, filter_fields, size, self.scroll_id)
        res = copy.deepcopy(self.res_example)
        mock_es_scroll.return_value = res

        results = searchv1()
        expected_results = {
            'version': 'v1',
            'index': 'N/A',
            'scroll_id': self.scroll_id,
            'returned_hits': len(res['hits']['hits']),
            'total_hits': res['hits']['total']['value'],
            'schema': {
                'schema': 'to_be_created'
            },
            'results': res['hits']['hits']
        }
        self.assertEqual(expected_results, results)

        # with search body, test with empty (invalid) alias
        mock_es_search.return_value = copy.deepcopy(self.res_example)
        mock_parse_args.return_value = ('', index_date, from_date, to_date, filter_fields, size, scroll_id)

        error, return_status = searchv1()
        self.assertEqual(400, return_status)

        # with search body, test with valid alias and without index date
        with patch('elasticsearch.client.CatClient.aliases') as mock_es_cat:
            res = copy.deepcopy(self.res_example)
            mock_es_search.return_value = res
            mock_parse_args.return_value = (alias, index_date, from_date, to_date, filter_fields, size, scroll_id)
            index_name = 'subset-agg-20201212'
            mock_es_cat.return_value = [{
                                            'index': index_name
                                        }]

            results = searchv1()
            expected_results = {
                'version': 'v1',
                'index': index_name,
                'scroll_id': self.scroll_id,
                'returned_hits': len(res['hits']['hits']),
                'total_hits': res['hits']['total']['value'],
                'schema': {
                    'schema': 'to_be_created'
                },
                'results': res['hits']['hits']
            }
            self.assertEqual(expected_results, results)

        # with search body, test with valid index date
        with patch('elasticsearch.client.IndicesClient.exists') as mock_es_indices:
            res = copy.deepcopy(self.res_example)
            mock_es_search.return_value = res
            index_date = '20200101'
            mock_parse_args.return_value = (alias, index_date, from_date, to_date, filter_fields, size, scroll_id)
            mock_es_indices.return_value = True

            results = searchv1()
            expected_results = {
                'version': 'v1',
                'index': alias + f"-{index_date}",
                'scroll_id': self.scroll_id,
                'returned_hits': len(res['hits']['hits']),
                'total_hits': res['hits']['total']['value'],
                'schema': {
                    'schema': 'to_be_created'
                },
                'results': res['hits']['hits']
            }
            self.assertEqual(expected_results, results)

            # with search body, test with invalid index date
            with patch('observatory.platform.api.app.elastic.list_available_index_dates') as mock_index_dates:
                res = copy.deepcopy(self.res_example)
                mock_es_search.return_value = res
                index_date = '20200101'
                mock_parse_args.return_value = (alias, index_date, from_date, to_date, filter_fields, size, scroll_id)
                mock_es_indices.return_value = False
                available_date = '20201212'
                mock_index_dates.return_value = [available_date]

                error, return_status = searchv1()
                self.assertEqual(400, return_status)
                self.assertIn(available_date, error)
