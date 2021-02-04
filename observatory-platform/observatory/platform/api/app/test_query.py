import unittest
from observatory.platform.api.app.api.query import (create_es_connection,
                                                              create_search_body,
                                                              validate_dates,
                                                              process_response,
                                                              create_schema,
                                                              parse_args,
                                                              search)

from observatory.platform.api.app.server import app


class TestQuery(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestQuery, self).__init__(*args, **kwargs)
        # from enum in openapi.yml
        self.aggregations = ['author', 'country', 'funder', 'group', 'institution', 'publisher']
        self.subsets = ['citations', 'collaborations', 'disciplines', 'events', 'funders', 'journals', 'oa-metrics',
                        'output-types', 'publishers']
        self.scroll_id = 'FGluY2x1ZGVfY29udGV4dF91dWlkDXF1ZXJ5QW5kRmV0Y2gBFlVrbkJuSmNzVFMtSUh3MkcxVmxXRVEAAAAAAJDtdhZkejJRLUUwZ1N0T05nUTV6ZUlVNGRn'

    def setUp(self):
        self.app = app.app.test_client()
        with app.app.app_context():
            self.query_filter_parameters = app.app.query_filter_parameters

    def test_main_page(self):
        response = self.app.get('/', follow_redirects=True)
        self.assertEqual(response.status_code, 200)

    def test_create_es_connection(self):
        #TODO mock ElasticSearch
        pass

    def test_parse_args(self):
        with self.app as c:
            # test minimal parameters
            parameters = {'agg': 'author', 'subset': 'citations'}
            c.get('/query', query_string=parameters)
            index, from_date, to_date, filter_fields, size, scroll_id = parse_args()
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
            index, from_date, to_date, filter_fields, size, scroll_id = parse_args()
            for field in filter_fields:
                self.assertEqual(['test1', 'test2'], filter_fields[field])

            # test dates
            parameters = {'from': '2000', 'to': '2010'}
            c.get('/query', query_string=parameters)
            index, from_date, to_date, filter_fields, size, scroll_id = parse_args()
            self.assertEqual('2000-12-31', from_date)
            self.assertEqual('2010-12-31', to_date)

            parameters = {'from': '2020-01-01', 'to': '2010'}
            c.get('/query', query_string=parameters)
            with self.assertRaises(ValueError):
                parse_args()

            # test indexes
            parameters = {}
            for agg in self.aggregations:
                for subset in self.subsets:
                    parameters['agg'] = agg
                    parameters['subset'] = subset
                    c.get('/query', query_string=parameters)
                    index, from_date, to_date, filter_fields, size, scroll_id = parse_args()

                    if agg == 'publisher' and subset == 'collaborations':
                        self.assertEqual('', index)
                    else:
                        if agg == 'author' or agg == 'funder':
                            self.assertEqual(f"{subset}-{agg}_test-20201205", index)
                        else:
                            self.assertEqual(f"{subset}-{agg}-20201205", index)

            # test limit
            parameters = {'limit': 500}
            c.get('/query', query_string=parameters)
            index, from_date, to_date, filter_fields, size, scroll_id = parse_args()
            self.assertEqual(500, size)

            c.get('/query')
            index, from_date, to_date, filter_fields, size, scroll_id = parse_args()
            self.assertEqual(10000, size)

            # test scroll_id
            parameters = {'scroll_id': self.scroll_id}
            c.get('/query', query_string=parameters)
            index, from_date, to_date, filter_fields, size, scroll_id = parse_args()
            self.assertEqual(self.scroll_id, scroll_id)

    def test_create_search_body(self):
        # test dates
        from_year = '2000-12-31'
        to_year = '2010-12-31'
        filter_fields = dict.fromkeys(self.query_filter_parameters)
        size = 500
        search_body = create_search_body(from_year, to_year, filter_fields, size)
        expected_range_dict = {'range': {'published_year': {'format': 'yyyy-MM-dd', 'gte': from_year, 'lt': to_year}}}
        self.assertIn(expected_range_dict, search_body['query']['bool']['filter'])

        from_year = None
        to_year = '2010-12-31'
        search_body = create_search_body(from_year, to_year, filter_fields, size)
        expected_range_dict = {'range': {'published_year': {'format': 'yyyy-MM-dd', 'lt': to_year}}}
        self.assertIn(expected_range_dict, search_body['query']['bool']['filter'])

        from_year = '2000-12-31'
        to_year = None
        search_body = create_search_body(from_year, to_year, filter_fields, size)
        expected_range_dict = {'range': {'published_year': {'format': 'yyyy-MM-dd', 'gte': from_year}}}
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

    def test_validate_dates(self):
        from_date = '2000'
        to_date = '2010'
        from_valid, to_valid = validate_dates(from_date, to_date)
        self.assertEqual(f"{from_date}-12-31", from_valid)
        self.assertEqual(f"{to_date}-12-31", to_valid)

        from_date = '-10'
        to_date = '2010'
        with self.assertRaises(ValueError):
            validate_dates(from_date, to_date)

    def test_process_response(self):
        res = {
            '_scroll_id': self.scroll_id,
            'hits': {
                'hits': [{'_index': 'journals-institution-20201205', '_type': '_doc', '_id': 'bQ88QXYBGinIh2YA4IaO',
                          '_score': None, '_source': {'id': 'example_id', 'name': 'Example Name', 'published_year': '2018-12-31'},
                          'sort': [77250]}, {
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
        scroll_id, hits = process_response(res)
        self.assertEqual(self.scroll_id, scroll_id)
        expected_hits = [{'_index': 'journals-institution-20201205', '_type': '_doc', '_id': 'bQ88QXYBGinIh2YA4IaO',
                          '_score': None, 'sort': [77250], 'id': 'example_id', 'name': 'Example Name', 'published_year': '2018-12-31'}, {'_index': 'journals-institution-20201205', '_type': '_doc', '_id': 'bQ88QXYBGinIh2YA4Ia1', '_score': None, 'sort': [77251], 'id': 'example_id2', 'name': 'Example Name2', 'published_year': '2018-12-31'}]
        self.assertEqual(expected_hits, hits)

    def test_create_schema(self):
        pass

    def test_search(self):
        #TODO mock es.search and es.scroll
        #assert 400 returns and results return
        pass