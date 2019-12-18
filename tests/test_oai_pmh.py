import unittest

from academic_observatory.oai_pmh import fetch_context_urls, InvalidOaiPmhContextPageException


class TestOaiPmh(unittest.TestCase):

    def test_fetch_contexts(self):
        # An invalid OAI-PMH context page
        endpoint_url = 'https://espace.curtin.edu.au/'
        with self.assertRaises(InvalidOaiPmhContextPageException):
            fetch_context_urls(endpoint_url)

        # An OAI-PMH endpoint
        endpoint_url = 'https://espace.curtin.edu.au/oai/request'
        with self.assertRaises(InvalidOaiPmhContextPageException):
            fetch_context_urls(endpoint_url)

        # Valid OAI-PMH context page
        contexts_url = 'https://espace.curtin.edu.au/oai/'
        urls = fetch_context_urls(contexts_url)
        self.assertListEqual(urls, ['https://espace.curtin.edu.au/oai/request',
                                    'https://espace.curtin.edu.au/oai/driver',
                                    'https://espace.curtin.edu.au/oai/openaire',
                                    'https://espace.curtin.edu.au/oai/openaccess'])

        # Valid OAI-PMH context page with malformed URLs that have to be sanitized
        contexts_url = 'http://dspace.nwu.ac.za/oai/'
        urls = fetch_context_urls(contexts_url)
        self.assertListEqual(urls, ['http://dspace.nwu.ac.za/oai/request',
                                    'http://dspace.nwu.ac.za/oai/driver',
                                    'http://dspace.nwu.ac.za/oai/openaire'])
