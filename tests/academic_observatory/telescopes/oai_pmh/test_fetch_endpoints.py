import unittest

from academic_observatory.telescopes.oai_pmh import possible_oai_pmh_urls


class TestFetchEndpoints(unittest.TestCase):

    def test_potential_oai_pmh_urls(self):
        base_urls = ['http://dspace.nwu.ac.za/oai/request?verb=ListIdentifiers&metadataPrefix=oai_dc',
                     'https://era.ed.ac.uk/dspace-oai',
                     'http://earsiv.ebyu.edu.tr/oai/request']
        expected_results = [['http://dspace.nwu.ac.za/oai/request', 'http://dspace.nwu.ac.za/oai'],
                            ['https://era.ed.ac.uk/dspace-oai', 'https://era.ed.ac.uk'],
                            ['http://earsiv.ebyu.edu.tr/oai/request', 'http://earsiv.ebyu.edu.tr/oai']]
        results = [possible_oai_pmh_urls(url) for url in base_urls]
        self.assertListEqual(results, expected_results)
