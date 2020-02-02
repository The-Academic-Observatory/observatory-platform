import datetime
import unittest

from academic_observatory.telescopes.common_crawl import WarcIndex, PageInfo, InstitutionIndex, WarcIndexInfo
from academic_observatory.telescopes.common_crawl import get_fetch_months, split_page_infos


class TestCcFetchFullText(unittest.TestCase):

    def test_get_fetch_months(self):
        # Same month
        start_month = datetime.date(2020, 1, 1)
        end_month = datetime.date(2020, 1, 1)
        months = get_fetch_months(start_month, end_month)
        months_expected = [start_month]
        self.assertListEqual(months, months_expected)

        # Normal case
        start_month = datetime.date(2020, 1, 1)
        end_month = datetime.date(2020, 3, 1)
        months = get_fetch_months(start_month, end_month)
        months_expected = [datetime.date(2020, 1, 1), datetime.date(2020, 2, 1), datetime.date(2020, 3, 1)]
        self.assertListEqual(months, months_expected)

        # User doesn't specify 1 as the day argument in datetime.date
        start_month = datetime.date(2020, 1, 1)
        end_month = datetime.date(2020, 1, 31)
        months = get_fetch_months(start_month, end_month)
        months_expected = [start_month]
        self.assertListEqual(months, months_expected)

        # Check that exception is raised if start_month > end_month
        start_month = datetime.date(2020, 2, 1)
        end_month = datetime.date(2020, 1, 1)
        with self.assertRaises(Exception):
            get_fetch_months(start_month, end_month)

    def test_split_page_infos(self):
        success_page_infos_expected = []
        failed_page_infos_expected = []

        for i in range(10):
            institution_index = InstitutionIndex('', '', '', '', '')
            warc_index = WarcIndex('', '', 0, 0)
            warc_index_info = WarcIndexInfo(datetime.date(2020, 1, 1), '', '', '', '', '', '', '', '', '', '', '', '',
                                            8080, '', '', datetime.datetime.now(), 200, '', '', '', '', '')
            page_info = PageInfo(institution_index, warc_index, warc_index_info)

            if i % 2 == 0:
                success_page_infos_expected.append(page_info)
            else:
                page_info.warc_index_info.fetch_status = 403
                failed_page_infos_expected.append(page_info)

        page_infos = success_page_infos_expected + failed_page_infos_expected

        # Test that split_page_infos separates the successful from unsuccessful correctly
        success_page_infos, failed_page_infos = split_page_infos(page_infos)
        self.assertListEqual(success_page_infos, success_page_infos_expected)
        self.assertListEqual(failed_page_infos, failed_page_infos_expected)
