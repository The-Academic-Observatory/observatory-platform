import unittest
from academic_observatory.common_crawl import common_crawl_serialize_custom_types
from academic_observatory.common_crawl import WarcIndex
import datetime


class TestCommonCrawl(unittest.TestCase):

    def test_common_crawl_serialize_custom_types(self):
        # datetime.datetime
        datetime_instance = datetime.datetime(year=2020, month=1, day=1, minute=0, second=0, microsecond=0,
                                              tzinfo=datetime.timezone.utc)
        datetime_actual = common_crawl_serialize_custom_types(datetime_instance)
        datetime_expected = '2020-01-01T00:00:00+00:00'
        self.assertEqual(datetime_actual, datetime_expected)

        # datetime.date
        date_instance = datetime.date(year=2020, month=1, day=1)
        date_actual = common_crawl_serialize_custom_types(date_instance)
        date_expected = '2020-01-01'
        self.assertEqual(date_actual, date_expected)

        # non supported type
        with self.assertRaises(TypeError):
            common_crawl_serialize_custom_types(WarcIndex('', '', 0, 0))
