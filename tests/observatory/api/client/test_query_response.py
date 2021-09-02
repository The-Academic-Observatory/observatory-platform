"""
    Observatory API

    The REST API for managing and accessing data from the Observatory Platform.   # noqa: E501

    The version of the OpenAPI document: 1.0.0
    Contact: agent@observatory.academy
    Generated by: https://openapi-generator.tech
"""

import copy
import unittest

from observatory.api.client.exceptions import ApiTypeError, ApiAttributeError
from observatory.api.client.model.query_response import QueryResponse
from tests.observatory.api.client.test_observatory_api import RES_EXAMPLE, SCROLL_ID


class TestQueryResponse(unittest.TestCase):
    """QueryResponse unit test stubs"""

    def testQueryResponse(self):
        """Test QueryResponse"""

        res = copy.deepcopy(RES_EXAMPLE)
        QueryResponse(
            version="v1",
            index="citations-country-20200101",
            scroll_id=SCROLL_ID,
            returned_hits=1000,
            total_hits=10000,
            schema={"schema": "to_be_created"},
            results=res["hits"]["hits"],
        )

        # Invalid argument
        with self.assertRaises(ApiTypeError):
            QueryResponse("hello")

        # Invalid keyword argument
        with self.assertRaises(ApiAttributeError):
            QueryResponse(hello="world")


if __name__ == "__main__":
    unittest.main()