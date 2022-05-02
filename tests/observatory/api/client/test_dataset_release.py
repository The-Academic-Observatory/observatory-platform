"""
    Observatory API

    The REST API for managing and accessing data from the Observatory Platform.   # noqa: E501

    The version of the OpenAPI document: 1.0.0
    Contact: agent@observatory.academy
    Generated by: https://openapi-generator.tech
"""


import unittest

from observatory.api.client.model.dataset import Dataset

globals()["Dataset"] = Dataset
import datetime

from observatory.api.client.exceptions import ApiAttributeError, ApiTypeError
from observatory.api.client.model.dataset_release import DatasetRelease


class TestDatasetRelease(unittest.TestCase):
    """DatasetRelease unit test stubs"""

    def testDatasetRelease(self):
        """Test DatasetRelease"""

        class Configuration:
            def __init__(self):
                self.discard_unknown_keys = True

        dt = datetime.datetime.utcnow()
        DatasetRelease(
            id=1,
            start_date=dt,
            end_date=dt,
            extra={},
            _configuration=Configuration(),
            unknown="var",
        )

        self.assertRaises(
            ApiAttributeError,
            DatasetRelease,
            start_date=dt,
            end_date=dt,
            extra={},
            _configuration=Configuration(),
            unknown="var",
            created=dt,
            modified=dt,
        )

        # Invalid argument
        with self.assertRaises(ApiTypeError):
            DatasetRelease("hello")

        # Invalid keyword argument
        with self.assertRaises(ApiAttributeError):
            DatasetRelease(hello="world")

        self.assertRaises(ApiTypeError, DatasetRelease._from_openapi_data, "hello")

        DatasetRelease._from_openapi_data(hello="world", _configuration=Configuration())


if __name__ == "__main__":
    unittest.main()
