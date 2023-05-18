"""
    Observatory API

    The REST API for managing and accessing data from the Observatory Platform.   # noqa: E501

    The version of the OpenAPI document: 1.0.0
    Contact: agent@observatory.academy
    Generated by: https://openapi-generator.tech
"""


import datetime
import unittest

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

        # Successfully create
        DatasetRelease(
            id=1,
            dag_id="doi_workflow",
            dataset_id="doi",
            dag_run_id="scheduled__2023-03-26T00:00:00+00:00",
            data_interval_start=dt,
            data_interval_end=dt,
            partition_date=dt,
            snapshot_date=dt,
            changefile_start_date=dt,
            changefile_end_date=dt,
            sequence_start=1,
            sequence_end=10,
            extra={},
        )

        # Created and modified are read only
        with self.assertRaises(ApiAttributeError):
            DatasetRelease(
                id=1,
                dag_id="doi_workflow",
                dataset_id="doi",
                partition_date=dt,
                snapshot_date=dt,
                start_date=dt,
                end_date=dt,
                sequence_num=1,
                extra={},
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
