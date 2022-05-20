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
from observatory.api.client.model.workflow_type import WorkflowType


class TestWorkflowType(unittest.TestCase):
    """WorkflowType unit test stubs"""

    def testWorkflowType(self):
        """Test WorkflowType"""

        class Configuration:
            def __init__(self):
                self.discard_unknown_keys = True

        # Create valid object
        dt = datetime.datetime.utcnow()
        WorkflowType(
            id=1,
            type_id="onix",
            name="ONIX Telescope",
            _configuration=Configuration(),
            unknown="var",
        )

        self.assertRaises(
            ApiAttributeError, WorkflowType, id=1, type_id="onix", name="ONIX Telescope", created=dt, modified=dt
        )

        # Invalid argument
        with self.assertRaises(ApiTypeError):
            WorkflowType("hello")

        # Invalid keyword argument
        with self.assertRaises(ApiAttributeError):
            WorkflowType(hello="world")

        self.assertRaises(ApiTypeError, WorkflowType._from_openapi_data, "hello")

        WorkflowType._from_openapi_data(hello="world", _configuration=Configuration())


if __name__ == "__main__":
    unittest.main()