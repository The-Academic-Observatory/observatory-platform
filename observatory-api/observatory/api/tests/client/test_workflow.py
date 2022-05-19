"""
    Observatory API

    The REST API for managing and accessing data from the Observatory Platform.   # noqa: E501

    The version of the OpenAPI document: 1.0.0
    Contact: agent@observatory.academy
    Generated by: https://openapi-generator.tech
"""


import sys
import unittest

import observatory.api.client
from observatory.api.client.model.dataset import Dataset
from observatory.api.client.model.organisation import Organisation
from observatory.api.client.model.workflow_type import WorkflowType

globals()["Dataset"] = Dataset
globals()["Organisation"] = Organisation
globals()["WorkflowType"] = WorkflowType
from observatory.api.client.model.workflow import Workflow


class TestWorkflow(unittest.TestCase):
    """Workflow unit test stubs"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testWorkflow(self):
        """Test Workflow"""
        # FIXME: construct object with mandatory attributes with example values
        # model = Workflow()  # noqa: E501
        pass


if __name__ == "__main__":
    unittest.main()
