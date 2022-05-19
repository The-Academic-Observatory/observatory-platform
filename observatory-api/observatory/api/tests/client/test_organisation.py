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
from observatory.api.client.model.workflow import Workflow

globals()["Workflow"] = Workflow
from observatory.api.client.model.organisation import Organisation


class TestOrganisation(unittest.TestCase):
    """Organisation unit test stubs"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testOrganisation(self):
        """Test Organisation"""
        # FIXME: construct object with mandatory attributes with example values
        # model = Organisation()  # noqa: E501
        pass


if __name__ == "__main__":
    unittest.main()
