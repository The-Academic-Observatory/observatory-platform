# Copyright 2021 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: James Diprose

from __future__ import annotations

import json
import os

from observatory.platform.utils.config_utils import module_file_path
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.utils.test_utils import ObservatoryEnvironment, ObservatoryTestCase
from observatory.platform.utils.workflow_utils import make_dag_id

from oaebu_workflows.config import elastic_mappings_folder
from oaebu_workflows.dags.elastic_import_workflow import load_elastic_mappings_oaebu


class TestElasticImportWorkflow(ObservatoryTestCase):
    """Tests for the Elastic Import Workflow"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

    def test_load_elastic_mappings_oaebu(self):
        """Test load_elastic_mappings_oaebu"""

        aggregate_level = "product"
        path = elastic_mappings_folder()
        expected = [
            (
                "oaebu_anu_press_book_product_author_metrics",
                render_template(
                    os.path.join(path, "oaebu-author-metrics-mappings.json.jinja2"), aggregation_level=aggregate_level
                ),
            ),
            (
                "oaebu_anu_press_book_product_list",
                render_template(
                    os.path.join(path, "oaebu-list-mappings.json.jinja2"), aggregation_level=aggregate_level
                ),
            ),
            (
                "oaebu_anu_press_book_product_metrics",
                render_template(
                    os.path.join(path, "oaebu-metrics-mappings.json.jinja2"), aggregation_level=aggregate_level
                ),
            ),
            (
                "oaebu_anu_press_book_product_metrics_city",
                render_template(
                    os.path.join(path, "oaebu-metrics-city-mappings.json.jinja2"), aggregation_level=aggregate_level
                ),
            ),
            (
                "oaebu_anu_press_book_product_metrics_country",
                render_template(
                    os.path.join(path, "oaebu-metrics-country-mappings.json.jinja2"), aggregation_level=aggregate_level
                ),
            ),
            (
                "oaebu_anu_press_book_product_metrics_events",
                render_template(
                    os.path.join(path, "oaebu-metrics-events-mappings.json.jinja2"), aggregation_level=aggregate_level
                ),
            ),
            (
                "oaebu_anu_press_book_product_metrics_institution",
                render_template(
                    os.path.join(path, "oaebu-metrics-institution-mappings.json.jinja2"),
                    aggregation_level=aggregate_level,
                ),
            ),
            (
                "oaebu_anu_press_book_product_metrics_referrer",
                render_template(
                    os.path.join(path, "oaebu-metrics-referrer-mappings.json.jinja2"), aggregation_level=aggregate_level
                ),
            ),
            (
                "oaebu_anu_press_book_product_publisher_metrics",
                render_template(
                    os.path.join(path, "oaebu-publisher-metrics-mappings.json.jinja2"),
                    aggregation_level=aggregate_level,
                ),
            ),
            (
                "oaebu_anu_press_book_product_subject_metrics",
                render_template(
                    os.path.join(path, "oaebu-subject-metrics-mappings.json.jinja2"), aggregation_level=aggregate_level
                ),
            ),
            (
                "oaebu_anu_press_book_product_subject_year_metrics",
                render_template(
                    os.path.join(path, "oaebu-subject-year-metrics-mappings.json.jinja2"),
                    aggregation_level=aggregate_level,
                ),
            ),
            (
                "oaebu_anu_press_book_product_year_metrics",
                render_template(
                    os.path.join(path, "oaebu-year-metrics-mappings.json.jinja2"), aggregation_level=aggregate_level
                ),
            ),
            (
                "oaebu_anu_press_unmatched_book_metrics",
                render_template(
                    os.path.join(path, "oaebu-unmatched-metrics-mappings.json.jinja2"),
                    aggregation_level=aggregate_level,
                ),
            ),
        ]

        for table_id, expected_mappings_str in expected:
            print(table_id)
            expected_mappings = json.loads(expected_mappings_str)
            actual_mappings = load_elastic_mappings_oaebu(path, table_id)
            self.assertEqual(expected_mappings, actual_mappings)

    def test_dag_load(self):
        """Test that the DAG can be loaded from a DAG bag.

        :return: None
        """

        env = ObservatoryEnvironment(self.project_id, self.data_location, enable_api=False)
        with env.create():
            expected_dag_ids = [
                make_dag_id("elastic_import", suffix)
                for suffix in ["anu_press", "ucl_press", "wits_press", "university_of_michigan_press"]
            ]

            dag_file = os.path.join(module_file_path("oaebu_workflows.dags"), "elastic_import_workflow.py")
            for dag_id in expected_dag_ids:
                self.assert_dag_load(dag_id, dag_file)
