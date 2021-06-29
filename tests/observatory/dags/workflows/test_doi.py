# Copyright 2020 Curtin University
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

import os
from datetime import datetime
from typing import Dict, List

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.dummy_operator import DummyOperator

from observatory.dags.model import (
    bq_load_observatory_dataset,
    make_observatory_dataset,
    Institution,
    make_doi_table,
    make_country_table,
    sort_events,
)
from observatory.dags.workflows.doi_workflow import DoiWorkflow, make_dataset_transforms, make_elastic_tables
from observatory.platform.utils.airflow_utils import set_task_state
from observatory.platform.utils.gc_utils import run_bigquery_query
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
)


def make_dummy_dag(dag_id: str, execution_date: datetime) -> DAG:
    """ A Dummy DAG for testing purposes.

    :param dag_id: the DAG id.
    :param execution_date: the DAGs execution date.
    :return: the DAG.
    """

    with DAG(
        dag_id=dag_id,
        schedule_interval="@weekly",
        default_args={"owner": "airflow", "start_date": execution_date},
        catchup=False,
    ) as dag:
        task1 = DummyOperator(task_id="dummy_task")

    return dag


class TestDoiWorkflow(ObservatoryTestCase):
    """ Tests for the functions used by the Doi workflow """

    def __init__(self, *args, **kwargs):
        super(TestDoiWorkflow, self).__init__(*args, **kwargs)
        # GCP settings
        self.gcp_project_id: str = os.getenv("TEST_GCP_PROJECT_ID")
        self.gcp_bucket_name: str = os.getenv("TEST_GCP_BUCKET_NAME")
        self.gcp_data_location: str = os.getenv("TEST_GCP_DATA_LOCATION")

        # Institutions
        inst_curtin = Institution(
            1,
            name="Curtin University",
            grid_id="grid.1032.0",
            country_code="AUS",
            country_code_2="AU",
            region="Oceania",
            subregion="Australia and New Zealand",
            types="Education",
            home_repo="curtin.edu.au",
            country="Australia",
            coordinates="-32.005931, 115.894397",
        )
        inst_anu = Institution(
            2,
            name="Australian National University",
            grid_id="grid.1001.0",
            country_code="AUS",
            country_code_2="AU",
            region="Oceania",
            subregion="Australia and New Zealand",
            types="Education",
            home_repo="anu.edu.au",
            country="Australia",
            coordinates="-35.2778, 149.1205",
        )
        inst_akl = Institution(
            3,
            name="University of Auckland",
            grid_id="grid.9654.e",
            country_code="NZL",
            country_code_2="NZ",
            region="Oceania",
            subregion="Australia and New Zealand",
            types="Education",
            home_repo="auckland.ac.nz",
            country="New Zealand",
            coordinates="-36.852304, 174.767734",
        )
        self.institutions = [inst_curtin, inst_anu, inst_akl]

    def test_set_task_state(self):
        """ Test

        :return:
        """

        set_task_state(True, "my-task-id")
        with self.assertRaises(AirflowException):
            set_task_state(False, "my-task-id")

    def test_dag_structure(self):
        """Test that the DOI DAG has the correct structure.

        :return: None
        """

        dag = DoiWorkflow().make_dag()
        self.assert_dag_structure(
            {
                "crossref_metadata_sensor": ["check_dependencies"],
                "crossref_fundref_sensor": ["check_dependencies"],
                "geonames_sensor": ["check_dependencies"],
                "grid_sensor": ["check_dependencies"],
                "mag_sensor": ["check_dependencies"],
                "open_citations_sensor": ["check_dependencies"],
                "unpaywall_sensor": ["check_dependencies"],
                "check_dependencies": ["create_datasets"],
                "create_datasets": [
                    "create_crossref_events",
                    "create_crossref_fundref",
                    "create_grid",
                    "create_mag",
                    "create_orcid",
                    "create_open_citations",
                    "create_unpaywall",
                ],
                "create_crossref_events": ["create_doi"],
                "create_crossref_fundref": ["create_doi"],
                "create_grid": ["create_doi"],
                "create_mag": ["create_doi"],
                "create_orcid": ["create_doi"],
                "create_open_citations": ["create_doi"],
                "create_unpaywall": ["create_doi"],
                "create_doi": ["create_book",],
                "create_book": [
                    "create_country",
                    "create_funder",
                    "create_group",
                    "create_institution",
                    "create_author",
                    "create_journal",
                    "create_publisher",
                    "create_region",
                    "create_subregion",
                ],
                "create_country": ["copy_to_dashboards"],
                "create_funder": ["copy_to_dashboards"],
                "create_group": ["copy_to_dashboards"],
                "create_institution": ["copy_to_dashboards"],
                "create_author": ["copy_to_dashboards"],
                "create_journal": ["copy_to_dashboards"],
                "create_publisher": ["copy_to_dashboards"],
                "create_region": ["copy_to_dashboards"],
                "create_subregion": ["copy_to_dashboards"],
                "copy_to_dashboards": ["create_dashboard_views"],
                "create_dashboard_views": [
                    "export_country",
                    "export_funder",
                    "export_group",
                    "export_institution",
                    "export_author",
                    "export_journal",
                    "export_publisher",
                    "export_region",
                    "export_subregion",
                ],
                "export_country": [],
                "export_funder": [],
                "export_group": [],
                "export_institution": [],
                "export_author": [],
                "export_journal": [],
                "export_publisher": [],
                "export_region": [],
                "export_subregion": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the DOI can be loaded from a DAG bag.

        :return: None
        """

        env = ObservatoryEnvironment(self.gcp_project_id, self.gcp_data_location)
        with env.create():
            dag_file = os.path.join(module_file_path("observatory.dags.dags"), "doi.py")
            self.assert_dag_load("doi", dag_file)

    def test_telescope(self):
        """Test the DOI telescope end to end.

        :return: None.
        """

        # Create datasets
        env = ObservatoryEnvironment(project_id=self.gcp_project_id, data_location=self.gcp_data_location)
        fake_dataset_id = env.add_dataset(prefix="fake")
        intermediate_dataset_id = env.add_dataset(prefix="intermediate")
        dashboards_dataset_id = env.add_dataset(prefix="dashboards")
        observatory_dataset_id = env.add_dataset(prefix="observatory")
        elastic_dataset_id = env.add_dataset(prefix="elastic")
        settings_dataset_id = env.add_dataset(prefix="settings")
        dataset_transforms = make_dataset_transforms(
            dataset_id_crossref_events=fake_dataset_id,
            dataset_id_crossref_metadata=fake_dataset_id,
            dataset_id_crossref_fundref=fake_dataset_id,
            dataset_id_grid=fake_dataset_id,
            dataset_id_iso=fake_dataset_id,
            dataset_id_mag=fake_dataset_id,
            dataset_id_orcid=fake_dataset_id,
            dataset_id_open_citations=fake_dataset_id,
            dataset_id_unpaywall=fake_dataset_id,
            dataset_id_settings=settings_dataset_id,
            dataset_id_observatory=observatory_dataset_id,
            dataset_id_observatory_intermediate=intermediate_dataset_id,
        )
        transforms, transform_doi, transform_book = dataset_transforms

        with env.create():
            # Make dag
            start_date = pendulum.datetime(year=2021, month=5, day=9)
            doi_dag = DoiWorkflow(
                intermediate_dataset_id=intermediate_dataset_id,
                dashboards_dataset_id=dashboards_dataset_id,
                observatory_dataset_id=observatory_dataset_id,
                elastic_dataset_id=elastic_dataset_id,
                transforms=dataset_transforms,
                start_date=start_date,
            ).make_dag()

            # Test that sensors do go into the 'up_for_reschedule' state as the DAGs that they wait for haven't run
            # execution_date = pendulum.datetime(year=2021, month=5, day=9)
            expected_state = "up_for_reschedule"
            with env.create_dag_run(doi_dag, start_date):
                for task_id in DoiWorkflow.SENSOR_DAG_IDS:
                    ti = env.run_task(f"{task_id}_sensor", doi_dag, execution_date=start_date)
                    self.assertEqual(expected_state, ti.state)

            # Run Dummy Dags
            execution_date = pendulum.datetime(year=2021, month=5, day=16)
            release_date = pendulum.datetime(year=2021, month=5, day=22)
            release_suffix = release_date.strftime("%Y%m%d")
            expected_state = "success"
            for dag_id in DoiWorkflow.SENSOR_DAG_IDS:
                dag = make_dummy_dag(dag_id, execution_date)
                with env.create_dag_run(dag, execution_date):
                    # Running all of a DAGs tasks sets the DAG to finished
                    ti = env.run_task("dummy_task", dag, execution_date=execution_date)
                    self.assertEqual(expected_state, ti.state)

            # Run end to end tests for DOI DAG
            with env.create_dag_run(doi_dag, execution_date):
                # Test that sensors go into 'success' state as the DAGs that they are waiting for have finished
                for task_id in DoiWorkflow.SENSOR_DAG_IDS:
                    ti = env.run_task(f"{task_id}_sensor", doi_dag, execution_date=execution_date)
                    self.assertEqual(expected_state, ti.state)

                # Check dependencies
                ti = env.run_task("check_dependencies", doi_dag, execution_date=execution_date)
                self.assertEqual(expected_state, ti.state)

                # Create datasets
                ti = env.run_task("create_datasets", doi_dag, execution_date=execution_date)
                self.assertEqual(expected_state, ti.state)

                # Generate fake dataset
                observatory_dataset = make_observatory_dataset(self.institutions)
                bq_load_observatory_dataset(
                    observatory_dataset,
                    env.download_bucket,
                    fake_dataset_id,
                    settings_dataset_id,
                    release_date,
                    self.gcp_data_location,
                )

                # Test that source dataset transformations run
                for transform in transforms:
                    task_id = f"create_{transform.output_table.table_id}"
                    ti = env.run_task(task_id, doi_dag, execution_date=execution_date)
                    self.assertEqual(expected_state, ti.state)

                # Test create DOI task
                ti = env.run_task("create_doi", doi_dag, execution_date=execution_date)
                self.assertEqual(expected_state, ti.state)

                # DOI assert table exists
                expected_table_id = f"{self.gcp_project_id}.{observatory_dataset_id}.doi{release_suffix}"
                expected_rows = len(observatory_dataset.papers)
                self.assert_table_integrity(expected_table_id, expected_rows=expected_rows)

                # DOI assert correctness of output
                expected_output = make_doi_table(observatory_dataset)
                actual_output = self.query_table(observatory_dataset_id, f"doi{release_suffix}", "doi")
                self.assert_doi(expected_output, actual_output)

                # Test create book
                ti = env.run_task("create_book", doi_dag, execution_date=execution_date)
                self.assertEqual(expected_state, ti.state)
                expected_table_id = f"{self.gcp_project_id}.{observatory_dataset_id}.book{release_suffix}"
                expected_rows = 0
                self.assert_table_integrity(expected_table_id, expected_rows)

                # Test aggregations tasks
                for agg in DoiWorkflow.AGGREGATIONS:
                    task_id = f"create_{agg.table_id}"
                    ti = env.run_task(task_id, doi_dag, execution_date=execution_date)
                    self.assertEqual(expected_state, ti.state)

                    # Aggregation assert table exists
                    expected_table_id = f"{self.gcp_project_id}.{observatory_dataset_id}.{agg.table_id}{release_suffix}"
                    self.assert_table_integrity(expected_table_id)

                # Assert country aggregation output
                expected_output = make_country_table(observatory_dataset)
                actual_output = self.query_table(observatory_dataset_id, f"country{release_suffix}", "id, time_period")
                self.assert_aggregate(expected_output, actual_output)
                # TODO: test correctness of remaining outputs

                # Test copy to dashboards
                ti = env.run_task("copy_to_dashboards", doi_dag, execution_date=execution_date)
                self.assertEqual(expected_state, ti.state)
                table_ids = [agg.table_id for agg in DoiWorkflow.AGGREGATIONS] + ["doi"]
                for table_id in table_ids:
                    self.assert_table_integrity(f"{self.gcp_project_id}.{dashboards_dataset_id}.{table_id}")

                # Test create dashboard views
                ti = env.run_task("create_dashboard_views", doi_dag, execution_date=execution_date)
                self.assertEqual(expected_state, ti.state)
                for table_id in ["country", "funder", "group", "institution", "publisher", "subregion"]:
                    self.assert_table_integrity(f"{self.gcp_project_id}.{dashboards_dataset_id}.{table_id}_comparison")

                # Test create exported tables for Elasticsearch
                for agg in DoiWorkflow.AGGREGATIONS:
                    table_id = agg.table_id
                    task_id = f"export_{table_id}"
                    ti = env.run_task(task_id, doi_dag, execution_date=execution_date)
                    self.assertEqual(expected_state, ti.state)

                    # Check that the correct tables exist for each aggregation
                    tables = make_elastic_tables(
                        table_id,
                        relate_to_institutions=agg.relate_to_institutions,
                        relate_to_countries=agg.relate_to_countries,
                        relate_to_groups=agg.relate_to_groups,
                        relate_to_members=agg.relate_to_members,
                        relate_to_journals=agg.relate_to_journals,
                        relate_to_funders=agg.relate_to_funders,
                        relate_to_publishers=agg.relate_to_publishers,
                    )
                    for table in tables:
                        aggregate = table["aggregate"]
                        facet = table["facet"]
                        expected_table_id = (
                            f"{self.gcp_project_id}.{elastic_dataset_id}.{aggregate}_{facet}{release_suffix}"
                        )
                        self.assert_table_integrity(expected_table_id)

    def query_table(self, observatory_dataset_id: str, table_id: str, order_by_field: str) -> List[Dict]:
        """ Query a BigQuery table, sorting the results and returning results as a list of dicts.

        :param observatory_dataset_id: the observatory dataset id.
        :param table_id: the table id.
        :param order_by_field: what field or fields to order by.
        :return: the table rows.
        """

        return [
            dict(row)
            for row in run_bigquery_query(
                f"SELECT * from {self.gcp_project_id}.{observatory_dataset_id}.{table_id} ORDER BY {order_by_field} ASC;"
            )
        ]

    def assert_aggregate(self, expected: List[Dict], actual: List[Dict]):
        """ Assert an aggregate table.

        :param expected: the expected rows.
        :param actual: the actual rows.
        :return: None.
        """

        # Check that expected and actual are same length
        self.assertEqual(len(expected), len(actual))

        # Check that each item matches
        for expected_item, actual_item in zip(expected, actual):
            # Check that top level fields match
            for key in [
                "id",
                "time_period",
                "name",
                "country",
                "country_code",
                "country_code_2",
                "region",
                "subregion",
                "coordinates",
                "total_outputs",
            ]:
                self.assertEqual(expected_item[key], actual_item[key])

            # Access types
            self.assert_sub_fields(
                expected_item,
                actual_item,
                "access_types",
                ["oa", "green", "gold", "gold_doaj", "hybrid", "bronze", "green_only"],
            )

    def assert_sub_fields(self, expected: Dict, actual: Dict, field: str, sub_fields: List[str]):
        """ Checks that the sub fields in the aggregate match.

        :param expected: the expected item.
        :param actual: the actual item.
        :param field: the field name.
        :param sub_fields: the sub field name.
        :return:
        """

        for key in sub_fields:
            self.assertEqual(expected[field][key], actual[field][key])

    def assert_doi(self, expected: List[Dict], actual: List[Dict]):
        """ Assert the DOI table.

        :param expected: the expected DOI table rows.
        :param actual: the actual DOI table rows.
        :return: None.
        """

        # Assert DOI output is correct
        self.assertEqual(len(expected), len(actual))
        for expected_record, actual_record in zip(expected, actual):
            # Check that DOIs match
            self.assertEqual(expected_record["doi"], actual_record["doi"])

            # Check events
            self.assert_doi_events(expected_record["events"], actual_record["events"])

            # Check grids
            self.assertSetEqual(set(expected_record["grids"]), set(actual_record["grids"]))

            # Check affiliations
            self.assert_doi_affiliations(expected_record["affiliations"], actual_record["affiliations"])

    def assert_doi_events(self, expected: Dict, actual: Dict):
        """ Assert the DOI table events field.

        :param expected: the expected events field.
        :param actual: the actual events field.
        :return: None
        """

        if expected is None:
            # When no events exist assert they are None
            self.assertIsNone(actual)
        else:
            # When events exist check that they are equal
            self.assertEqual(expected["doi"], actual["doi"])
            sort_events(actual["events"], actual["months"], actual["years"])

            event_keys = ["events", "months", "years"]
            for key in event_keys:
                self.assertEqual(len(expected[key]), len(actual[key]))
                for ee, ea in zip(expected[key], actual[key]):
                    self.assertDictEqual(ee, ea)

    def assert_doi_affiliations(self, expected: Dict, actual: Dict):
        """ Assert DOI affiliations.

        :param expected: the expected DOI affiliation rows.
        :param actual: the actual DOI affiliation rows.
        :return: None.
        """

        # DOI
        self.assertEqual(expected["doi"], actual["doi"])

        # Subfields
        fields = ["institutions", "countries", "subregions", "regions", "journals", "publishers", "funders"]
        for field in fields:
            self.assert_doi_affiliation(expected, actual, field)

    def assert_doi_affiliation(self, expected: Dict, actual: Dict, key: str):
        """ Assert a DOI affiliation row.

        :param expected: the expected DOI affiliation row.
        :param actual: the actual DOI affiliation row.
        :return: None.
        """

        items_expected_ = expected[key]
        items_actual_ = actual[key]
        self.assertEqual(len(items_expected_), len(items_actual_))
        items_actual_.sort(key=lambda x: x["identifier"])
        for item_ in items_actual_:
            item_["home_repo"].sort()
            item_["members"].sort()
        self.assertListEqual(items_expected_, items_actual_)
