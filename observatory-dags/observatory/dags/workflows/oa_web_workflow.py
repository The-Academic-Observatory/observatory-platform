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


import json
import os
import os.path
import urllib.parse
from typing import Optional, List, Dict

import google.cloud.bigquery as bigquery
import pandas as pd
import pendulum

from observatory.platform.telescopes.snapshot_telescope import SnapshotRelease
from observatory.platform.telescopes.telescope import Telescope
from observatory.platform.utils.airflow_utils import AirflowVars, AirflowVariable
from observatory.platform.utils.clearbit_utils import clearbit_download_logo
from observatory.platform.utils.gc_utils import (
    select_table_shard_dates,
    bigquery_sharded_table_id,
    download_blobs_from_cloud_storage,
)
from observatory.platform.utils.url_utils import get_url_domain_suffix

# The minimum number of outputs before including an entity in the analysis
INCLUSION_THRESHOLD = 1000

# The query that pulls data to be included in the dashboards
QUERY = """
SELECT
  agg.id,
  agg.name,
  agg.time_period as year,
  (SELECT * from grid.links LIMIT 1) AS url,
  (SELECT * from grid.types LIMIT 1) AS type,
  DATE(agg.time_period, 12, 31) as date,
  agg.total_outputs as n_outputs,
  agg.access_types.oa.total_outputs AS n_outputs_oa,
  agg.access_types.gold.total_outputs AS n_outputs_gold,
  agg.access_types.green.total_outputs AS n_outputs_green,
  agg.access_types.hybrid.total_outputs AS n_outputs_hybrid,
  agg.access_types.bronze.total_outputs AS n_outputs_bronze
FROM
  `{project_id}.{agg_dataset_id}.{agg_table_id}` as agg 
  LEFT OUTER JOIN `{project_id}.{grid_dataset_id}.{grid_table_id}` as grid ON agg.id = grid.id
WHERE agg.time_period <= EXTRACT(YEAR FROM CURRENT_DATE())
ORDER BY year DESC, name ASC
"""

# Overrides for country names
NAME_OVERRIDES = {
    "Bolivia (Plurinational State of)": "Bolivia",
    "Bosnia and Herzegovina": "Bosnia",
    "Brunei Darussalam": "Brunei",
    "Congo": "Congo Republic",
    "Congo, Democratic Republic of the": "DR Congo",
    "Iran (Islamic Republic of)": "Iran",
    "Korea (Democratic People's Republic of)": "North Korea",
    "Korea, Republic of": "South Korea",
    "Lao People's Democratic Republic": "Laos",
    "Micronesia (Federated States of)": "Micronesia",
    "Moldova, Republic of": "Moldova",
    "Palestine, State of": "Palestine",
    "Saint Kitts and Nevis": "St Kitts & Nevis",
    "Saint Lucia": "St Lucia",
    "Saint Vincent and the Grenadines": "St Vincent",
    "Sint Maarten (Dutch part)": "Sint Maarten",
    "Svalbard and Jan Mayen": "Svalbard & Jan Mayen",
    "Syrian Arab Republic": "Syria",
    "Taiwan, Province of China": "Taiwan",
    "Tanzania, United Republic of": "Tanzania",
    "Trinidad and Tobago": "Trinidad & Tobago",
    "United Kingdom of Great Britain and Northern Ireland": "United Kingdom",
    "United States of America": "United States",
    "Venezuela (Bolivarian Republic of)": "Venezuela",
    "Viet Nam": "Vietnam",
    "Virgin Islands (British)": "Virgin Islands",
    "Antigua and Barbuda": "Antigua & Barbuda",
    "Russian Federation": "Russia",
}


def bq_query_to_gcs(*, query: str, project_id: str, destination_uri: str, location: str = "us"):
    client = bigquery.Client()

    # Run query
    query_job: bigquery.QueryJob = client.query(query, location=location)
    query_job.result()

    # Create and run extraction job
    source_table_id = f"{project_id}.{query_job.destination.dataset_id}.{query_job.destination.table_id}"
    extract_job_config = bigquery.ExtractJobConfig()
    extract_job_config.destination_format = bigquery.DestinationFormat.CSV
    extract_job: bigquery.ExtractJob = client.extract_table(
        source_table_id, destination_uri, job_config=extract_job_config, location=location
    )
    extract_job.result()

    return query_job.state == "DONE" and extract_job.state == "DONE"


def calc_percentages(df, keys: List[str]):
    for key in keys:
        df[f"p_{key}"] = round(df[f"n_{key}"] / df.n_outputs * 100, 0)


class OaWebRelease(SnapshotRelease):
    PERCENTAGE_FIELD_KEYS = ["outputs_oa", "outputs_gold", "outputs_green", "outputs_hybrid", "outputs_bronze"]

    def __init__(
        self,
        *,
        dag_id: str,
        project_id: str,
        release_date: pendulum.Pendulum,
        table_ids: List[str],
        change_chart_years: int = 10,
        agg_dataset_id: str = "observatory",
        grid_dataset_id: str = "digital_science",
    ):
        """

        :param dag_id:
        :param project_id:
        :param release_date:
        :param table_ids:
        :param change_chart_years:
        :param agg_dataset_id:
        :param grid_dataset_id:
        """

        super().__init__(dag_id=dag_id, release_date=release_date)
        self.project_id = project_id
        self.table_ids = table_ids
        self.change_chart_years = change_chart_years
        self.agg_dataset_id = agg_dataset_id
        self.grid_dataset_id = grid_dataset_id

    def query(self):
        """ Fetch the data for each table.

        :return: whether the queries ran successfully or not.
        """

        results = []
        for agg_table_id in self.table_ids:
            # Aggregate release dates
            agg_release_date = select_table_shard_dates(
                project_id=self.project_id,
                dataset_id=self.agg_dataset_id,
                table_id=agg_table_id,
                end_date=self.release_date,
            )[0]
            agg_sharded_table_id = bigquery_sharded_table_id(agg_table_id, agg_release_date)

            # GRID release date
            grid_table_id = "grid"
            grid_release_date = select_table_shard_dates(
                project_id=self.project_id,
                dataset_id=self.grid_dataset_id,
                table_id=grid_table_id,
                end_date=self.release_date,
            )[0]
            grid_sharded_table_id = bigquery_sharded_table_id(grid_table_id, grid_release_date)

            # Fetch data
            destination_uri = f"gs://{self.download_bucket}/{self.dag_id}/{self.release_id}/{agg_table_id}.csv"
            success = bq_query_to_gcs(
                query=QUERY.format(
                    project_id=self.project_id,
                    agg_dataset_id=self.agg_dataset_id,
                    agg_table_id=agg_sharded_table_id,
                    grid_dataset_id=self.grid_dataset_id,
                    grid_table_id=grid_sharded_table_id,
                ),
                project_id=self.project_id,
                destination_uri=destination_uri,
            )
            results.append(success)

        return all(results)

    def download(self):
        """ Download the queried data.

        :return: None.
        """

        prefix = f"{self.dag_id}/{self.release_id}"
        download_blobs_from_cloud_storage(
            bucket_name=self.download_bucket, prefix=prefix, destination_path=self.download_folder
        )

    def transform(self):
        """ Transform the queried data into the final format for the open access website.

        :return: None.
        """

        # Make required folders
        base_path = os.path.join(self.transform_folder, "data")
        os.makedirs(base_path, exist_ok=True)
        auto_complete = []
        for table_id in self.table_ids:
            csv_path = os.path.join(self.download_folder, f"{table_id}.csv")
            df = pd.read_csv(csv_path)
            df.fillna("", inplace=True)
            ts_data = self.build_timeseries_data(table_id, df)
            df_summary = self.build_summary_data(table_id, df, ts_data)
            auto_complete += self.build_auto_complete_data(table_id, df_summary)
            self.build_entity_summary_data(table_id, df_summary)

        # Save auto complete data as json
        output_path = os.path.join(base_path, "autocomplete.json")
        df_ac = pd.DataFrame(auto_complete)
        df_ac.to_json(output_path, orient="records")

    def build_entity_summary_data(self, table_id: str, df: pd.DataFrame):
        """

        :param table_id:
        :param df:
        :return:
        """

        base_path = os.path.join(self.transform_folder, "data", table_id)
        os.makedirs(base_path, exist_ok=True)
        df["category"] = table_id
        records = df.to_dict("records")
        for row in records:
            entity_id = row["id"]
            output_path = os.path.join(base_path, f"{entity_id}_summary.json")
            with open(output_path, mode="w") as f:
                json.dump(row, f, separators=(",", ":"))

    def build_auto_complete_data(self, table_id: str, df: pd.DataFrame):
        """

        :param table_id:
        :param df:
        :return:
        """

        records = []
        for i, row in df.iterrows():
            id = row["id"]
            name = row["name"]
            category = table_id
            logo = row["logo"]
            records.append({"id": id, "name": name, "category": category, "logo": logo})
        return records

    def build_summary_data(self, table_id: str, df: pd.DataFrame, ts_data: Dict):
        """

        :param table_id:
        :param df:
        :param ts_data:
        :return:
        """

        # Aggregate
        df_summary = df.groupby(["id"]).agg(
            {
                "id": "first",
                "name": "first",
                "url": "first",
                "type": "first",
                "n_outputs": "sum",
                "n_outputs_oa": "sum",
                "n_outputs_gold": "sum",
                "n_outputs_green": "sum",
                "n_outputs_hybrid": "sum",
                "n_outputs_bronze": "sum",
            },
            index=False,
        )

        # Exclude countries with small samples
        df_summary = df_summary[df_summary["n_outputs"] >= INCLUSION_THRESHOLD]

        # Add percentages to dataframe
        calc_percentages(df_summary, self.PERCENTAGE_FIELD_KEYS)

        # Sort from highest oa percentage to lowest
        df_summary.sort_values(by=["p_outputs_oa"], ascending=False, inplace=True)

        # Add ranks
        df_summary["rank"] = list(range(1, len(df_summary) + 1))

        # Add category
        df_summary["category"] = table_id

        # Name overrides
        df_summary["name"] = df_summary["name"].apply(
            lambda name: NAME_OVERRIDES[name] if name in NAME_OVERRIDES else name
        )

        # Clean URLs
        df_summary["friendly_url"] = df_summary["url"].apply(
            lambda u: get_url_domain_suffix(u) if not pd.isnull(u) else u
        )

        # If country add wikipedia url
        if table_id == "country":
            df_summary["url"] = df_summary["name"].apply(
                lambda name: f"https://en.wikipedia.org/wiki/{urllib.parse.quote(name)}"
            )

        # Integrate time series data
        change_points = []
        for i, row in df_summary.iterrows():
            entity_id = row["id"]
            change_points.append(ts_data[entity_id])
        df_summary["change_points"] = change_points

        # Make logos
        self.make_logos(table_id, df_summary)

        # Save subset
        base_path = os.path.join(self.transform_folder, "data", table_id)
        os.makedirs(base_path, exist_ok=True)
        summary_path = os.path.join(base_path, "summary.json")
        columns = [
            "id",
            "rank",
            "name",
            "category",
            "logo",
            "p_outputs_oa",
            "p_outputs_gold",
            "p_outputs_green",
            "n_outputs",
            "n_outputs_oa",
            "change_points",
        ]
        df_summary_subset = df_summary[columns]
        df_summary_subset.to_json(summary_path, orient="records")
        return df_summary

    def make_logos(self, table_id: str, df: pd.DataFrame, size=32, fmt="jpg"):
        """

        :param table_id:
        :param df:
        :param size:
        :param fmt:
        :return:
        """

        # Make logos
        if table_id == "country":
            df["logo"] = df["id"].apply(lambda country_code: f"/logos/{table_id}/{country_code}.svg")
        elif table_id == "institution":
            base_path = os.path.join(self.transform_folder, "logos", table_id)
            logo_path_unknown = f"/unknown.svg"
            os.makedirs(base_path, exist_ok=True)
            logos = []
            for i, row in df.iterrows():
                grid_id = row["id"]
                url = row["url"]
                logo_path = logo_path_unknown
                if not pd.isnull(url):
                    file_path = os.path.join(base_path, f"{grid_id}.{fmt}")
                    if not os.path.isfile(file_path):
                        clearbit_download_logo(company_url=url, file_path=file_path, size=size, fmt=fmt)

                    if os.path.isfile(file_path):
                        logo_path = f"/logos/{table_id}/{grid_id}.{fmt}"

                logos.append(logo_path)
            df["logo"] = logos

    def build_timeseries_data(self, table_id: str, df: pd.DataFrame):
        """

        :param table_id:
        :param df:
        :return:
        """

        ts_data = {}

        # Time series statistics for each entity
        base_path = os.path.join(self.transform_folder, "data", table_id)
        os.makedirs(base_path, exist_ok=True)
        base_change_charts_path = os.path.join(self.transform_folder, "change-charts", table_id)
        os.makedirs(base_change_charts_path, exist_ok=True)
        ts_groups = df.groupby(["id"])
        for entity_id, df_group in ts_groups:
            # Exclude institutions with small num outputs
            total_outputs = df_group["n_outputs"].sum()
            if total_outputs >= INCLUSION_THRESHOLD:
                calc_percentages(df_group, self.PERCENTAGE_FIELD_KEYS)
                df_group = df_group.sort_values(by=["year"])
                df_group = df_group.loc[:, ~df_group.columns.str.contains("^Unnamed")]

                # Save to csv
                df_group = df_group[
                    [
                        "year",
                        "n_outputs",
                        "n_outputs_oa",
                        "p_outputs_oa",
                        "p_outputs_gold",
                        "p_outputs_green",
                        "p_outputs_hybrid",
                        "p_outputs_bronze",
                    ]
                ]
                ts_path = os.path.join(base_path, f"{entity_id}_ts.json")
                df_group.to_json(ts_path, orient="records")

                # Fill in data for years with missing points
                df_ts = df_group[["year", "p_outputs_oa"]]
                end = pendulum.now().year
                start = end - self.change_chart_years - 1
                df_ts = df_ts[(start <= df_ts["year"]) & (df_ts["year"] < end)]  # Filter

                df_ts.set_index("year", inplace=True)
                df_ts = df_ts.reindex(list(range(start, end)))
                df_ts = df_ts.sort_values(by=["year"])
                all_null = df_ts["p_outputs_oa"].isnull().values.any()
                if all_null:
                    df_ts["p_outputs_oa"] = df_ts["p_outputs_oa"].fillna(0)
                else:
                    df_ts.interpolate(method="linear", inplace=True)

                ts_data[entity_id] = make_change_points(df_ts["p_outputs_oa"].tolist())

        return ts_data


class OaWebWorkflow(Telescope):
    def __init__(
        self,
        *,
        dag_id: str = "oa_web_workflow",
        start_date: Optional[pendulum.Pendulum] = pendulum.Pendulum(2021, 5, 2),
        schedule_interval: Optional[str] = "@weekly",
        catchup: Optional[bool] = False,
        table_ids: List[str] = None,
        airflow_vars: List[str] = None,
    ):
        """

        :param dag_id:
        :param start_date:
        :param schedule_interval:
        :param catchup:
        :param table_ids:
        :param airflow_vars:
        """

        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.DATA_PATH,
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
            ]

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            catchup=catchup,
            airflow_vars=airflow_vars,
        )
        self.table_ids = table_ids
        if table_ids is None:
            self.table_ids = ["country", "institution"]

        self.add_setup_task(self.check_dependencies)
        self.add_task(self.query)
        self.add_task(self.download)
        self.add_task(self.transform)

    def make_release(self, **kwargs) -> OaWebRelease:
        """ Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: A list of grid release instances
        """

        project_id = AirflowVariable.get(AirflowVars.PROJECT_ID)
        release_date = kwargs["next_execution_date"].subtract(microseconds=1).date()

        return OaWebRelease(
            dag_id=self.dag_id, project_id=project_id, release_date=release_date, table_ids=self.table_ids,
        )

    def query(self, release: OaWebRelease, **kwargs):
        """

        :param release:
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return:
        """

        release.query()

    def download(self, release: OaWebRelease, **kwargs):
        """

        :param release:
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return:
        """

        release.download()

    def transform(self, release: OaWebRelease, **kwargs):
        """

        :param release:
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return:
        """

        release.transform()
