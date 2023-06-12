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

# Author: James Diprose, Aniek Roelofs

import datetime
import glob
import gzip
import io
import logging
import os
import re
from copy import deepcopy
from typing import Dict, Tuple, Union, List, Optional

import jsonlines
import pendulum
from deepdiff import DeepDiff
from google.api_core.exceptions import BadRequest, Conflict
from google.cloud import bigquery
from google.cloud.bigquery import LoadJob, LoadJobConfig, QueryJob, SourceFormat, CopyJobConfig, CopyJob
from google.cloud.bigquery import dataset
from google.cloud.bigquery.job import QueryJobConfig
from google.cloud.bigquery.table import Table
from google.cloud.exceptions import Conflict, NotFound
from natsort import natsorted

from observatory.platform.config import sql_templates_path
from observatory.platform.utils.jinja2_utils import (
    make_sql_jinja2_filename,
    render_template,
)

# BigQuery single query byte limit.
# Daily limit is set in Terraform
BIGQUERY_SINGLE_QUERY_BYTE_LIMIT = int(2 * 2**40)  # 2 TiB


def assert_table_id(table_id: str):
    """Assert that a BigQuery table_id contains three parts.

    :param table_id: the BigQuery fully qualified table identifier.
    :return: None.
    """

    n_parts = len(table_id.split("."))
    assert n_parts == 3, f"bq_table_id_parts: table_id={table_id} requires 3 parts but only has {n_parts}"


def compare_lists_of_dicts(expected: List[Dict], actual: List[Dict], primary_key: str) -> bool:
    """Compare two lists of dictionaries, using a primary_key as the basis for the top level comparisons.

    :param expected: the expected data.
    :param actual: the actual data.
    :param primary_key: the primary key.
    :return: whether the expected and actual match.
    """

    expected_dict = {item[primary_key]: item for item in expected}
    actual_dict = {item[primary_key]: item for item in actual}

    if set(expected_dict.keys()) != set(actual_dict.keys()):
        logging.error("Primary keys don't match:")
        logging.error(f"Only in expected: {set(expected_dict.keys()) - set(actual_dict.keys())}")
        logging.error(f"Only in actual: {set(actual_dict.keys()) - set(expected_dict.keys())}")
        return False

    all_matched = True
    for key in expected_dict:
        diff = DeepDiff(expected_dict[key], actual_dict[key], ignore_order=True)
        logging.info(f"primary_key: {key}")
        for diff_type, changes in diff.items():
            all_matched = False
            if diff_type == "values_changed":
                for key_path, change in changes.items():
                    logging.error(
                        f"(expected) != (actual) {key_path}: {change['old_value']} (expected) != (actual) {change['new_value']}"
                    )
            elif diff_type == "dictionary_item_added":
                for change in changes:
                    logging.error(f"dictionary_item_added: {change}")
            elif diff_type == "dictionary_item_removed":
                for change in changes:
                    logging.error(f"dictionary_item_removed: {change}")
            elif diff_type == "type_changes":
                for key_path, change in changes.items():
                    logging.error(
                        f"(expected) != (actual) {key_path}: {change['old_type']} (expected) != (actual) {change['new_type']}"
                    )

    return all_matched


def bq_table_id(project_id: str, dataset_id: str, table_id: str) -> str:
    """Convert project_id, dataset_id and table_id into a BigQuery fully qualified table identifier.

    :param project_id: the BigQuery project_id.
    :param dataset_id: the BigQuery dataset_id.
    :param table_id: the BigQuery table_id.
    :return: the fully qualified BigQuery table identifier.
    """

    return f"{project_id}.{dataset_id}.{table_id}"


def bq_sharded_table_id(
    project_id: str, dataset_id: str, table_name: str, date: Union[pendulum.Date, pendulum.DateTime]
) -> str:
    """Create a sharded table identifier for a BigQuery table.

    :param project_id: the BigQuery project_id.
    :param dataset_id: the BigQuery dataset_id.
    :param table_name: the name of the table, excluding any shard date.
    :param date: the date to append as a shard suffix. If a DateTime is passed, the time elements are ignored.
    :return: the table id.
    """

    return f"{project_id}.{dataset_id}.{table_name}{date.strftime('%Y%m%d')}"


def bq_table_id_parts(table_id: str) -> Tuple[str, str, str, str, Optional[pendulum.Date]]:
    """Convert a BigQuery fully qualified table identifier into its parts which consist of project_id, dataset_id,
    and table_id, table_name and shard_date.

    :param table_id: the fully qualified BigQuery table identifier.
    :return: project_id, dataset_id and table_id and the table_name and optional shard date.
    """

    assert_table_id(table_id)
    parts = table_id.split(".")
    project_id = parts[0]
    dataset_id = parts[1]
    table_id = parts[2]
    table_name, shard_date = bq_table_shard_info(table_id)

    return project_id, dataset_id, table_id, table_name, shard_date


def bq_table_name(table_id: str):
    """Remove the date from a table_id.

    :param table_id: the table_id including a shard date.
    :return: the table name.
    """

    # -8 is removing the date from the string.
    return table_id[:-8]


def bq_table_shard_info(table_id: str) -> Tuple[str, Optional[pendulum.Date]]:
    """Extract the table_id and the date from an index.

    :param table_id: the table id.
    :return: table_id and date.
    """

    results = re.search(r"\d{8}$", table_id)

    if results is None:
        return table_id, None

    return bq_table_name(table_id), pendulum.parse(results.group(0))


def bq_table_exists(table_id: str) -> bool:
    """Checks whether a BigQuery table exists or not.

    :param table_id: the fully qualified BigQuery table identifier
    :return: whether the table exists or not.
    """

    assert_table_id(table_id)
    client = bigquery.Client()
    table_exists = True

    try:
        client.get_table(table_id)
    except NotFound:
        table_exists = False

    return table_exists


def bq_select_table_shard_dates(
    *, table_id: str, end_date: Union[pendulum.DateTime, pendulum.Date], limit: int = 1
) -> List[pendulum.Date]:
    """Returns a list of table shard dates, sorted from the most recent to the oldest date. By default it returns
    the first result.

    :param table_id: the fully qualified BigQuery table identifier, excluding any shard date.
    :param end_date: the end date of the table suffixes to search for (most recent date).
    :param limit: the number of results to return.
    :return:
    """

    assert_table_id(table_id)
    template_path = os.path.join(sql_templates_path(), make_sql_jinja2_filename("select_table_shard_dates"))
    query = render_template(
        template_path,
        table_id=table_id,
        end_date=end_date.strftime("%Y-%m-%d"),
        limit=limit,
    )
    rows = bq_run_query(query)
    dates = []
    for row in rows:
        py_date = row["suffix"]
        date = pendulum.Date(py_date.year, py_date.month, py_date.day)
        dates.append(date)
    return dates


def bq_select_latest_table(
    *,
    table_id: str,
    end_date: Union[pendulum.DateTime, pendulum.Date],
    sharded: bool,
):
    """Select the latest fully qualified BigQuery table identifier.

    :param table_id: the fully qualified BigQuery table identifier, excluding a shard date.
    :param end_date: latest date considered.
    :param sharded: whether the table is sharded or not.
    """

    assert_table_id(table_id)
    if sharded:
        table_date = bq_select_table_shard_dates(
            table_id=table_id,
            end_date=end_date,
        )[0]
        table_id = f"{table_id}{table_date.strftime('%Y%m%d')}"

    return table_id


def bq_find_schema(
    *,
    path: str,
    table_name: str,
    release_date: Union[pendulum.DateTime, pendulum.Date] = None,
    prefix: str = "",
) -> Union[str, None]:

    """Finds a schema file on a given path, with a particular table name, optional release date and prefix.

    Depending on the input and available files in the directory, this function's return will change
    If no release date is specified, will attempt to find a schema without a date (schema.json)
    If a release date is specified, the schema with a date <= to the release date is used (schema_1970-01-01.json)
    When looking for a schema with a date, the schema with the most recent date prior to the release date is preferred

    Examples:
    Say there is a schema_folder containing the following files:
        - table_1900-01-01.json
        - table_2000-01-01.json
        - table.json
    find_schema(schema_folder, table) -> table.json
    find_schema(schema_folder, table, release_date=2020-01-01) -> table_2000-01-01.json
    find_schema(schema_folder, table, release_date=1980-01-01) -> table_1900-01-01.json
    find_schema(schema_folder, table_2) -> None

    Now if schema_folder's contents change to
        - table.json
    find_schema(schema_folder, table) -> table.json
    find_schema(schema_folder, table, release_date=2020-01-01) -> None
    find_schema(schema_folder, table, release_date=1980-01-01) -> None
    find_schema(schema_folder, table_2) -> None

    :param path: the path to search within.
    :param table_name: the name of the table, excluding any shard date.
    :param release_date: the release date of the table.
    :param prefix: an optional prefix.
    :return: the path to the schema or None if no schema was found.
    """

    logging.info(
        f"Looking for schema with search parameters: analysis_schema_path={path}, "
        f"prefix={prefix}, table_name={table_name}, release_date={release_date}, "
    )

    # Make search path for schemas
    # Schema format: "prefix_table_name_YYY-MM-DD.json"
    date_re = "_[0-9]{4}-[0-9]{2}-[0-9]{2}" if release_date else ""
    re_string = f"{prefix}{table_name}{date_re}.json"
    search_pattern = re.compile(re_string)

    # Find potential schemas with a glob search
    schema_paths = glob.glob(os.path.join(path, "*"))

    # Filter the paths with the regex pattern
    file_names = [os.path.basename(i) for i in schema_paths]
    filtered_names = list(filter(search_pattern.match, file_names))
    filtered_paths = [os.path.join(path, i) for i in filtered_names]

    # No schemas were found
    if len(filtered_paths) == 0:
        logging.error("No schemas were found")
        return None

    # No release date supplied
    if not release_date:
        return filtered_paths[0]

    # Sort schema paths naturally
    filtered_paths = natsorted(filtered_paths)

    # Get schemas with dates <= release date
    suffix_len = 5  # .json
    date_str_len = 10  # YYYY-MM-DD
    selected_paths = []
    for path in filtered_paths:
        file_name = os.path.basename(path)
        date_str_start = -(date_str_len + suffix_len)
        date_str_end = -suffix_len
        datestr = file_name[date_str_start:date_str_end]
        schema_date = pendulum.parse(datestr)

        if schema_date <= release_date:
            selected_paths.append(path)
        else:
            break

    # Return the schema with the most recent release date
    if len(selected_paths):
        return selected_paths[-1]

    # No schemas were found
    logging.error("No schema found.")
    return None


def bq_update_table_description(*, table_id: str, description: str):
    """Update a BigQuery table's description.

    :param table_id: the fully qualified BigQuery table identifier.
    :param description: the description.
    :return: None.
    """

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Set description on table
    table = bigquery.Table(table_id)
    table.description = description

    # Update the table in BigQuery.
    client.update_table(table, ["description"])


def bq_load_table(
    *,
    uri: str,
    table_id: str,
    schema_file_path: str,
    source_format: str,
    csv_field_delimiter: str = ",",
    csv_quote_character: str = '"',
    csv_allow_quoted_newlines: bool = False,
    csv_skip_leading_rows: int = 0,
    partition: bool = False,
    partition_field: Union[None, str] = None,
    partition_type: bigquery.TimePartitioningType = bigquery.TimePartitioningType.DAY,
    require_partition_filter=False,
    write_disposition: str = bigquery.WriteDisposition.WRITE_TRUNCATE,  # TODO: overwrite?
    table_description: str = "",
    cluster: bool = False,
    clustering_fields=None,
    ignore_unknown_values: bool = False,
) -> bool:
    """Load a BigQuery table from an object on Google Cloud Storage.

    :param uri: the uri of the object to load from Google Cloud Storage into BigQuery.
    :param table_id: the fully qualified BigQuery table identifier.
    :param schema_file_path: path on local file system to BigQuery table schema.
    :param source_format: the format of the data to load into BigQuery.
    :param csv_field_delimiter: the field delimiter character for data in CSV format.
    :param csv_quote_character: the quote character for data in CSV format.
    :param csv_allow_quoted_newlines: whether to allow quoted newlines for data in CSV format.
    :param csv_skip_leading_rows: the number of leading rows to skip for data in CSV format.
    :param partition: whether to partition the table.
    :param partition_field: the name of the partition field.
    :param partition_type: the type of partitioning.
    :param require_partition_filter: whether the partition filter is required or not when querying the table.
    :param write_disposition: whether to append, overwrite or throw an error when data already exists in the table.
    :param table_description: the description of the table.
    :param cluster: whether to cluster the table or not.
    :param clustering_fields: what fields to cluster on.
    Default is to overwrite.
    :param ignore_unknown_values: whether to ignore unknown values or not.
    :return:
    """

    func_name = bq_load_table.__name__
    msg = f"uri={uri}, table_id={table_id}, schema_file_path={schema_file_path}, source_format={source_format}"
    logging.info(f"{func_name}: load bigquery table {msg}")

    assert uri.startswith("gs://"), "load_big_query_table: 'uri' must begin with 'gs://'"
    assert_table_id(table_id)

    # Handle mutable default arguments
    if clustering_fields is None:
        clustering_fields = []

    # Create load job
    client = bigquery.Client()
    job_config = LoadJobConfig()

    # Set global options
    job_config.source_format = source_format
    job_config.schema = client.schema_from_json(schema_file_path)
    job_config.write_disposition = write_disposition
    job_config.destination_table_description = table_description
    job_config.ignore_unknown_values = ignore_unknown_values

    # Set CSV options
    if source_format == SourceFormat.CSV:
        job_config.field_delimiter = csv_field_delimiter
        job_config.quote_character = csv_quote_character
        job_config.allow_quoted_newlines = csv_allow_quoted_newlines
        job_config.skip_leading_rows = csv_skip_leading_rows

    # Set partitioning settings
    if partition:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=partition_type, field=partition_field, require_partition_filter=require_partition_filter
        )
    # Set clustering settings
    if cluster:
        job_config.clustering_fields = clustering_fields

    load_job = None
    try:
        load_job: [LoadJob, None] = client.load_table_from_uri(uri, table_id, job_config=job_config)

        result = load_job.result()
        state = result.state == "DONE"

        logging.info(f"{func_name}: load bigquery table result.state={result.state}, {msg}")
    except BadRequest as e:
        logging.error(f"{func_name}: load bigquery table failed: {e}.")
        if load_job:
            logging.error(f"Error collection:\n{load_job.errors}")
        state = False

    return state


def bq_load_from_memory(table_id: str, records: List[Dict]) -> bool:
    """Load data into BigQuery from memory.

    :param table_id: the full table_id, including project id, dataset id and table name.
    :param records: the records to load.
    :return: whether the table loaded or not.
    """

    # Save as JSON Lines in memory
    with io.BytesIO() as bytes_io:
        with gzip.GzipFile(fileobj=bytes_io, mode="w") as gzip_file:
            with jsonlines.Writer(gzip_file) as writer:
                writer.write_all(records)

        # Load into BigQuery
        client = bigquery.Client()
        job_config = LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )

        try:
            load_job: LoadJob = client.load_table_from_file(bytes_io, table_id, job_config=job_config, rewind=True)
            success = load_job.result().state == "DONE"
        except BadRequest as e:
            logging.error(f"Load bigquery table {table_id} failed: {e}.")
            if load_job:
                logging.error(f"Errors:\n{load_job.errors}")
            success = False

    return success


def bq_query_bytes_estimate(query: str, *args, **kwargs) -> int:
    """Do a dry run of a BigQuery query to estimate the bytes processed.

    :param query: the query string.
    :param args: Positional arguments to pass onto the bigquery.Client().query function.
    :param kwargs: Named arguments to pass onto the bigquery.Client().query function.
    :return: Query bytes estimate.
    """

    if "job_config" not in kwargs:
        kwargs["job_config"] = QueryJobConfig()

    config = deepcopy(kwargs["job_config"])
    config.dry_run = True
    kwargs["job_config"] = config

    bytes_estimate = bigquery.Client().query(query, *args, **kwargs).total_bytes_processed
    return bytes_estimate


def bq_query_bytes_budget_check(*, bytes_budget: int, bytes_estimate: int):
    """Check that the estimated number of processed bytes required does not exceed the budgeted number of bytes for the
    query. If the estimate exceeds the budget, this function throws an exception.

    Call bq_query_bytes_estimate to calculate the estimate for a query.

    :param bytes_budget: The processed bytes budget for this query.
    :param bytes_estimate: Estimated number of bytes processed in query.
    """

    assert bytes_budget is not None and isinstance(
        bytes_budget, int
    ), f"bq_query_bytes_budget_check: bytes_budget is None or not an integer {bytes_budget}"
    assert bytes_estimate is not None and isinstance(
        bytes_estimate, int
    ), f"bq_query_bytes_budget_check: bytes_estimate is None or not an integer {bytes_estimate}"

    if bytes_estimate > bytes_budget:
        raise Exception(f"Bytes estimate {bytes_estimate} exceeds the budget {bytes_budget}.")


def bq_run_query(query: str, bytes_budget: int = BIGQUERY_SINGLE_QUERY_BYTE_LIMIT) -> list:
    """Run a BigQuery query.  Defaults to 1 TiB query budget.

    :param query: the query to run.
    :param bytes_budget: Maximum bytes allowed to be processed by the query.
    :return: the results.
    """

    bytes_estimate = bq_query_bytes_estimate(query)
    bq_query_bytes_budget_check(bytes_budget=bytes_budget, bytes_estimate=bytes_estimate)

    client = bigquery.Client()
    query_job = client.query(query)
    rows = query_job.result()
    success = query_job.errors is None  # throws error when query didn't work

    return list(rows)


def bq_copy_table(
    *,
    src_table_id: Union[str, list],
    dst_table_id: str,
    write_disposition: bigquery.WriteDisposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
) -> bool:
    """Copy a BigQuery table.

    :param src_table_id: the fully qualified BigQuery table identifier the source table.
    :param dst_table_id: the fully qualified BigQuery table identifier of the destination table.
    :param write_disposition: whether to append, overwrite or throw an error when data already exists in the table.
    :return: whether the table was copied successfully or not.
    """

    func_name = bq_copy_table.__name__
    msg = f"source_table_ids={src_table_id}, destination_table_id={dst_table_id}"
    logging.info(f"{func_name}: copying bigquery table {msg}")

    assert_table_id(src_table_id)
    assert_table_id(dst_table_id)

    client = bigquery.Client()
    job_config = bigquery.CopyJobConfig()

    job_config.write_disposition = write_disposition

    job = client.copy_table(src_table_id, dst_table_id, job_config=job_config)
    result = job.result()
    return result.done()


def bq_create_view(*, view_id: str, query: str, update_if_exists: bool = True) -> Table:
    """Create a BigQuery view.

    :param view_id: the fully qualified BigQuery table identifier for the view.
    :param query: the query for the view.
    :param update_if_exists: whether to update the view with the input query if it already exists
    :return: The bigquery table object of the view created/updated
    """

    assert_table_id(view_id)

    client = bigquery.Client()
    view = bigquery.Table(view_id)
    view.view_query = query
    try:
        view = client.create_table(view)
    except Conflict:
        if update_if_exists:
            view = client.update_table(view, ["view_query"])
        else:
            raise
    return view


def bq_create_table_from_query(
    *,
    sql: str,
    table_id: str,
    labels=None,
    query_parameters=None,
    clustering_fields=None,
    bytes_budget: int = BIGQUERY_SINGLE_QUERY_BYTE_LIMIT,
    schema_file_path: str = None,
) -> bool:
    """Create a BigQuery dataset from a provided query. Defaults to 0.5 TiB query budget.
    If a schema file path is given and the table does not exist yet, then an empty table will be created with this
    schema. Note: attempting to add data to a table with a schema will fail if the data does not match that schema.

    :param sql: the sql query to be executed
    :param table_id: the fully qualified BigQuery table identifier of the table to create.
    :param labels: labels to place on the new table
    :param query_parameters: parameters for a parametrised query.
    :param clustering_fields: what fields to cluster on.
    :param bytes_budget: Maximum bytes allowed to be processed by query.
    :param schema_file_path: path on local file system to BigQuery table schema.
    :return: whether successful or not.
    """

    assert_table_id(table_id)

    # Handle mutable default arguments
    if labels is None:
        labels = {}
    if query_parameters is None:
        query_parameters = []

    func_name = bq_create_table_from_query.__name__
    msg = f"table_id={table_id}, schema_file_path(optional)={schema_file_path}"
    logging.info(f"{func_name}: create bigquery table from query, {msg}")

    # Create empty table with schema. Delete the original table if it exists.
    client = bigquery.Client()
    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    table = bigquery.Table(table_id)
    if schema_file_path:
        # We delete the existing table instead of using WRITE_TRUNCATE (overwrite) if there's a schema supplied and the
        # table exists already. This is because BQ will ignore any existing schema when using WRITE_TRUNCATE
        try:
            client.get_table(table)
            client.delete_table(table, not_found_ok=False)
            logging.info(f"Deleted exising bigquery table: {table_id}")
        except NotFound:
            pass
        table = bq_create_empty_table(
            table_id=table_id,
            schema_file_path=schema_file_path,
            clustering_fields=clustering_fields,
        )
        write_disposition = bigquery.WriteDisposition.WRITE_EMPTY

    job_config = bigquery.QueryJobConfig(
        allow_large_results=True,
        destination=table,
        labels=labels,
        use_legacy_sql=False,
        query_parameters=query_parameters,
        write_disposition=write_disposition,
    )

    if clustering_fields:
        job_config.clustering_fields = clustering_fields

    bytes_estimate = bq_query_bytes_estimate(sql, job_config=job_config)
    bq_query_bytes_budget_check(bytes_budget=bytes_budget, bytes_estimate=bytes_estimate)

    query_job: QueryJob = client.query(sql, job_config=job_config)
    query_job.result()
    success = query_job.done()
    logging.info(f"{func_name}: create bigquery table from query {msg}: {success}")
    return success


def bq_create_dataset(*, project_id: str, dataset_id: str, location: str, description: str = "") -> bigquery.Dataset:
    """Create a BigQuery dataset.

    :param project_id: the Google Cloud project id.
    :param dataset_id: the BigQuery dataset id.
    :param location: the location where the dataset will be stored:
    https://cloud.google.com/compute/docs/regions-zones/#locations
    :param description: a description for the dataset
    :return: None
    """

    func_name = bq_create_dataset.__name__

    # Make the dataset reference
    dataset_ref = f"{project_id}.{dataset_id}"

    # Make dataset handle
    client = bigquery.Client()
    ds = bigquery.Dataset(dataset_ref)

    # Set properties
    ds.location = location
    ds.description = description

    # Create dataset, if already exists then catch exception
    try:
        logging.info(f"{func_name}: creating dataset dataset_ref={dataset_ref}")
        ds = client.create_dataset(ds)
    except Conflict as e:
        logging.warning(f"{func_name}: dataset already exists dataset_ref={dataset_ref}, exception={e}")
    return ds


def bq_create_empty_table(
    *,
    table_id: str,
    schema_file_path: str = None,
    clustering_fields: List = None,
):
    """Creates an empty BigQuery table. If a path to a schema file is given the table will be created using this
    schema.

    :param table_id: the fully qualified BigQuery table identifier of the table we will create.
    :param schema_file_path: path on local file system to BigQuery table schema.
    :param clustering_fields: what fields to cluster on.
    :return: The table instance if the request was successful.
    """

    func_name = bq_create_empty_table.__name__
    msg = f"table_id={table_id}, schema_file_path={schema_file_path}"
    logging.info(f"{func_name}: creating empty bigquery table {msg}")

    client = bigquery.Client()
    if schema_file_path:
        schema = client.schema_from_json(schema_file_path)
        table = bigquery.Table(table_id, schema=schema)
    else:
        table = bigquery.Table(table_id)

    # Note that clustering fields can only be set if a schema is specified
    if clustering_fields:
        table.clustering_fields = clustering_fields

    table = client.create_table(table)
    return table


def bq_list_tables(project_id: str, dataset_id: str) -> List[str]:
    """List all the tables within a BigQuery dataset.

    :param project_id: the Google Cloud project id.
    :param dataset_id: the BigQuery dataset id.
    :return: the fully qualified BigQuery table ids.
    """

    src_client = bigquery.Client()
    table_ids = []
    ds = bigquery.Dataset(f"{project_id}.{dataset_id}")

    tables = src_client.list_tables(ds, max_results=10000)
    for table in tables:
        table_id = str(table.reference)
        table_ids.append(table_id)

    return table_ids


def bq_export_table(*, table_id: str, file_type: str, destination_uri: str) -> bool:
    """Export a BigQuery table.

    :param table_id: the fully qualified BigQuery table identifier.
    :param file_type: the type of file to save the exported data as; csv or jsonl.
    :param destination_uri: the Google Cloud storage bucket destination URI.
    :return: whether the dataset was exported successfully or not.
    """

    assert_table_id(table_id)

    # Set destination format
    if file_type in {"csv", "csv.gz"}:
        destination_format = bigquery.DestinationFormat.CSV
    elif file_type in {"jsonl", "jsonl.gz"}:
        destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
    else:
        raise ValueError(f"export_bigquery_table: file type '{file_type}' is not supported")

    # Create and run extraction job
    client = bigquery.Client()
    extract_job_config = bigquery.ExtractJobConfig()

    # Set gz compression if file type ends in .gz
    if file_type.endswith(".gz"):
        extract_job_config.compression = bigquery.Compression.GZIP

    extract_job_config.destination_format = destination_format
    extract_job: bigquery.ExtractJob = client.extract_table(table_id, destination_uri, job_config=extract_job_config)
    extract_job.result()

    return extract_job.state == "DONE"


def bq_list_datasets_with_prefix(*, prefix: str = "") -> List[dataset.Dataset]:
    """List all BigQuery datasets with prefix.

    Due to multiple unit tests being run at once, need to include
    a try and except as test datasets can be deleted inbetween the time
    that it is listed and then that grabbed by the API.

    :param prefix: Prefix of datasets to list.
    :return: A list of dataset objects that are under the project.
    """

    client = bigquery.Client()
    datasets = list(client.list_datasets())
    dataset_list = []
    for dataset in datasets:
        if dataset.dataset_id.startswith(prefix):

            # Try to grab dataset object from the Google API.
            try:
                dataset_list.append(client.get_dataset(dataset.dataset_id))
            except NotFound:
                logging.info(
                    f"Dataset {dataset.dataset_id} was not found and added to list of datasets. It may have already been deleted."
                )
                pass

    return dataset_list


def bq_delete_old_datasets_with_prefix(*, prefix: str, age_to_delete: int):
    """Deletes datasets that share the same prefix and if it is older than "age_to_delete" hours.

    Due to multiple unit tests being run at once, need to include a try and except as
    test datasets could have been deleted by other unit tests inbetween the time that they were
    grabbed and then processed, resulting in a "not found" error.

    :param prefix: The identifying prefix of the datasets to delete.
    :param age_to_delete: Delete if the age of the bucket is older than this amount.
    """

    client = bigquery.Client()

    # List all datsets in the project with prefix
    dataset_list = bq_list_datasets_with_prefix(prefix=prefix)

    datasets_deleted = []
    for dataset in dataset_list:

        # Get age of the dataset.
        dataset_age = (datetime.datetime.now(datetime.timezone.utc) - dataset.created).total_seconds() / 3600.0

        # Delete dataset if older than specified age
        if dataset_age >= age_to_delete:

            # Try to delete the dataset - to get around the not found exception error if deleted previously.
            try:
                client.delete_dataset(dataset.dataset_id, delete_contents=True, not_found_ok=False)
                datasets_deleted.append(dataset.dataset_id)
            except NotFound:
                logging.info(
                    f"Dataset {dataset.dataset_id} was not found and removed. It may have already been deleted."
                )
                pass

    if len(datasets_deleted) < 1:
        logging.info(f"No datasets with prefix '{prefix}' older than {age_to_delete} hours to delete.")
    else:
        logging.info(
            f"Deleted the following datasets with prefix '{prefix}' older than {age_to_delete} hours: {datasets_deleted}"
        )


###################################################
# Functions used for loading incremental datasets
###################################################


def bq_snapshot(
    *,
    src_table_id: str,
    dst_table_id: str,
    expiry_date: pendulum.DateTime = None,
):
    """Create a BigQuery snapshot of a table.

    :param src_table_id: the BigQuery table name of the table to snapshot.
    :param dst_table_id: the date to give the snapshot table.
    :param expiry_date: the datetime for when the table should expire, e.g. datetime.datetime.now() + datetime.timedelta(minutes=60). If None then table will be permanent.
    :return: if the request was successful.
    """

    logging.info(
        f"bq_create_snapshot src_table_id={src_table_id}, dst_table_id={dst_table_id}, expiry_date={expiry_date}"
    )
    assert_table_id(src_table_id)
    assert_table_id(dst_table_id)

    client = bigquery.Client()
    job_config = CopyJobConfig(
        operation_type="SNAPSHOT", write_disposition="WRITE_EMPTY", destination_expiration_time=expiry_date.isoformat()
    )
    copy_job: CopyJob = client.copy_table(sources=src_table_id, destination=dst_table_id, job_config=job_config)
    copy_job.result()
    success = copy_job.done()

    logging.info(f"bq_create_snapshot: create bigquery snapshot: {success}")

    return success


def bq_select_columns(
    *,
    table_id: str,
    bytes_budget: Optional[int] = BIGQUERY_SINGLE_QUERY_BYTE_LIMIT,
) -> List[Dict]:
    """Select columns from a BigQuery table.

    :param table_id: the fully qualified BigQuery table identifier.
    :param bytes_budget: the BigQuery bytes budget.
    :return: the columns, which includes column_name and data_type.
    """

    assert_table_id(table_id)
    project_id, dataset_id, table_id, _, _ = bq_table_id_parts(table_id)
    template_path = os.path.join(sql_templates_path(), "select_columns.sql.jinja2")
    query = render_template(
        template_path,
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
    )
    rows = bq_run_query(query, bytes_budget=bytes_budget)
    return [dict(row) for row in rows]


def bq_upsert_records(
    *,
    main_table_id: str,
    upsert_table_id: str,
    primary_key: str,
    bytes_budget: Optional[int] = BIGQUERY_SINGLE_QUERY_BYTE_LIMIT,
):
    """Upserts records (updates and inserts) from an upsert_table into a main_table based on a primary_key.

    :param main_table_id: the fully qualified table identifier for the BigQuery main table that will be udpated.
    :param upsert_table_id: the fully qualified table identifier for the BigQuery table containing the upserts.
    :param primary_key: the primary key to use to determine which records to upsert.
    :param bytes_budget: the BigQuery bytes budget.
    :return: whether the upsert was successful or not.
    """

    assert_table_id(main_table_id)
    assert_table_id(upsert_table_id)

    # Fetch column names in main and upsert table which are used for the update part of the merge
    # and to check that the columns match
    main_columns = bq_select_columns(table_id=main_table_id)
    upsert_columns = bq_select_columns(table_id=upsert_table_id)

    # Assert that the column names and data types in main_table and upsert_table are the same
    columns_match = compare_lists_of_dicts(main_columns, upsert_columns, "column_name")
    assert columns_match, f"bq_upsert_records: columns in {main_table_id} do not match {upsert_table_id}"

    # Check that primary_key is in both tables
    # The data_type of primary_key must match because of the above assert
    main_column_names = [col["column_name"] for col in main_columns]
    upsert_column_names = [col["column_name"] for col in upsert_columns]
    assert (
        primary_key in main_column_names and primary_key in upsert_column_names
    ), f"bq_upsert_records: primary_key={primary_key} not in {main_table_id} or {upsert_table_id}"

    # Run query to upsert records
    template_path = os.path.join(sql_templates_path(), "upsert_records.sql.jinja2")
    query = render_template(
        template_path,
        upsert_table_id=upsert_table_id,
        main_table_id=main_table_id,
        primary_key=primary_key,
        columns=main_column_names,
    )
    bq_run_query(query, bytes_budget=bytes_budget)


def bq_delete_records(
    *,
    main_table_id: str,
    delete_table_id: str,
    main_table_primary_key: str,
    delete_table_primary_key: str,
    main_table_primary_key_prefix: str = "",
    delete_table_primary_key_prefix: str = "",
    bytes_budget: Optional[int] = BIGQUERY_SINGLE_QUERY_BYTE_LIMIT,
):
    """Deletes records from a main_table based on records in a delete_table.

    :param main_table_id: the fully qualified table identifier for the main BigQuery table where records will be deleted from.
    :param delete_table_id: the fully qualified table identifier for the BigQuery table containing the records to delete.
    :param main_table_primary_key: the primary key to use in the main table.
    :param delete_table_primary_key: the primary key to use in the delete table.
    :param main_table_primary_key_prefix: an optional prefix to add to the primary key main table cells.
    :param delete_table_primary_key_prefix: an optional prefix to add to the primary key delete table cells.
    :param bytes_budget: the bytes budget.
    :return:
    """

    assert_table_id(main_table_id)
    assert_table_id(delete_table_id)

    # Fetch column names in main and delete table to check if primary keys match
    main_column_index = {item["column_name"]: item["data_type"] for item in bq_select_columns(table_id=main_table_id)}
    delete_column_index = {
        item["column_name"]: item["data_type"] for item in bq_select_columns(table_id=delete_table_id)
    }

    # Check that primary_keys are in tables and that data types match
    assert (
        main_table_primary_key in main_column_index
    ), f"bq_delete_records: main_table_primary_key={main_table_primary_key} not in {main_table_id}"
    assert (
        delete_table_primary_key in delete_column_index
    ), f"bq_delete_records: delete_table_primary_key={delete_table_primary_key} not in {delete_table_id}"
    assert (
        main_column_index[main_table_primary_key] == delete_column_index[delete_table_primary_key]
    ), f"bq_delete_records: data types for main_table_primary_key ({main_column_index[main_table_primary_key]}) and delete_table_primary_key ({main_column_index[delete_table_primary_key]}) do not match"

    template_path = os.path.join(sql_templates_path(), "delete_records.sql.jinja2")
    query = render_template(
        template_path,
        delete_table_id=delete_table_id,
        main_table_id=main_table_id,
        main_table_primary_key=main_table_primary_key,
        delete_table_primary_key=delete_table_primary_key,
        main_table_primary_key_prefix=main_table_primary_key_prefix,
        delete_table_primary_key_prefix=delete_table_primary_key_prefix,
    )
    bq_run_query(query, bytes_budget=bytes_budget)
