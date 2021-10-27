# StreamTelescope template
## StreamTelescope
```eval_rst
See :meth:`platform.workflows.stream_telescope.StreamTelescope` for the API reference.
```

The StreamTelescope is another subclass of the Workflow class.
This subclass can be used for 'stream' type telescopes. 
A 'stream' telescope is defined by the fact that there is one main table with data and this table is
 constantly kept up to date with a stream of data. 
The telescope has a start and end date (rather than just a release date) and these are based on when the previous DAG
 run was started (start) and on the current run date (end). 
The `get_release_info` method can be used to retrieve these start and end dates.
These dates can then be used in the `make_release` method that always has to be implemented and used to create
 the release instance.
 
Because there is one main table that is kept up to date, the first time the telescope runs is slightly different to any
 later runs. 
For the first release, all available data is downloaded and loaded into the BigQuery 'main' table from a file in the
 storage bucket using the `bq_append_new` method. 
In this first run, the data is not loaded into a separate partition.

For later releases, any new data since the last run as well as any updated/deleted data is loaded into a separate
 partition in the BigQuery 'partitions' table. 
The partitions in this table are ingestion time based partitions, meaning that the partition date is based on when
 the data was loaded into BigQuery, instead of on one of the table fields.
The ingestion date is derived from the release end date, so that the partition date will always correspond to the
 release, even when multiple releases are ingested into BigQuery on the same day. 
Then, there are 2 tasks to replace the old data in the main table with the new, updated data from the partition.
 
The first task is called `bq_delete_old`. 
This task will use an SQL merge query to find any matching rows between the main table and the relevant table
 partition and will then delete those matching rows from the main table.  
The second task is called `bq_append_new`.
This task will copy all rows of the relevant partition into the main table.  
Any new rows are now added to the main table and any old rows have been updated in place.

As an example, let's assume there is a stream telescope with the `schedule_interval` set to `@weekly` and the
 following two DAG runs. 
Below is an overview of the expected states for each of the BigQuery load tasks for the different run dates.

On 2021-01-01. First run:   
 * bq_load_partition - skipped
 * bq_delete_old - skipped
 * bq_append_new - success (loads data from file into main table)

On 2021-01-08. Second run:  
 * bq_load_partition - success (loads new/updated data into partition)
 * bq_delete_old - success (use partition created in the previous step to delete data from main table)
 * bq_append_new - success (copy partition created in the previous step into the mmain table
 
The DAGs created with the stream telescope have catchup set to False by default.
Setting catchup to True will not break the tasks related to loading data into BigQuery, however the
 `get_release_info` task will have to be customised.
This is to prevent that the start and end dates are not the same when multiple releases are created by DAG runs executed
 on the same day.
See the Unpaywall telescope for an example of the Stream Telescope where catchup is set to True.
  
Examples of stream telescopes found in the Observatory Platform include:
 * Crossref Events
 * DOAB
 * OAPEN Metadata
 * ORCID
 * Unpaywall

### Implemented methods
The following methods are implemented and can all be added as general tasks:

### get_release_info
Gets release info from the DAG and DAG run info.
The release info includes the start date, end date and a boolean to describe whether this is the first release.

The start date is set to the start date of the telescope if it is the first release.
If it is a later release, the start date is set to the end date of the previous run plus 1 day, because the end
 date of the previous run is already processed in that run.

The end date is set to the current daytime minus 1 day, because some data might not be available on the same day of
 the release.

### upload_transformed
Uploads all files listed with the `transform_files` property of the release to the transform storage bucket.

### bq_load_partition
For the first release, this task will be skipped. 
For any later releases it loads each blob that is in the release directory of the transform bucket into a separate
 BigQuery table partition, where each partition is based on the ingestion time. 
This BigQuery table has the `_partitions` suffix.

### bq_delete_old
For the first release, this task will be skipped. 
For any later releases, it runs an SQL merge query which matches rows from one or more table partitions with rows in
 the main table, the matching is done based on the `merge_partition_field` property. 
When there is a matching row, this row will be deleted from the main table. 
All partitions that have been added since the last successful execution of this task will be processed. 
When adding this task, the task specific setting `trigger_rule` should be set to 'none_failed'. 
This ensures that any downstream tasks (such as clean up) will still be executed successfully.

### bq_append_new
For the first release, this task will load each blob that is in the release directory of the transform bucket into a
 separate main BigQuery table. 
For any later releases, it will load the partition created in `bq_load_partition` from the BigQuery partitions table
 into the main BigQuery table. 
When adding this task, the task specific setting `trigger_rule` should be set to 'none_failed'.  
This ensures that any downstream tasks (such as clean up) will still be executed successfully.  

### cleanup
The local download, extract and transform directories of the release are deleted including all files in those
 directories.
 
## StreamRelease
```eval_rst
See :meth:`platform.workflows.stream_telescope.StreamRelease` for the API reference.
```

The StreamRelease is used with the StreamTelescope. 
The stream release has the start date, end date and first_release properties. 
The first_release property is a boolean and described whether this release is the first release, the start and end
 date are used to create the release id.
 
 ## Example
Below is an example of a simple telescope using the StreamTelescope template.

Workflow file:  
```python
import pendulum
from airflow.models.taskinstance import TaskInstance
from typing import Dict, List

from observatory.dags.config import schema_folder as default_schema_folder
from observatory.platform.workflows.stream_telescope import StreamRelease, StreamTelescope
from observatory.platform.utils.airflow_utils import AirflowVars


class MyStreamRelease(StreamRelease):
    def __init__(self, dag_id: str, start_date: pendulum.DateTime, end_date: pendulum.DateTime, first_release: bool):
        """Construct a MyStreamRelease instance

        :param dag_id: the id of the DAG.
        :param start_date: the start_date of the release.
        :param end_date: the end_date of the release.
        :param first_release: whether this is the first release that is processed for this DAG
        """

        super().__init__(dag_id, start_date, end_date, first_release)


class MyStream(StreamTelescope):
    """ MyStream Telescope."""

    DAG_ID = "my_stream"

    def __init__(
        self,
        dag_id: str = DAG_ID,
        start_date: pendulum.DateTime = pendulum.datetime(2020, 1, 1),
        schedule_interval: str = "@weekly",
        dataset_id: str = "your_dataset_id",
        dataset_description: str = "The your_dataset_name dataset: https://dataseturl",
        merge_partition_field: str = "id",
        batch_load: bool = True,
        load_bigquery_table_kwargs: Dict = None,
        table_descriptions: Dict = None,
        schema_folder: str = default_schema_folder(),
        airflow_vars: List = None,
        airflow_conns: List = None,
    ):
        """Construct a MyStream telescope instance.

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the dataset id.
        :param dataset_description: the dataset description.
        :param merge_partition_field: the BigQuery field used to match partitions for a merge
        :param table_descriptions: a dictionary with table ids and corresponding table descriptions.
        :param schema_folder: the path to the SQL schema folder.
        :param batch_load: whether all files in the transform folder are loaded into 1 table at once
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        :param airflow_conns: list of airflow connection keys, for each connection it is checked if it exists in airflow
        """

        if table_descriptions is None:
            table_descriptions = {dag_id: "Table with up to date data."}

        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.DATA_PATH,
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
            ]

        # if airflow_conns is None:
        #     airflow_conns = [AirflowConns.SOMEDEFAULT_CONNECTION]

        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            dataset_id,
            merge_partition_field,
            schema_folder,
            batch_load=batch_load,
            load_bigquery_table_kwargs=load_bigquery_table_kwargs,
            dataset_description=dataset_description,
            table_descriptions=table_descriptions,
            airflow_vars=airflow_vars,
            airflow_conns=airflow_conns,
        )

        # Add sensor tasks
        # self.add_operator(some_airflow_sensor)

        # Add setup tasks
        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.get_release_info)

        # Add ETL tasks
        self.add_task(self.task1)  # User provided
        # self.add_task(self.upload_transformed)  # From StreamTelescope

        # BQ loading functions from StreamTelescope
        # self.add_task(self.bq_load_partition)
        # self.add_task_chain([self.bq_delete_old,
        #                     self.bq_append_new], trigger_rule="none_failed")

        # cleanup
        self.add_task(self.cleanup)  # From StreamTelescope

    def make_release(self, **kwargs) -> MyStreamRelease:
        """Make a Release instance

        :param kwargs: The context passed from the PythonOperator.
        :return: MyStreamRelease
        """
        ti: TaskInstance = kwargs["ti"]
        start_date, end_date, first_release = ti.xcom_pull(key=MyStream.RELEASE_INFO, include_prior_dates=True)

        start_date = pendulum.parse(start_date)
        end_date = pendulum.parse(end_date)

        release = MyStreamRelease(self.dag_id, start_date, end_date, first_release)
        return release

    def task1(self, release: MyStreamRelease, **kwargs):
        """Add your own comments.

        :param release: A MyStream instance
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        pass
```

DAG file:
```python
# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html

from my_dags.workflows.my_stream import MyStream

workflow = MyStream()
globals()[workflow.dag_id] = workflow.make_dag()
```