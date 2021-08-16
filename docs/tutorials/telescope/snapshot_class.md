# SnapshotTelescope template
## SnapshotTelescope
 ```eval_rst
See :meth:`platform.telescopes.snapshot_telescope.SnapshotTelescope` for the API reference.
```

The SnapshotTelescope is a subclass of the Telescope class.
This subclass can be used for 'snapshot' type telescopes.  
A 'snapshot' telescope is defined by the fact that each release contains a complete snapshot of all data and is loaded
 into a BigQuery table shard.  
The DAGs created with the snapshot telescope have catchup set to True by default, meaning that the DAG will catch up
 with any scheduled DAG runs in the past when the DAG is turned on, but setting catchup to False won't break the
  telescope.  
Within each scheduled period, there might be multiple releases available.  

Examples of snapshot telescopes found in the Observatory Platform include:
 * Crossref Fundref
 * Crossref Metadata
 * Geonames
 * GRID
 * ONIX

### Implemented methods
The following methods are implemented and can all be added as general tasks:

#### upload_downloaded
For each release, uploads all files listed with the `download_files` property to the download storage bucket.

#### upload_transformed
For each release, uploads all files listed with the `transform_files` property to the transform storage bucket.

#### bq_load
For each release, loads each blob that is in the release directory of the transform bucket into a separate BigQuery
 table shard.
The table shard has the release date as a suffix in the table name, e.g. `dataset_id.table_id20200101`

#### cleanup
For each release, the local download, extract and transform directories are deleted including all files in those
 directories.

## SnapshotRelease
 ```eval_rst
See :meth:`platform.telescopes.snapshot_telescope.SnapshotRelease` for the API reference.
```

The SnapshotRelease is used with the SnapshotTelescope.  
The snapshot release always has a release date, and this date is used to create the release id.

## Example
Below is an example of a simple telescope using the SnapshotTelescope template.

Telescope file:  
```python
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

# Author: Aniek Roelofs

import pendulum
from typing import List, Dict

from observatory.platform.telescopes.snapshot_telescope import SnapshotRelease, SnapshotTelescope
from observatory.platform.utils.airflow_utils import AirflowConns, AirflowVars


class MySnapshotRelease(SnapshotRelease):
    def __init__(self, dag_id: str, release_date: pendulum.DateTime):
        """Create a MySnapshotRelease instance.

        :param dag_id: the DAG id.
        :param release_date: the date of the release.
        """

        super().__init__(dag_id, release_date)


class MySnapshot(SnapshotTelescope):
    """MySnapshot Telescope."""

    DAG_ID = "my_snapshot"

    def __init__(
        self,
        dag_id: str = DAG_ID,
        start_date: pendulum.DateTime = pendulum.datetime(2020, 1, 1),
        schedule_interval: str = "@weekly",
        dataset_id: str = "your_dataset_id",
        dataset_description: str = "The your_dataset dataset: https://dataseturl/",
        load_bigquery_table_kwargs: Dict = None,
        table_descriptions: Dict = None,
        airflow_vars: List = None,
        airflow_conns: List = None,
        max_active_runs: int = 1,
    ):

        """The MySnapshot telescope

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the BigQuery dataset id.
        :param dataset_description: description for the BigQuery dataset.
        :param load_bigquery_table_kwargs: the customisation parameters for loading data into a BigQuery table.
        :param table_descriptions: a dictionary with table ids and corresponding table descriptions.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow.
        :param airflow_conns: list of airflow connection keys, for each connection it is checked if it exists in airflow
        :param max_active_runs: the maximum number of DAG runs that can be run at once.
        """

        if table_descriptions is None:
            table_descriptions = {dag_id: "A single your_dataset_name snapshot."}

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
            load_bigquery_table_kwargs=load_bigquery_table_kwargs,
            dataset_description=dataset_description,
            table_descriptions=table_descriptions,
            airflow_vars=airflow_vars,
            airflow_conns=airflow_conns,
            max_active_runs=max_active_runs,
        )

        # Add sensor tasks
        # self.add_sensor(some_airflow_sensor)

        # Add setup tasks
        self.add_setup_task(self.check_dependencies)  # From SnapshotTelescope

        # Add ETL tasks
        self.add_task(self.task1)
        # self.add_task(self.upload_downloaded)  # From SnapshotTelescope
        # self.add_task(self.upload_transformed)  # From SnapshotTelescope
        # self.add_task(self.bq_load)  # From SnapshotTelescope
        self.add_task(self.cleanup)  # From SnapshotTelescope

    def make_release(self, **kwargs) -> List[MySnapshotRelease]:
        """Make release instances.

        :param kwargs: the context passed from the PythonOperator.
        :return: a list of MySnapshotRelease instances.
        """
        release_date = kwargs["execution_date"]
        return [MySnapshotRelease(self.dag_id, release_date)]

    def task1(self, releases: List[MySnapshotRelease], **kwargs):
        """Add your own comments.

        :param releases: A list of MySnapshotRelease instances
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        pass
```

DAG file:
```python
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

# Author: Aniek Roelofs

# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html

from observatory.dags.telescopes.my_snapshot import MySnapshot

telescope = MySnapshot()
globals()[telescope.dag_id] = telescope.make_dag()
```
