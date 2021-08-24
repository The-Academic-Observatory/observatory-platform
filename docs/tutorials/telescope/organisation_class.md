# OrganisationTelescope template
## OrganisationTelescope
```eval_rst
See :meth:`platform.telescopes.organisation_telescope.OrganisationTelescope` for the API reference.
```

The OrganisationTelescope is a subclass of the Telescope class.
This subclass can be used for 'organisation' type telescopes.  
An 'organisation' telescope is defined by the fact that it uses an Organisation which is provided by the Observatory
 API.
The Google Cloud project id is derived from the organisation, generally each organisation has it's own Google Cloud
 project and the data processed in this telescope is loaded into a BigQuery dataset inside that project.  
In contrast, for the other telescope templates, the project id is derived from an Airflow variable inside the BigQuery
 loading tasks and data is loaded into the same Google Cloud project.

Examples of snapshot telescopes found in the Observatory Platform include:
 * Google Analytics
 * Google Books
 * JSTOR
 * OAPEN IRUS-UK
 * UCL Discovery

### Implemented methods
The following methods are implemented and can all be added as general tasks:

### upload_downloaded
For each release, uploads all files listed with the `download_files` property to the download storage bucket.

### upload_transformed
For each release, uploads all files listed with the `transform_files` property to the transform storage bucket.

### bq_load_partition
For each release, loads each blob that is in the release directory of the transform bucket into a separate BigQuery
 table partition.  
The partition is based on the "release_date" field.
This means that to be able to use this task the data needs to have a "release_date" field with a corresponding value
 that is of a BigQuery DATE type.  
The partition type is set to 'monthly' by default, but could be changed.  
The table partition has the release date as a suffix in the table name and the format is dependent on the partition
 type, for the monthly partition type it would be e.g. `dataset_id.table_id$20200101`.

### cleanup
For each release, the local download, extract and transform directories are deleted including all files in those
 directories.

## OrganisationRelease
```eval_rst
See :meth:`platform.telescopes.organisation_telescope.OrganisationRelease` for the API reference.
```

The OrganisationRelease is used with the OrganisationTelescope.
The organisation release always has a release date, and this date is used to create the release id.  
Additionally, the release requires an Organisation instance.  
This organisation is used to determine the `download_bucket` and `transform_bucket` names.  
Each organisation generally has it's own Google Cloud project and a download and transform bucket inside this project
 to store data in.  
This means the release overwrites the methods for the `download_bucket` and `transform_bucket` of the base Release
 class.

## Example
Below is an example of a simple telescope using the OrganisationTelescope template.

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

import logging
from typing import Dict, List, Optional

import pendulum
from airflow.exceptions import AirflowException

from observatory.api.client.model.organisation import Organisation
from observatory.platform.workflows.organisation_telescope import OrganisationRelease, OrganisationTelescope
from observatory.platform.utils.airflow_utils import AirflowConns, AirflowVars
from observatory.platform.utils.telescope_utils import make_dag_id


class MyOrganisationRelease(OrganisationRelease):
    def __init__(self, dag_id: str, release_date: pendulum.DateTime, organisation: Organisation):
        """Construct a MyOrganisationRelease.

        :param dag_id: the id of the DAG.
        :param release_date: the date of the release.
        :param organisation: the Organisation of which data is processed.
        """
        super().__init__(dag_id=dag_id, release_date=release_date, organisation=organisation)


class MyOrganisation(OrganisationTelescope):
    """MyOrganisation Telescope."""

    DAG_ID_PREFIX = "my_organisation"

    def __init__(
        self,
        organisation: Organisation,
        extra1: str,
        dag_id: Optional[str] = None,
        start_date: pendulum.DateTime = pendulum.datetime(2020, 1, 1),
        schedule_interval: str = "@weekly",
        catchup: bool = True,
        dataset_id: str = "your_dataset_id",
        dataset_description: str = "The your_dataset dataset: https://dataseturl/",
        load_bigquery_table_kwargs: Dict = None,
        table_descriptions: Dict = None,
        schema_prefix: str = "",
        airflow_vars=None,
        airflow_conns=None,
    ):
        """Construct a MyOrganisationTelescope instance.

        :param organisation: the Organisation of which data is processed.
        :param extra1: the value for extra1, obtained from the 'extra' info from the API regarding the telescope.
        :param dag_id: the id of the DAG, by default this is automatically generated based on the DAG_ID_PREFIX and the
        organisation name.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param catchup: whether to catchup the DAG or not.
        :param dataset_id: the BigQuery dataset id.
        :param dataset_description: description for the BigQuery dataset.
        :param load_bigquery_table_kwargs: the customisation parameters for loading data into a BigQuery table.
        :param table_descriptions: a dictionary with table ids and corresponding table descriptions.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        :param airflow_conns: list of airflow connection keys, for each connection it is checked if it exists in airflow
        :param schema_prefix: the prefix used to find the schema path.
        """
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

        if dag_id is None:
            dag_id = make_dag_id(self.DAG_ID_PREFIX, organisation.name)

        super().__init__(
            organisation,
            dag_id,
            start_date,
            schedule_interval,
            dataset_id,
            load_bigquery_table_kwargs=load_bigquery_table_kwargs,
            dataset_description=dataset_description,
            table_descriptions=table_descriptions,
            catchup=catchup,
            schema_prefix=schema_prefix,
            airflow_vars=airflow_vars,
            airflow_conns=airflow_conns,
        )

        self.extra1 = extra1

        # Add sensor tasks
        # self.add_sensor(some_airflow_sensor)

        # Add setup tasks
        self.add_setup_task(self.check_dependencies)  # From OrganisationTelescope

        # Add ETL tasks
        self.add_task(self.task1)
        # self.add_task(self.upload_downloaded)  # From OrganisationTelescope
        # self.add_task(self.upload_transformed)  # From OrganisationTelescope
        # self.add_task(self.bq_load_partition)  # From OrganisationTelescope
        self.add_task(self.cleanup)  # From OrganisationTelescope

    def make_release(self, **kwargs) -> List[MyOrganisationRelease]:
        """Make release instances.

        :param kwargs: the context passed from the PythonOperator.
        :return: a list of MyOrganisationRelease instances.
        """
        release_date = kwargs["execution_date"]
        logging.info(f"Release date: {release_date}")
        releases = [MyOrganisationRelease(self.dag_id, release_date, self.organisation)]
        return releases

    def check_dependencies(self, **kwargs) -> bool:
        """Check dependencies of DAG. Add to parent method to additionally check for a expected extra key

        :return: True if dependencies are valid.
        """
        super().check_dependencies()

        if self.extra1 is None:
            expected_extra = {"extra1": "expected_value"}
            raise AirflowException(
                f"Value for extra1 is not set in 'extra' of telescope, " f"extra example: {expected_extra}"
            )
        return True

    def task1(self, releases: List[MyOrganisationRelease], **kwargs):
        """Add your own comments.

        :param releases: A list of MyOrganisationRelease instances
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

from observatory.api.client.identifiers import TelescopeTypes
from observatory.dags.workflows.my_organisation import MyOrganisation
from observatory.platform.utils.telescope_utils import make_observatory_api

# Fetch all telescopes
api = make_observatory_api()
telescope_type = api.get_telescope_type(type_id=TelescopeTypes.my_organisation)
telescopes = api.get_telescopes(telescope_type_id=telescope_type.id, limit=1000)

# Make all telescopes
for telescope in telescopes:
    key1 = telescope.extra.get("key1")
    telescope = MyOrganisation(telescope.organisation, key1)
    globals()[telescope.dag_id] = telescope.make_dag()
```