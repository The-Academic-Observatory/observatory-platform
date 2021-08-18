# Step by step tutorial

A typical telescope pipeline will:
1. Create a DAG file that calls code to construct the telescope in `observatory-dags/observatory/dags/dags`
2. Create a telescope file containing code for the telescope itself in `observatory-dags/observatory/dags/telescopes` 
3. Create one or multiple schema files for the telescope data loaded into BigQuery in `observatory-dags/observatory/dags/database/schema`
4. Create a file with tests for the telescope in `tests/observatory/dags/telescopes`
5. Create a documentation file about the telescope in `docs/telescopes` and update the `index.rst` file

## 1. Creating a DAG file
For Airflow to pickup new DAGs, it is required to create a DAG file that contains the DAG object as well as the keywords
 'airflow' and 'DAG'.  
Any code in this file is executed every time the file is loaded into the Airflow dagbag, which is once per minute by
 default.  
This means that the code in this file should be as minimal as possible, preferably limited to just creating the DAG
 object.  
The filename is usually similar to the DAG id and should be inside the `observatory-dags/observatory/dags/dags` directory.

An example of the DAG file:
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

# Author: <Your Name>

# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html

from observatory.dags.telescopes.my_telescope import MyTelescope

telescope = MyTelescope()
globals()[telescope.dag_id] = telescope.make_dag()
```

## 2. Creating a telescope file
The telescope file contains the release class at the top, then the telescope class and at the bottom any functions that
 are used within these classes.  
This filename is also usually similar to the DAG id and should be inside the `observatory-dags/observatory/dags/telescopes` 
 directory.  

An example of the telescope file:
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

# Author: <Your Name>

import pendulum

from observatory.platform.telescopes.telescope import Telescope, Release
from observatory.platform.utils.airflow_utils import AirflowConns, AirflowVars


class MyRelease(Release):
    def __init__(self, dag_id: str, release_date: pendulum.DateTime):
        """Create a MyRelease instance.

        :param dag_id: the DAG id.
        :param release_date: the date of the release.
        """

        download_files_regex = ".*.json.tar.gz$"
        extract_files_regex = f".*.json$"
        transform_files_regex = f".*.jsonl$"
        release_id = f'{dag_id}_{release_date.strftime("%Y_%m_%d")}'
        super().__init__(dag_id, release_id, download_files_regex, extract_files_regex, transform_files_regex)

        self.url = MyTelescope.URL.format(year=release_date.year, month=release_date.month)

    def download(self):
        success = download_from_url(self.url)


class MyTelescope(Telescope):
    """
    Simple telescope DAG
    """

    URL = "https://api.snapshot/{year}/{month:02d}/all.json.tar.gz"

    def __init__(
        self,
        dag_id: str = "my_telescope",
        start_date: pendulum.DateTime = pendulum.datetime(2017, 3, 20),
        schedule_interval: str = "@weekly",
        catchup: bool = False,
    ):
        """Construct a MyTelescope instance.

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        """
        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            catchup=catchup,
            airflow_conns=[AirflowConns.ORCID],
            airflow_vars=[AirflowVars.PROJECT_ID, AirflowVars.DATA_LOCATION],
        )

        self.add_setup_task(self.check_dependencies, retries=3)
        self.add_task(self.download)
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> MyRelease:
        """Create a release instance.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for more info.
        :return: A list with a single release instance.
        """
        release_date = kwargs["execution_date"]
        return MyRelease(self.dag_id, release_date)

    def download(self, release: MyRelease, **kwargs):
        """Task to download data.

        :param release: A release instance.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for more info.
        :return: None.
        """
        release.download()

    def cleanup(self, release: MyRelease, **kwargs):
        release.cleanup()


def download_from_url(url: str) -> bool:
    return True
```

### Using airflow Xcoms
Xcoms are an Airflow concept and are used with the telescopes to pass on information between tasks.
The description of Xcoms by Airflow can be read 
 [here](https://airflow.apache.org/docs/apache-airflow/stable/concepts/xcoms.html#xcoms) and is as follows:
 
>XComs (short for “cross-communications”) are a mechanism that let Tasks talk to each other, as by default Tasks are
 entirely isolated and may be running on entirely different machines.
An XCom is identified by a key (essentially its name), as well as the task_id and dag_id it came from. 
They can have any (serializable) value, but they are only designed for small amounts of data; do not use them to pass
 around large values, like dataframes.
XComs are explicitly “pushed” and “pulled” to/from their storage using the xcom_push and xcom_pull methods on Task
 Instances. 
Many operators will auto-push their results into an XCom key called return_value if the do_xcom_push argument is set
 to True (as it is by default), and @task functions do this as well.

For the telescopes they are commonly used to pass on release information.  
One task at the beginning of the telescope will retrieve release information such as the release date or possible a
 relevant release url.  
The release information is then pushed during this task using Xcoms and it is pulled in the subsequent tasks, so a
 release instance can be made with the given information.    
An example of this can be seen in the implemented method `get_release_info` of the StreamTelescope class.  

The `get_release_info` method:
```python
def get_release_info(self, **kwargs) -> bool:
    """Push the release info (start date, end date, first release) using Xcoms.

    :param kwargs: The context passed from the PythonOperator.
    :return: None.
    """
    ti: TaskInstance = kwargs["ti"]

    first_release = False
    release_info = ti.xcom_pull(key=self.RELEASE_INFO, include_prior_dates=True)
    if not release_info:
        first_release = True
        # set start date to the start of the DAG
        start_date = pendulum.instance(kwargs["dag"].default_args["start_date"]).start_of("day")
    else:
        # set start date to end date of previous DAG run, add 1 day, because end date was processed in prev run.
        start_date = pendulum.parse(release_info[1]) + timedelta(days=1)
    # set start date to current day, subtract 1 day, because data from same day might not be available yet.
    end_date = pendulum.today("UTC") - timedelta(days=1)
    logging.info(f"Start date: {start_date}, end date: {end_date}, first release: {first_release}")

    # Turn dates into strings.  Prefer JSON'able data over pickling in Airflow 2.
    start_date = start_date.format("YYYYMMDD")
    end_date = end_date.format("YYYYMMDD")

    ti.xcom_push(self.RELEASE_INFO, (start_date, end_date, first_release))
    return True
```

The start date, end date and first_release boolean are pushed using Xcoms with the `RELEASE_INFO` property as a key.
The info is then used within the `make_release` method.  

See for example the `make_release` method of the OrcidTelescope, which uses the StreamTelescope as a template.  
```python
def make_release(self, **kwargs) -> OrcidRelease:
    """Make a release instance. The release is passed as an argument to the function (TelescopeFunction) that is
    called in 'task_callable'.

    :param kwargs: the context passed from the PythonOperator. See
    https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
    passed to this argument.
    :return: an OrcidRelease instance.
    """
    ti: TaskInstance = kwargs["ti"]
    start_date, end_date, first_release = ti.xcom_pull(key=OrcidTelescope.RELEASE_INFO, include_prior_dates=True)

    release = OrcidRelease(
        self.dag_id, pendulum.parse(start_date), pendulum.parse(end_date), first_release, self.max_processes
    )
    return release
```

### Using Airflow variables and connections
Any information that should not be hardcoded inside the telescope, but is still required for the telescope to function
 can be passed on using Airflow variables and connections.   
Values for both the variables and connections are read from the relevant config file (`config.yaml` in local develop
 environment and `config-terraform.yaml` in deployed terraform environment).  
In the local develop environment, environment variables are created both for Airflow variables and connections, these
 environment variables are made up of the `AIRLFOW_VAR_` or `AIRFLOW_CONN_` prefix and the name that is used for the
  variable or connection in the config file.  
These prefixes are determined by Airflow and any environment variables with these prefixes will automatically be
 picked up, see the Airflow documentation for more info on managing [variables](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html#storing-variables-in-environment-variables)
 and [connections](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#storing-a-connection-in-environment-variables) 
 with environment variables.  
In the deployed terraform environment, the Google Cloud Secret Manager is used as a backend to store both Airflow
 variables and connections, because this is more secure than using environment variables.  
A secret is created for each individual Airflow variable or connection, see the Airflow documentation for more info
 on the [secrets backend](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html#secrets-backend). 

#### Variables
Airflow variables should never contain any sensitive information and are used for example for the project_id, bucket
 names or data location.  

#### Connections
Airflow connections can contain sensitive information and are often used to store credentials like API keys or
 usernames and passwords.  
In the local development environment, the Airflow connections are simply stored in the metastore database.  
There, the passwords inside the connection configurations are encrypted using Fernet.  
The value for the Airflow connection should always be a connection URI, see the [Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#generating-a-connection-uri)
 for more detailed information on how to construct this URI.
 
#### Using a new variable or connection
To use a new Airflow variable or connection, it has to be added to the relevant class in the airflow_utils file.  
This file can be found at:  
`observatory-platform/observatory/platform/utils/airflow_utils.py`

In there are the AirflowVars and AirflowConns classes, these classes make it easier to use the same key name for a
 variable or connection in many different DAGs.
The python variable name is used inside the telescope and the value is used inside the `config.yaml` or `config-terraform.yaml`
 file.
 
For example, to add the airflow variable 'new_variable' and connection 'new_connection', the relevant classes are
 updated like this:  
```python
# Inside observatory-platform/observatory/platform/utils/airflow_utils.py
class AirflowVars:
    """Common Airflow Variable names used with the Observatory Platform"""

    # add to existing variables
    NEW_VARIABLE = "new_variable"


class AirflowConns:
    """Common Airflow Connection names used with the Observatory Platform"""

    # add to existing connections
    NEW_CONNECTION = "new_connection"
```

The variable or connection can then be used inside the telescope like this:
```python
# Inside observatory-dags/observatory/dags/telescopes/my_telescope.py
from observatory.platform.utils.airflow_utils import AirflowVars, AirflowConns

airflow_conn = AirflowConns.NEW_CONNECTION
airflow_var = AirflowVars.NEW_VARIABLE
```

The relevant section of both the `config.yaml` and `config-terraform.yaml` files will look like this:
```yaml
# User defined Apache Airflow variables:
airflow_variables:
  new_variable: my-variable-value

# User defined Apache Airflow Connections:
airflow_connections:
  new_connection: http://my-username:my-password@
```

#### Secrets backend issue, use custom AirflowVariable class
In the cloud environment that is deployed with terraform, Airflow uses Google Cloud Secret Manager as a secrets
 backend and both the Airflow variable and connections are stored in there as secrets.
 
Unfortunately, there is currently an issue with Airflow's method to obtain variables when using the secrets backend.  
This issue has been resolved by implementing a custom `AirflowVariable` class inside 
`observatory-platform/observatory/platform/utils/airflow_utils.py`.  
This class should always be used to get variables instead of the standard Airflow `Variable` class inside `airflow
.models.variable.py`.

For example, to get a variable:  
```python
from observatory.platform.utils.airflow_utils import AirflowVariable, AirflowVars

variable = AirflowVariable.get(AirflowVars.DOWNLOAD_BUCKET)
```

The problem occurs when Airflow tries to get a variable while the Google Cloud Secrets Manager is set as a secrets
 backend, but the variable is not stored as a Google Cloud Secret.  
This is the case for some Airflow variables that are often used in the telescopes (test_data_path, data_path, 
download_bucket, transform_bucket).  
They are not set in the 'airflow_variables' section of the `config-terraform.yaml` file and this means that these
 variables are not stored as Google Cloud Secrets.
They do exist as valid Airflow variables, but as environment variables instead of Google Cloud Secrets.  
The search order for variables/connections for Airflow is not configurable and with a secrets backend enabled the
 order is:   
```
secrets backend > environment variables > metastore
```

The issue is that the current Airflow method to get variables from the secrets backend will return an error when a
 Google Cloud Secret can not be found.
This error prevents Airflow from searching for a variable in remaining places (environment variables & metastore),
 so the value for a variable can never be resolved if it does not exist as a Google Cloud Secret.   
The custom `AirflowVariable` class solves this by catching the error in a Try/Except clause, meaning that Airflow
 will continue to look for a variable in the remaining places.

## 3. Creating a BigQuery schema file
BigQuery database schema json files are put in `observatory-dags/dags/database/schema`.  
They follow the scheme: `<table_name>_YYYY-MM-DD.json`.  
To provide an additional custom version as well as the date, the files should follow the scheme:  
 `<table_name>_<customversion>_YYYY-MM-DD.json`.

The BigQuery table loading utility functions in the Observatory Platform will try to find the correct schema to use
 for loading table data, based on release date and version information.  
If no version is specified, the most recent schema with a date less than or equal to the release date of the data is
 returned.  
If a version string is specified, the most current (date) schema in that series is returned.  
The utility functions are used by the BigQuery load tasks of the sub templates (Snapshot, Stream, Organisation) and
 it is required to set the `schema_version` parameter to automatically pick up the schema version when using these
  templates.
 
## 4. Creating a test file
The Observatory Platform uses the `unittest` Python framework as a base and provides additional methods to run tasks
 and test DAG structure.
It also uses the Python `coverage` package to analyse test coverage.

To ensure that the telescope works as expected and in order to pick up any changes in the code base that would break the
 telescope it is required to add unit tests that cover the code in the developed telescope.  

The test files for telescopes are stored in `tests/observatory/dags/telescopes`.  
The `ObservatoryTestCase` class in the `test_utils.py` file contains common test methods and should be used as a
 parent class for the unit tests.  
Additionally, the `ObservatoryEnvironment` class in the `test_utils.py` can be used to simulate the Airflow
 environment and the different telescope tasks can be run and tested inside this environment.  

### Testing DAG structure
The telescope's DAG structure can be tested through the `assert_dag_structure` method of `ObservatoryTestCase`.  
The DAG object is compared against a dictionary, where the key is the source node, and the value is a list of sink
 nodes.  
This expresses the relationship that the source node task is a dependency of all of the sink node tasks.  

Example:
```python
import pendulum

from observatory.platform.utils.test_utils import ObservatoryTestCase
from observatory.platform.telescopes.telescope import Telescope, Release


class MyTelescope(Telescope):
    def __init__(
        self,
        dag_id: str = "my_telescope",
        start_date: pendulum.DateTime = pendulum.datetime(2017, 3, 20),
        schedule_interval: str = "@weekly",
    ):
        super().__init__(dag_id, start_date, schedule_interval)

        self.add_task(self.task1)
        self.add_task(self.task2)

    def make_release(self, **kwargs) -> Release:
        release_date = kwargs["execution_date"]
        return Release(self.dag_id, release_date)

    def task1(self, release, **kwargs):
        pass

    def task2(self, release, **kwargs):
        pass


class MyTestClass(ObservatoryTestCase):
    """Tests for the telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(MyTestClass, self).__init__(*args, **kwargs)

    def test_dag_structure(self):
        """Test that the DAG has the correct structure.

        :return: None
        """
        expected = {"task1": ["task2"], "task2": []}
        telescope = MyTelescope()
        dag = telescope.make_dag()
        self.assert_dag_structure(expected, dag)
```

### Testing DAG loading
To test if a DAG loads from a DagBag, the `assert_dag_load` method can be used within an `ObservatoryEnvironment`.  

Example:
```python
import os
import pendulum

from observatory.platform.utils.config_utils import module_file_path
from observatory.platform.utils.test_utils import ObservatoryTestCase, ObservatoryEnvironment
from observatory.platform.telescopes.telescope import Telescope, Release


class MyTelescope(Telescope):
    def __init__(
        self,
        dag_id: str = "my_telescope",
        start_date: pendulum.DateTime = pendulum.datetime(2017, 3, 20),
        schedule_interval: str = "@weekly",
    ):
        super().__init__(dag_id, start_date, schedule_interval)

        self.add_task(self.task1)
        self.add_task(self.task2)

    def make_release(self, **kwargs) -> Release:
        release_date = kwargs["execution_date"]
        return Release(self.dag_id, release_date)

    def task1(self, release, **kwargs):
        pass

    def task2(self, release, **kwargs):
        pass


class MyTestClass(ObservatoryTestCase):
    """Tests for the telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(MyTestClass, self).__init__(*args, **kwargs)

    def test_dag_load(self):
        """Test that the DAG can be loaded from a DAG bag.

        :return: None
        """
        with ObservatoryEnvironment().create():
            dag_file = os.path.join(module_file_path("observatory.dags.dags"), "my_telescope.py")
            self.assert_dag_load("my_telescope", dag_file)
```

### Testing telescope tasks
To run and test a telescope task, the `run_task` method can be used within an `ObservatoryEnvironment`.  

The ObservatoryEnvironment is used to simulate the Airflow environment, to ensure that a telescope can be run from
 end to end it creates additional resources such as storage buckets and BigQuery datasets.

Creating the Observatory Environment involves:  
* Creating a temporary local directory.
* Setting the OBSERVATORY_HOME environment variable.
* Initialising a temporary Airflow database.
* Creating download and transform Google Cloud Storage buckets.
* Creating BigQuery dataset(s).
* Creating default Airflow Variables: 
    * AirflowVars.DATA_PATH
    * AirflowVars.PROJECT_ID
    * AirflowVars.DATA_LOCATION
    * AirflowVars.DOWNLOAD_BUCKET
    * AirflowVars.TRANSFORM_BUCKET.
* Creating an ObservatoryApiEnvironment.
* Starting an Elastic environment.
* Cleaning up all resources when the environment is closed.

Note that if the unit test is stopped with a forced interrupt, the code block to clean up the created storage buckets
 and datasets will not be executed and those resources will have to be manually removed.  

The run dependencies that are imposed on each task by the DAG structure are preserved in the test environment.  
This means that to run a specific task, all the previous tasks in the DAG have to run successfully before that task
 within the same `create_dag_run` environment.
 
Example:
```python
import pendulum

from observatory.platform.utils.test_utils import ObservatoryTestCase, ObservatoryEnvironment
from observatory.platform.telescopes.telescope import Telescope, Release


class MyTelescope(Telescope):
    def __init__(
        self,
        dag_id: str = "my_telescope",
        start_date: pendulum.DateTime = pendulum.datetime(2017, 3, 20),
        schedule_interval: str = "@weekly",
    ):
        super().__init__(dag_id, start_date, schedule_interval)

        self.add_task(self.task1)
        self.add_task(self.task2)

    def make_release(self, **kwargs) -> Release:
        release_date = kwargs["execution_date"]
        return Release(self.dag_id, release_date)

    def task1(self, release, **kwargs):
        pass

    def task2(self, release, **kwargs):
        pass


class MyTestClass(ObservatoryTestCase):
    """Tests for the telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(MyTestClass, self).__init__(*args, **kwargs)
        self.execution_date = pendulum.datetime(2020, 1, 1)

    def test_telescope(self):
        """Test the telescope end to end.
        :return: None.
        """
        # Setup Observatory environment
        env = ObservatoryEnvironment()

        # Setup Telescope
        telescope = MyTelescope()
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create():
            with env.create_dag_run(dag, self.execution_date):
                # Run task1
                env.run_task(telescope.task1.__name__)
```

### Temporary GCP datasets
Unit testing frameworks often run tests in parallel, so there is no guarantee of execution order.  
When running code that modifies datasets or tables in the Google Cloud, it is recommended to create temporary
 datasets for each task to prevent any bugs caused by race conditions.  
The `ObservatoryEnvironment` has a method called `add_dataset` that can be used to create a new dataset in the linked
 project for the duration of the environment.

### Observatory Platform API
Some telescopes make use of the Observatory Platform API in order to fetch necessary metadata.  
When writing unit tests for telescopes that use the platform API, it is necessary to use an isolated API environment
 where the relevant TelescopeType, Organisations and Telescope exist.  
The ObservatoryEnvironment that is mentioned above can be used to achieve this.  
An API session is started when creating the ObservatoryEnvironment and the TelescopeType, Organisations and Telescope
 can all be added to this session.  
 
Example:
```python
import pendulum
from airflow.models.connection import Connection

from observatory.api.client.identifiers import TelescopeTypes
from observatory.api.server import orm
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.test_utils import ObservatoryEnvironment

dt = pendulum.now("UTC")

# Create observatory environment
env = ObservatoryEnvironment()

# Add the Observatory API connection, used from make_observatory_api() in DAG file
conn = Connection(conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:password@host:port")
env.add_connection(conn)


# Create telescope type with API
telescope_type = orm.TelescopeType(name="ONIX Telescope", type_id=TelescopeTypes.onix, created=dt, modified=dt)
env.api_session.add(telescope_type)

# Create organisation with API
organisation = orm.Organisation(name="Curtin Press", created=dt, modified=dt)
env.api_session.add(organisation)

# Create telescope with API
telescope = orm.Telescope(
    name="Curtin Press ONIX Telescope",
    telescope_type=telescope_type,
    organisation=organisation,
    modified=dt,
    created=dt,
)
env.api_session.add(telescope)

# Commit changes
env.api_session.commit()
```

## 5. Creating a documentation file
The Observatory Platform builds documentation using [Sphinx](https://www.sphinx-doc.org).  
Documentation is contained in the `docs` directory.  
Currently index pages are written in [RST format (Restructured Text)](https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html), 
 and content pages are written with [Markdown](https://www.sphinx-doc.org/en/master/usage/markdown.html) for simplicity.

It is possible to build the documentation by using the command:
```
cd docs
make html
```
This will output html documentation in the `docs/_build/html` directory and the file `docs_/build/index.html` can be
 opened in a browser to preview what the documentation will look like.
 
A documentation file with info on the telescope should be added in the `docs/telescopes` directory.  
This documentation should at least include:  
 * A short summary on the data source
 * A summary table, see example below 
 * Any details on set-up steps that are required to run this telescope
 * Info on any Airflow connections and variables that are used (see further below) 
 * The latest schema.
 
 Example of a summary table using `eval_rst` to format the RST table:
 
     ```eval_rst
    +------------------------------+---------+
    | Summary                      |         |
    +==============================+=========+
    | Average runtime              | 10 min  |
    +------------------------------+---------+
    | Average download size        | 500 MB  |
    +------------------------------+---------+
    | Harvest Type                 | API     |
    +------------------------------+---------+
    | Harvest Frequency            | Monthly |
    +------------------------------+---------+
    | Runs on remote worker        | True    |
    +------------------------------+---------+
    | Catchup missed runs          | True    |
    +------------------------------+---------+
    | Table Write Disposition      | Truncate|
    +------------------------------+---------+
    | Update Frequency             | Monthly |
    +------------------------------+---------+
    | Credentials Required         | No      |
    +------------------------------+---------+
    | Uses Telescope Template      | Snapshot|
    +------------------------------+---------+
    | Each shard includes all data | Yes     |
    +------------------------------+---------+
    ```

### Including Airflow variable/connection info in documentation
If a newly developed telescope uses an Airflow connection or variable, this should be explained in the documentation on
 the telescope.  
An example of the variable/connection is required as well as an explanation on how the value for this 
 variable/connection can be obtained.

See for example this info section on the Airflow connection required with the google_books telescope:

---
## Airflow connections
Note that all values need to be urlencoded.  
In the config.yaml file, the following airflow connection is required:  

### sftp_service
```yaml
sftp_service: ssh://<username>:<password>@<host>?host_key=<host_key>
```
The sftp_service airflow connection is used to connect to the sftp_service and download the reports.  
The username and password are created by the sftp service and the host is e.g. `oaebu.exavault.com`.  
The host key is optional, you can get it by running ssh-keyscan, e.g.:  
```
ssh-keyscan oaebu.exavault.com
```
---

### Including schemas in documentation
The documentation build system automatically converts all the schema files from `observatory-dags/observatory/dags/database/schemas` 
 into CSV files.  
This is temporarily stored in the `docs/schemas` folder.  
The csv files have the same filename as the original schema files, except for the suffix, which is changed to csv.  
If there are multiple schemas for the same telescope, the `_latest` suffix can be used to always get the latest
 version of the schema.  
The schemas folder is cleaned up as part of the build process so this directory is not visible, but can be made
 visable by disabling the cleanup code in the `Makefile`.  

To include a schema in the documentation markdown file, it is necessary to embed some RST that loads a table from a
 csv file.  
Since the recommonmark package is used, this can be done with an `eval_rst` codeblock that contains RST:

    ``` eval_rst
    .. csv-table::
    :file: /path/to/schema_latest.csv
    :width: 100%
    :header-rows: 1
    ```

To determine the correct file path, it is recommended to construct a relative path to the `docs/schemas` directory
 from the directory of the markdown file.  
 
For example, if the markdown file resides in  
`docs/telescopes/my_telescope.md`

And the schema file path is  
`observatory-dags/observatory/dags/database/schema/my_telescpe_2021-01-01.json`

then the correct file path that should be used in the RST code block is  
```
:file: ../schemas/my_telescope_latest.csv
```
The `..` follows the parent directory, this is needed once to reach `docs` from `docs/telescopes/my_telescope.md`.