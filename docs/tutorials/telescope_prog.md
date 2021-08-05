# Background

## Description of a telescope
The observatory platform collects data from many different sources. Each individual data source in the observatory is
 referred to as a telescope.  
The telescope can be seen as a workflow or data pipeline and should try to capture the data in it's original state as
 much as possible.  
The general workflow can be described with these tasks:
 * Extract the raw data from an external source
 * Store the raw data in a bucket
 * Transform the data, so it is ready to be loaded into the data warehouse
 * Store the transformed data in a bucket
 * Load the data into the data warehouse

## Managing telescopes with Airflow
The telescopes are all managed using Airflow. 
This workflow management system helps to schedule and monitor the many different telescopes.
Airflow works with DAG (Directed Acyclic Graph) objects that are defined in a Python script. 
The definition of a DAG according to Airflow is as follows:
 > A dag (directed acyclic graph) is a collection of tasks with directional dependencies. A dag also has a schedule, a start date and an end date (optional). For each schedule, (say daily or hourly), the DAG needs to run each individual tasks as their dependencies are met.

Generally speaking, one DAG maps to one telescope.

## The telescope template
Initially the telescopes in the observatory platform were each developed individually, there would be a telescope and
 release class that was unique for each telescope.  
After developing a few telescopes it became clear that there are many similarities between the telescopes
 and the classes that were developed. 
For example, many tasks such as uploading data to a bucket or loading data into BigQuery were the same for different
 telescopes and only variables like filenames and schemas would be different.  
The same properties were also often implemented, for example a download folder, release date and the many Airflow
 related properties such as the DAG id, schedule interval, start date etc.
 
These similarities prompted the development of a telescope template, that can be used as a basis for a new telescope.  
The template abstracts away the code to create the DAG object used in Airflow, making it possible to use the template
 without previous Airflow knowledge, although having basic Airflow knowledge might help to understand the
  possibilities and limitations of the template.
It also implements properties that are often used and common tasks such as cleaning up local files at the end of the
 telescope.  
The base template is used for two other templates that implement more specific tasks for loading data into
 BigQuery and have some properties set to specific values (such as whether previous DAG runs should be run using the
  airflow 'catchup' setting).  
The base template and the other two templates (snapshot and stream) are all explained in more detail below.
Each of the templates also have their own corresponding release class, this class contains properties and methods
 that are related to the specific release.  

## The template classes
### Telescope
The telescope class is the most basic template that can be used. 
It implements methods from the AbstractTelescope class and it is not recommended that the AbstractTelescope class is
 used directly itself.  

#### make_dag
The `make_dag` method of the telescope class is used to create an Airflow DAG object. This object is picked up by the
 Airflow scheduler and ensures that all tasks are scheduled.

#### Adding tasks to DAG
It is possible to add one of the three types of tasks to this DAG object:
 * Sensor
 * Set-up task
 * Task

All three types of tasks can be added individually per task using the `add_<type_of_task>` method or a list of tasks
 can be added using the `add_<type_of_task>_chain` method.  
To better understand the difference between these type of tasks, it is helpful to know how tasks are created in
 Airflow.  
Within a DAG, each task that is part of the DAG is created by instantiating an Operator class.   
There are many different types of Airflow Operators available and in the case of the template the usage is limited to
 the BaseSensorOperator, PythonOperator and the ShortCircuitOperator.  
The BaseSensorOperator keeps executing at a time interval and succeeds when a criteria is met and fails if and when
 they time out.   
The PythonOperator simply calls an executable Python function.  
The ShortCircuitOperator is derived from the PythonOperator and additionally evaluates a condition. When the
 conditions is False it short-circuits the workflow.  

The **sensor** instantiates the BaseSensorOperator (or a child class of this operator) and all sensor tasks are always
 chained to the beginning of the DAG.
This task is useful for example to probe whether another task has finished successfully using the ExternalTaskSensor.

The **set-up task** instantiates the ShortCircuitOperator, this means that the executable Python function has to
 return a boolean.
The returned value is then evaluated to determine whether the workflow continues. 
Additionally, the set-up task does not require a release instance as an argument passed to the Python function, in
 contrast to a 'general' task. 
The set-up tasks are chained after any sensors and before any remaining 'general' tasks. 
They are useful to e.g. check whether all dependencies for a telescope are met or to list which releases
 are available.

The general **task** instantiates the PythonOperator, the executable Python function that is called requires a release
 instance to be passed on as an argument.
These tasks are always chained after any sensors and set-up tasks.

By default all tasks within their type (sensor, setup task, task) are chained linearly in the order they are inserted.
There is a context manager `parallel_tasks` which can be used to parallelise some tasks.  
All tasks that are added within that context are added in parallel, as of now this can only be used with the setup
 tasks type.

#### Always implement 'make_release' method 
Because the general task requires a release instance, the `make_release` method of the telescope class always has to be
 implemented by the developer. 
This method is called when the PythonOperator for the general task is made and has to return a release instance, 
the release class on which this instance is based is discussed in detail further below.

#### check_dependencies
The telescope class also has a method `check_dependencies` that can be added as a set-up task. 
All telescopes require that at least some Airflow Variables and Connections are set, so these dependencies should be
 checked at the start of each telescope.

#### Example
Below is an example of a simple telescope using the telescope class:
```python
from airflow.operators.sensors import ExternalTaskSensor
from pendulum import Pendulum, datetime
from observatory.platform.telescopes.telescope import Telescope, Release
from observatory.platform.utils.airflow_utils import  AirflowConns, AirflowVars

class MyTelescope(Telescope):
    """
    Simple telescope DAG
    """
    def __init__(self, dag_id: str = 'my_telescope', start_date: Pendulum = datetime(2017, 3, 20),
                 schedule_interval: str = '@weekly', catchup: bool = False):
        """ Construct a Telescope instance.

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        """
        super().__init__(dag_id, start_date, schedule_interval, catchup=catchup, airflow_conns=[AirflowConns.ORCID],
                         airflow_vars=[AirflowVars.PROJECT_ID, AirflowVars.DATA_LOCATION])
        
        sensor = ExternalTaskSensor(external_dag_id='my_other_telescope', task_id='important_task', mode='reschedule')

        # add tasks to DAG object
        self.add_sensor(sensor)
        self.add_setup_task_chain([self.check_dependencies,
                                   self.list_releases], retries=3)
        self.add_task(self.download)

    def make_release(self, **kwargs) -> Release:
        """ Create a single release instance.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for more info.
        :return: A release instance.
        """
        release_id = f"telescope_{datetime.now()}"
        release = Release(self.dag_id, release_id)
        return release

    def list_releases(self, **kwargs) -> bool:
        """ List available releases. This is a custom task that is executed before a release is made. 
        The return value is used for a shortcircuit operator"

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for more info.
        :return: Whether at least one release is available.
        """
        release_available = True
        return release_available

    def download(self, release: Release, **kwargs):
        """ Task to download data. This is a custom task that can use a release instance.

        :param release: A release instance.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for more info.
        :return: None.
        """
        print('Downloading data')

# Create the DAG object that is loaded in the DAG bag by Airflow 
telescope = MyTelescope()
globals()[telescope.dag_id] = telescope.make_dag()
```

And the equivalent without using the template:
```python
from pendulum import datetime
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators.sensors import ExternalTaskSensor
from observatory.platform.utils.airflow_utils import  AirflowConns, AirflowVars, check_connections, check_variables

def check_dependencies() -> bool:
    """Checks the 'telescope' attributes, airflow variables & connections and possibly additional custom checks.
    
    :return: Whether variables and connections are available.
    """
    # check that vars and connections are available
    airflow_vars = [AirflowVars.PROJECT_ID, AirflowVars.DATA_LOCATION]
    vars_valid = check_variables(*airflow_vars)

    airflow_conns = [AirflowConns.ORCID]
    conns_valid = check_connections(*airflow_conns)
    
    if not vars_valid or not conns_valid:
        raise AirflowException("Required variables or connections are missing")
    
    return True

def list_releases() -> bool:
    """ List available releases.

    :return: Whether at least one release is available.
    """
    release_available = True
    return release_available
    

def download_data():
    """ Task to download data.

    :return: None.
    """
    print('Downloading data')

with DAG('my_telescope', description='Simple telescope DAG', start_date=datetime(2017, 3, 20), 
         schedule_interval='@weekly', catchup=False) as dag:
    sensor_task = ExternalTaskSensor(external_dag_id='my_other_telescope', task_id='important_task', mode='reschedule')

    check_dependencies_task = ShortCircuitOperator(task_id='check_dependencies', python_callable=check_dependencies, retries=3)

    list_releases_task = ShortCircuitOperator(task_id='list_releases', python_callable=list_releases, retries=3)
    
    download_task = PythonOperator(task_id='download', python_callable=download_data)

sensor_task >> check_dependencies_task >> list_releases_task >> download_task
```

### Release
An instance of the release class is passed on as an argument to any general tasks that are added to the telescope. 
Similarly in set-up to the telescope class, it implements methods from the AbstractRelease class and it is not
 recommended that the AbstractRelease class is used directly by itself.  

#### release_id
The Release class always needs a release id. 
This release id is usually based on the release date so it is unique for each release and relates to the date when
 the data became available or was processed.

#### Folder paths
The release has the paths for 3 different folders as properties `download_folder`, `extract_folder` and
 `transform_folder`, it is convenient to use these when downloading/extract/transforming data and writing the data
  to a file in the matching folder. 
The paths for these folders always include the release id. 
The format is as follows:  
`/path/to/telescopes/{download|extract|transform}/{dag_id}/{release_id}/`

The path to telescopes is determined by a separate function.  
Having these folder paths as properties of the release class makes it easy to have the same file structure for each
 telescope.

#### List files in folders
The folder paths are also used for the 3 corresponding properties, `download_files`, `extract_files` and
 `transform_files`.  
These properties will each return a list of files in their corresponding folder that match a given regex pattern.
This is useful when e.g. iterating through all download files to transform them, or passing on the list of transform
 files to a function that uploads all files to a storage bucket.   
The regex patterns for each of the 3 folders can be passed on separately when instantiating the release class.  

#### Bucket names
There are 2 storage buckets used to store the data processed with the telescope, a download bucket and a transform
 bucket.
The bucket names are retrieved from Airflow Variables and there are 2 corresponding properties in the release class, 
`download_bucket` and `transform_bucket`.  
These properties are convenient to use when uploading data to either one of these buckets.

#### Clean up
The release class has a `cleanup` method which can be called inside a task that will clean up by deleting all local
 files.  
This method is part of the release class, because it has to be done for each telescope and uses the folder paths
 described above.   

### SnapshotTelescope
The SnapshotTelescope is a subclass of the Telescope class.
This subclass can be used for 'snapshot' type telescopes.
A 'snapshot' telescope is defined by the fact that each release contains a complete snapshot of all data and is loaded
 into a BigQuery table shard.
The DAGs created with the snapshot telescope have catchup set to True by default, meaning that the DAG will catch up
 with any scheduled DAG runs in the past when the DAG is turned on, but setting catchup to False won't break the
  telescope.
Within each scheduled period, there might be multiple releases available.

The following methods are implemented and can all be added as general tasks:
 * upload_downloaded
 * upload_transformed
 * bq_load
 * cleanup

Examples of snapshot telescopes found in the Observatory Platform include:
 * Crossref Fundref
 * Crossref Metadata
 * Geonames
 * GRID
 * ONIX

### SnapshotRelease
The SnapshotRelease is used with the SnapshotTelescope.
The snapshot release always has a release date, and this date is used to create the release id.

### StreamTelescope
The StreamTelescope is another subclass of the Telescope class.
This subclass can be used for 'stream' type telescopes.
A 'stream' telescope is defined by the fact that there is one main table with data and this table is
 constantly kept up to date with a stream of data.
The telescope has a start and end date (rather than just a release date) and these are based on when the previous DAG
 run was started (start) and on the current run date (end).
The `get_release_info` method can be used to push these start and end dates as XCOMs.
These XCOMs can then be pulled in the `make_release` method that always has to be implemented and used to create
 the release instance.
 
Because there is one main table that is kept up to date, the first time the telescope runs is slightly different to any
 later runs.  
For the first release, all available data is downloaded and loaded into the BigQuery 'main' table from a file in the
 storage bucket using the `bq_append_new` method.
In this first run, the data is not loaded into a separate partition.

For any later releases, any new data since the last run as well as any updated/deleted data is loaded into a separate
 partition in the BigQuery 'partitions' table.
Then, there are 2 tasks to replace the old data (from the partitions) with the new, updated data in the main table.
These updates might not be done every DAG run, but instead the update frequency is determined by the stream
 telescope property `bq_merge_days`. 
The telescope keeps track of the number of days since the last merge, by checking when the relevant task had the last
 'success' state. 
 
When it is time to update the main table, a SQL merge query will find any rows in the main table that match the rows
 in the relevant table partitions and delete those matching rows from the main table.
This is done with the `bq_delete_old` method.
Next, all rows from the relevant table partitions are appended to the main table.
This is done with the `bq_append_new` method.
After these 2 tasks, any new rows are added to the main table and any old rows are updated in place.

As an example, let's assume there is a stream telescope with `bq_merge_days` set to 14 and the `schedule_interval` 
 set to `@weekly`.
Below is an overview of the expected states for each of the BigQuery load tasks for different run dates.

On 2021-01-01. First release:   
 * bq_load_partition - skipped
 * bq_delete_old - success (does not do anything, but set to success to keep track of days since last merge)
 * bq_append_new - success (loads data from file into main table)

On 2021-01-08. Later release, no merge yet:  
 * bq_load_partition - success (loads new/updated data into partition)
 * bq_delete_old - skipped
 * bq_append_new - skipped
 
On 2021-01-15. Later release and merge:
 * bq_load_partition - success (loads new/updated data into partition)
 * bq_delete_old - success (deletes matching data of 2 partitions (2021-01-08 and 2021-01-15) from main table)
 * bq_append_new - success (appends all data of 2 partitions (2021-01-08 and 2021-01-15) to main table)

The DAGs created with the stream telescope have catchup set to False by default, setting the catchup to True will
 break the functionality of updating the BigQuery tables as explained above.

The following methods are implemented and can all be added as general tasks:
 * get_release_info
 * upload_transformed
 * bq_load_partition
 * bq_delete_old
 * bq_append_new
 * cleanup
 
Examples of stream telescopes found in the Observatory Platform include:
 * Crossref Events
 * DOAB
 * OAPEN Metadata
 * ORCID

### StreamRelease
The StreamRelease is used with the StreamTelescope.
The stream release has the start date, end date and first_release properties.
The first_release property is a boolean and described whether this release is the first release, the start and end
 date are used to create the release id.

### OrganisationTelescope

### OrganisationRelease

# Step by step tutorial
## A typical development pipeline

A typical telescope pipeline will:
``` eval_rst
#. Create a DAG file that calls code to construct the telescope in observatory-dags/observatory/dags/dags 
#. Create a telescope file containing code for the telesecope itself in observatory-dags/observatory/dags/telescopes 
#. Create one or multiple schema files for the telescope data loaded into BigQuery in observatory-dags/observatory/dags/database/schema
#. Create a file with tests for the telescope in tests/observatory/dags/telescopes
#. Create documentation for the telescope in docs/telescopes and update the index.rst file
```

## Creating a DAG file

For Airflow to pickup new DAGs, it is required to create a DAG file with content similar to:

```python
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

# Author: <Your Name>

# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html

from observatory.dags.telescopes.my_telescope import MyTelescope

telescope = MyTelescope()
globals()[telescope.dag_id] = telescope.make_dag()
```

The filename is usually similar to the DAG id and the same for the telescope file in the `observatory-dags/observatory/dags/telescopes` 
directory.

## Creating a telescope file
The telescope file contains the release class at the top, then the telescope class and finally any functions that are
 used within these classes.

An example of the telescope file:
```python
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

# Author: <Your Name>

from pendulum import Pendulum, datetime
from observatory.platform.telescopes.telescope import Telescope, Release
from observatory.platform.utils.airflow_utils import  AirflowConns, AirflowVars

class MyRelease(Release):
    def __init__(self, dag_id: str, release_date: Pendulum):
        """ Create a MyRelease instance.
    
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

    def __init__(self, dag_id: str = 'my_telescope', start_date: Pendulum = datetime(2017, 3, 20),
                 schedule_interval: str = '@weekly', catchup: bool = False):
        """ Construct a MyTelescope instance.

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        """
        super().__init__(dag_id, start_date, schedule_interval, catchup=catchup, airflow_conns=[AirflowConns.ORCID],
                         airflow_vars=[AirflowVars.PROJECT_ID, AirflowVars.DATA_LOCATION])
        
        self.add_setup_task(self.check_dependencies, retries=3)
        self.add_task(self.download)
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> MyRelease:
        """ Create a release instance.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for more info.
        :return: A list with a single release instance.
        """
        release_date = kwargs["execution_date"]
        return MyRelease(self.dag_id, release_date)


    def download(self, release: MyRelease, **kwargs):
        """ Task to download data.

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

## BigQuery schemas

BigQuery database schema json files are put in `observatory-dags/dags/database/schema`.  
They follow the scheme: `<table_name>_YYYY-MM-DD.json`.  
If you wish to provide an additional custom version as well as the date, then the files should follow the scheme: 
 `<table_name>_<customversion>_YYYY-MM-DD.json`.

The BigQuery table loading utility functions in the Observatory Platform will try to find the correct schema to use
 for loading table data, based on release date information.
These utility functions are used by the BigQuery load tasks of the sub templates (Snapshot, Stream, Organisation) and
 to pick up the schema version when using these templates it is required to set the `schema_version` parameter.

## Generating a telescope template

You can use the observatory cli tool to generate a telescope template for a new `Telescope`, `StreamTelescope` or
 `SnapshotTelescope` telescope and release class. Use the command:
```shell script
observatory generate telescope <type> <name>
```
where the type can be `Telescope`, `StreamTelescope`, `SnapshotTelescope` and name is the class name of your new
 telescope.  
It will generate a new dag `.py` file in `observatory-dags/observatory/dags/dags` with the lower case class name. 
Similarly a telescope template will be created in `observatory-dags/observatory/dags/telescopes` with the same lower case class name.

For example:
```shell script
observatory generate telescope SnapshotTelescope MyNewTelescope
```

Creates the following files:
 * `observatory-dags/observatory/dags/dags/my_new_telescope.py`
 * `observatory-dags/observatory/dags/telescopes/my_new_telescope.py`
 * `tests/observatory/dags/telescopes/tests_my_new_telescope.py`
 * `docs/telescopes/mynewtelescope.md`

## Documentation

The Academic Observatory builds documentation using [Sphinx](https://www.sphinx-doc.org).  Documentation is contained in the `docs` directory. Currently index pages are written in [RST format (Restructured Text)](https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html), and content pages are written with [Markdown](https://www.sphinx-doc.org/en/master/usage/markdown.html) for simplicity.

You can generate documentation by using the command:
```
cd docs
make html
```
This will output html documentation in the `docs/_build/html` directory.

### Including schemas in documentation

The documentation build system automatically converts all the schema files from `observatory-dags/observatory/dags/database/schemas` into CSV files.  This is temporarily stored in the `docs/schemas` folder. The csv files have the same filename as the original schema files, except for the suffix, which is changed to csv.  The schemas folder is cleaned up as part of the build process so you will not be able to see the directory unless you disable the cleanup code in the `Makefile`.

To include a schema in your documentation markdown file, we need to embed some RST that loads a table from a csv file. Since we use the recommonmark package, this can be done with an `eval_rst` codeblock that contains RST:

    ``` eval_rst
    .. csv-table::
    :file: /path/to/schema.csv
    :width: 100%
    :header-rows: 1
    ```

To figure out the file path, it is recommended you construct a relative path to the `docs/schemas` directory from the directory of your markdown file. For example, if your documentation file resides in
```
docs/datasets/mydataset
```
then you should set
```
:file: ../../schemas/myschemafile.csv
```
The `..` follows the parent directory, and we need to do this twice to reach `docs` from `docs/datasets/mydataset`.

## Style

We try to conform to the Python PEP-8 standard, and the default format style of the `Black` formatter.  This is done with the [autopep8 package](https://pypi.org/project/autopep8), and the [black formatter](https://pypi.org/project/black/).

We recommend you use those format tools as part of your coding workflow.

### Type hinting

You should provide type hints for all of the function arguments you use, and for return types.  Because Python is a weakly typed language, it can be confusing to those unacquainted with the codebase what type of objects are being manipulated in a particular function.  Type hints help reduce this ambiguity.

### Docstring

Please provide docstring comments for all your classes, methods, and functions.  This includes descriptions of arguments, and returned objects.  These comments will be automatically compiled into the Academic Observatory API reference documentation section.