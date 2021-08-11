# Telescope template
## Telescope
```eval_rst
See :meth:`platform.telescopes.telescope.Telescope` for the API reference.
```

The telescope class is the most basic template that can be used. 
It implements methods from the AbstractTelescope class and it is not recommended that the AbstractTelescope class is
 used directly itself.  

### Make DAG
The `make_dag` method of the telescope class is used to create an Airflow DAG object. This object is picked up by the
 Airflow scheduler and ensures that all tasks are scheduled.
 
### Adding tasks to DAG
It is possible to add one of the three types of tasks to this DAG object:
 * Sensor
 * Set-up task
 * Task

All three types of tasks can be added individually per task using the `add_<type_of_task>` method or a list of tasks
 can be added using the `add_<type_of_task>_chain` method.  
To better understand the difference between these type of tasks, it is helpful to know how tasks are created in
 Airflow.  
Within a DAG, each task that is part of the DAG is created by instantiating an Operator class.   
There are many different types of Airflow Operators available, but in the case of the template the usage is limited to
 the BaseSensorOperator, PythonOperator and the ShortCircuitOperator.  
 
* The BaseSensorOperator keeps executing at a time interval and succeeds when a criteria is met and fails if and when
 they time out.   
* The PythonOperator simply calls an executable Python function.  
* The ShortCircuitOperator is derived from the PythonOperator and additionally evaluates a condition. When the
 conditions is False it short-circuits the workflow.  

The **sensor** instantiates the BaseSensorOperator (or a child class of this operator) and all sensor tasks are always
 chained to the beginning of the DAG.  
This task is useful for example to probe whether another task has finished successfully using the ExternalTaskSensor.

The **set-up task** instantiates the ShortCircuitOperator, this means that the executable Python function that is
 called with this operator has to return a boolean.  
The returned value is then evaluated to determine whether the workflow continues. 
Additionally, the set-up task does not require a release instance as an argument passed to the Python function, in
 contrast to a 'general' task.   
The set-up tasks are chained after any sensors and before any remaining 'general' tasks. 
They are useful to e.g. check whether all dependencies for a telescope are met or to list which releases
 are available.

The general **task** instantiates the PythonOperator and the executable Python function that is called with
 this operator requires a release instance to be passed on as an argument.
These tasks are always chained after any sensors and set-up tasks.

Order of the different task types within a telescope:  
<p align="center">
<img title="Order of telescope tasks" alt="Order of telescope tasks" src="../../graphics/telescope_flow.png">
</p>

By default all tasks within the same type (sensor, setup task, task) are chained linearly in the order they are
 inserted.  
There is a context manager `parallel_tasks` which can be used to parallelise tasks.  
All tasks that are added within that context are added in parallel, however as of now this can only be used with the
 setup tasks type.

### The 'make_release' method 
The general task requires a release instance and because of this the `make_release` method of the telescope class
 always has to be implemented by the developer.  
This method is called when the PythonOperator for the general task is made and has to return a release instance, the
 release class on which this instance is based is discussed in detail further below.  

### Checking dependencies
The telescope class also has a method `check_dependencies` implemented that can be added as a set-up task.  
All telescopes require that at least some Airflow Variables and Connections are set, so these dependencies should be
 checked at the start of each telescope and this can be done with this task.

## Release
```eval_rst
See :meth:`platform.telescopes.telescope.Release` for the API reference.
```

An instance of the release class is passed on as an argument to any general tasks that are added to the telescope.   
Similarly in set-up to the telescope class, it implements methods from the AbstractRelease class and it is not
 recommended that the AbstractRelease class is used directly by itself.  

### The release id
The Release class always needs a release id.  
This release id is usually based on the release date, so it is unique for each release and relates to the date when
 the data became available or was processed.

### Folder paths
The release has properties for the paths of 3 different folders:
 * `download_folder`
 * `extract_folder`
 * `transform_folder`
 
 It is convenient to use these when downloading/extract/transforming data and writing the data to a file in the
  matching folder.  
The paths for these folders always include the release id and the format is as follows:    
`/path/to/telescopes/{download|extract|transform}/{dag_id}/{release_id}/`

The `path/to/telescopes` is determined by a separate function.  
Having these folder paths as properties of the release class makes it easy to have the same file structure for each
 telescope.

### List files in folders
The folder paths are also used for the 3 corresponding properties:
 * `download_files`
 * `extract_files`
 * `transform_files`  
 
These properties will each return a list of files in their corresponding folder that match a given regex pattern.  
This is useful when e.g. iterating through all download files to transform them, or passing on the list of transform
 files to a function that uploads all files to a storage bucket.   
The regex patterns for each of the 3 folders is passed on separately when instantiating the release class.  

### Bucket names
There are 2 storage buckets used to store the data processed with the telescope, a download bucket and a transform
 bucket.  
The bucket names are retrieved from Airflow Variables and there are 2 corresponding properties in the release class, 
`download_bucket` and `transform_bucket`.  
These properties are convenient to use when uploading data to either one of these buckets.

### Clean up
The release class has a `cleanup` method which can be called inside a task that will 'clean up' by deleting the 3
 folders mentioned above.  
This method is part of the release class, because a clean up task is part of each telescope and it uses those
 folder paths described above that are properties of the release class. 
 

## Example
Below is an example of a simple telescope using the Telescope template.

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
from airflow.operators.sensors import ExternalTaskSensor

from observatory.platform.telescopes.telescope import Release, Telescope
from observatory.platform.utils.airflow_utils import AirflowVars, AirflowConns

class MyRelease(Release):
    def __init__(self, dag_id: str, release_date: pendulum.Pendulum):
        """Construct a Release instance

        :param dag_id: the id of the DAG.
        :param release_date: the release date (used to construct release_id).
        """
        
        self.release_date = release_date
        release_id = f'{dag_id}_{self.release_date.strftime("%Y_%m_%d")}'
        super().__init__(dag_id, release_id)


class MyTelescope(Telescope):
    """ MyTelescope Telescope."""
    DAG_ID = "my_telescope"

    def __init__(self,
                 dag_id: str = DAG_ID,
                 start_date: pendulum.Pendulum = pendulum.Pendulum(2020, 1, 1),
                 schedule_interval: str = '@weekly',
                 catchup: bool = True, 
                 queue: str = 'default', 
                 max_retries: int = 3, 
                 max_active_runs: int = 1,
                 airflow_vars: list = None, 
                 airflow_conns: list = None):
        """ Construct a Telescope instance.

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param catchup: whether to catchup the DAG or not.
        :param queue: the Airflow queue name.
        :param max_retries: the number of times to retry each task.
        :param max_active_runs: the maximum number of DAG runs that can be run at once.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        :param airflow_conns: list of airflow connection keys, for each connection it is checked if it exists in airflow
        """
        
        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.DATA_PATH,
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
            ]

        if airflow_conns is None:
            airflow_conns = [AirflowConns.SOMEDEFAULT_CONNECTION]
        
        super().__init__(dag_id, 
                         start_date, 
                         schedule_interval,
                         catchup=catchup,
                         queue=queue, 
                         max_retries=max_retries, 
                         max_active_runs=max_active_runs,
                         airflow_vars=airflow_vars,
                         airflow_conns=airflow_conns)

        # Add sensor tasks
        sensor = ExternalTaskSensor(external_dag_id='my_other_telescope', task_id='important_task', mode='reschedule')
        self.add_sensor(sensor)

        # Add setup tasks
        self.add_setup_task(self.check_dependencies)

        # Add generic tasks
        self.add_task(self.task1)
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> MyRelease:
        """ Make a release instance.
        
        :param kwargs: the context passed from the PythonOperator.
        :return: A release instance
        """
        release_date = kwargs["execution_date"]
        release = MyRelease(dag_id=self.dag_id, release_date=release_date)
        return release

    def task1(self, release: MyRelease, **kwargs):
        """ Add your own comments.
        
        :param release: A MyRelease instance
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        pass

    def cleanup(self, release: MyRelease, **kwargs):
        """ Delete downloaded, extracted and transformed files of the release.
        
        :param release: A MyRelease instance
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        release.cleanup()
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

from observatory.dags.telescopes.my_telescope import MyTelescope

telescope = MyTelescope()
globals()[telescope.dag_id] = telescope.make_dag()
```

In case you are familiar with creating DAGs in Airflow, below is the equivalent telescope without using the template.  
This might help to understand how the template works behind the scenes.  
  
Telescope and DAG in one file:  
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
import shutil
import logging
from pendulum import datetime
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators.sensors import ExternalTaskSensor

from observatory.platform.utils.airflow_utils import (
    AirflowConns, 
    AirflowVars, 
    check_connections, 
    check_variables
)
from observatory.platform.utils.template_utils import (
    SubFolder,
    on_failure_callback,
    telescope_path,
)


def check_dependencies() -> bool:
    """Checks the 'telescope' attributes, airflow variables & connections and possibly additional custom checks.

    :return: Whether variables and connections are available.
    """
    # check that vars and connections are available
    airflow_vars = [AirflowVars.DATA_PATH,
                    AirflowVars.PROJECT_ID,
                    AirflowVars.DATA_LOCATION,
                    AirflowVars.DOWNLOAD_BUCKET,
                    AirflowVars.TRANSFORM_BUCKET,]
    vars_valid = check_variables(*airflow_vars)

    airflow_conns = [AirflowConns.SOMEDEFAULT_CONNECTION]
    conns_valid = check_connections(*airflow_conns)

    if not vars_valid or not conns_valid:
        raise AirflowException("Required variables or connections are missing")

    return True


def task1(**kwargs):
    """ Add your own comments.

    :param kwargs: The context passed from the PythonOperator.
    :return: None.
    """
    pass


def cleanup(**kwargs):
    """ Delete downloaded, extracted and transformed files of the release.

    :param kwargs: The context passed from the PythonOperator.
    :return: None.
    """
    dag_id = 'my_telescope'
    release_date = kwargs["execution_date"]
    release_id = f'{dag_id}_{release_date.strftime("%Y_%m_%d")}'
    download_folder = telescope_path(SubFolder.downloaded.value, dag_id, release_id)
    extract_folder = telescope_path(SubFolder.extracted.value, dag_id, release_id)
    transform_folder = telescope_path(SubFolder.transformed.value, dag_id, release_id)

    for path in [download_folder, extract_folder, transform_folder]:
        try:
            shutil.rmtree(path)
        except FileNotFoundError as e:
            logging.warning(f"No such file or directory {path}: {e}")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
    'on_failure_callback': on_failure_callback,
    'retries': 3
}

with DAG(dag_id='my_telescope',
         start_date=datetime(2020, 1, 1),
         schedule_interval='@weekly',
         default_args=default_args,
         catchup=True,
         max_active_runs=1,
         doc_md='MyTelescope Telescope') as dag:

    sensor_task = ExternalTaskSensor(external_dag_id='my_other_telescope', 
                                     task_id='important_task', 
                                     mode='reschedule', 
                                     queue='default', 
                                     default_args=default_args, 
                                     provide_context=True)

    check_dependencies_task = ShortCircuitOperator(task_id='check_dependencies', 
                                                   python_callable=check_dependencies,
                                                   queue='default', 
                                                   default_args=default_args, 
                                                   provide_context=True)

    task_1 = PythonOperator(task_id='task1', 
                            python_callable=task1, 
                            queue='default', 
                            default_args=default_args,
                            provide_context=True)

    cleanup_task = PythonOperator(task_id='cleanup', 
                                  python_callable=cleanup,  
                                  queue='default', 
                                  default_args=default_args, 
                                  provide_context=True)

sensor_task >> check_dependencies_task >> task_1 >> cleanup_task
```