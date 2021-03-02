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

# Author: Aniek Roelofs, James Diprose, Tuan Chien

import datetime
import functools
import logging
import shutil
from abc import ABC, abstractmethod
from functools import partial
from typing import Any, Callable, List
from typing import Union

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.variable import Variable
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.utils.helpers import chain
from observatory.platform.utils.airflow_utils import AirflowVars, check_connections, check_variables
from observatory.platform.utils.file_utils import list_files
from observatory.platform.utils.template_utils import SubFolder, on_failure_callback, telescope_path
from typing_extensions import Protocol


class ReleaseFunction(Protocol):
    def __call__(self, release: 'AbstractRelease', **kwargs: Any) -> Any: ...
    """ 
    :param release: A single instance of an AbstractRelease
    :param kwargs: the context passed from the PythonOperator. See 
    https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to 
    this argument.
    :return: Any.
    """


class ListReleaseFunction(Protocol):
    def __call__(self, releases: List['AbstractRelease'], **kwargs: Any) -> Any: ...
    """ 
    :param releases: A list of AbstractRelease instances
    :param kwargs: the context passed from the PythonOperator. See 
    https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to 
    this argument.
    :return: Any.
    """


TelescopeFunction = Union[ReleaseFunction, ListReleaseFunction]


class AbstractTelescope(ABC):
    @abstractmethod
    def add_setup_task(self, func: Callable):
        """ Add a setup task, which is used to run tasks before 'Release' objects are created, e.g. checking
        dependencies, fetching available releases etc.

        A setup task has the following properties:
        - Has the signature 'def func(self, **kwargs) -> bool', where
        kwargs is the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        - Run by a ShortCircuitOperator, meaning that a setup task can stop a DAG prematurely, e.g. if there is
        nothing to process.
        - func Needs to return a boolean
        :param func: the function that will be called by the ShortCircuitOperator task.
        :return: None.
        """
        pass

    @abstractmethod
    def add_setup_task_chain(self, funcs: List[Callable]):
        """ Add a list of setup tasks, which are used to run tasks before 'Release' objects are created, e.g. checking
        dependencies, fetching available releases etc. (See add_setup_task for more info.)

        :param funcs: The list of functions that will be called by the ShortCircuitOperator task.
        :return: None.
        """
        pass

    @abstractmethod
    def add_task(self, func: Callable):
        """ Add a task, which is used to process releases. A task has the following properties:

        - Has one of the following signatures 'def func(self, release: Release, **kwargs)' or 'def func(self,
        releases: List[Release], **kwargs)'
        - kwargs is the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        - Run by a PythonOperator.
        :param func: the function that will be called by the PythonOperator task.
        :return: None.
        """
        pass

    @abstractmethod
    def add_task_chain(self, funcs: List[Callable]):
        """ Add a list of tasks, which are used to process releases. (See add_task for more info.)

        :param funcs: The list of functions that will be called by the PythonOperator task.
        :return: None.
        """
        pass

    @abstractmethod
    def task_callable(self, func: TelescopeFunction, **kwargs) -> Any:
        """ Invoke a task callable. Creates a Release instance or Release instances and calls the given task method.

        :param func: the task method.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: Any.
        """
        pass

    @abstractmethod
    def make_release(self, **kwargs) -> Union['AbstractRelease', List['AbstractRelease']]:
        """ Make a release instance. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: A release instance or list of release instances
        """
        pass

    @abstractmethod
    def make_dag(self) -> DAG:
        """ Make an Airflow DAG for a telescope.

        :return: the DAG object.
        """
        pass


class Telescope(AbstractTelescope):
    RELEASE_INFO = 'releases'

    def __init__(self, dag_id: str, start_date: datetime, schedule_interval: str, catchup: bool = False,
                 queue: str = 'default', max_retries: int = 3, max_active_runs: int = 1, schema_prefix: str = '',
                 schema_version: str = None, airflow_vars: list = None, airflow_conns: list = None):
        """ Construct a Telescope instance.
        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param catchup: whether to catchup the DAG or not.
        :param queue: the Airflow queue name.
        :param max_retries: the number of times to retry each task.
        :param max_active_runs: the maximum number of DAG runs that can be run at once.
        :param schema_prefix: the prefix used to find the schema path
        :param schema_version: the version used to find the schema path
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        :param airflow_conns: list of airflow connection keys, for each connection it is checked if it exists in airflow
        """
        self.dag_id = dag_id
        self.start_date = start_date
        self.schedule_interval = schedule_interval
        self.catchup = catchup
        self.queue = queue
        self.max_retries = max_retries
        self.max_active_runs = max_active_runs
        self.schema_prefix = schema_prefix
        self.schema_version = schema_version
        self.airflow_vars = airflow_vars
        self.airflow_conns = airflow_conns

        self.setup_task_funcs = []
        self.task_funcs = []
        self.default_args = {
            "owner": "airflow",
            "start_date": self.start_date,
            'on_failure_callback': on_failure_callback
        }
        self.description = self.__doc__
        self.dag = DAG(dag_id=self.dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args,
                       catchup=self.catchup, max_active_runs=self.max_active_runs, doc_md=self.__doc__)

    def add_setup_task(self, func: Callable):
        """ Add a setup task, which is used to run tasks before 'Release' objects are created, e.g. checking
        dependencies, fetching available releases etc.

        A setup task has the following properties:
        - Has the signature 'def func(self, **kwargs) -> bool', where
        kwargs is the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        - Run by a ShortCircuitOperator, meaning that a setup task can stop a DAG prematurely, e.g. if there is
        nothing to process.
        - func Needs to return a boolean
        :param func: the function that will be called by the ShortCircuitOperator task.
        :return: None.
        """
        self.setup_task_funcs.append(func)

    def add_setup_task_chain(self, funcs: List[Callable]):
        """ Add a list of setup tasks, which are used to run tasks before 'Release' objects are created, e.g. checking
        dependencies, fetching available releases etc. (See add_setup_task for more info.)

        :param funcs: The list of functions that will be called by the ShortCircuitOperator task.
        :return: None.
        """
        self.setup_task_funcs += funcs

    def add_task(self, func: Callable):
        """ Add a task, which is used to process releases. A task has the following properties:

        - Has one of the following signatures 'def func(self, release: Release, **kwargs)' or 'def func(self,
        releases: List[Release], **kwargs)'
        - kwargs is the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        - Run by a PythonOperator.
        :param func: the function that will be called by the PythonOperator task.
        :return: None.
        """
        self.task_funcs.append(func)

    def add_task_chain(self, funcs: List[Callable]):
        """ Add a list of tasks, which are used to process releases. (See add_task for more info.)

        :param funcs: The list of functions that will be called by the PythonOperator task.
        :return: None.
        """
        self.task_funcs += funcs

    def task_callable(self, func: TelescopeFunction, **kwargs) -> Any:
        """ Invoke a task callable. Creates a Release instance and calls the given task method. The result can be
        pulled as an xcom in Airflow.
        :param func: the task method.
        :param kwargs: the context passed from the PythonOperator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: Any.
        """

        release = self.make_release(**kwargs)
        result = func(release, **kwargs)
        return result

    def make_dag(self) -> DAG:
        """ Make an Airflow DAG for a telescope.
        :return: the DAG object.
        """
        tasks = []
        with self.dag:
            # Process setup tasks first, which are always ShortCircuitOperators
            for func in self.setup_task_funcs:
                if isinstance(func, functools.partial):
                    task = func()
                else:
                    task = ShortCircuitOperator(task_id=func.__name__, python_callable=func, queue=self.queue,
                                                default_args=self.default_args, provide_context=True)
                tasks.append(task)

            # Process all other tasks next, which are always PythonOperators
            for func in self.task_funcs:
                if isinstance(func, functools.partial):
                    task = func()
                else:
                    task = PythonOperator(task_id=func.__name__, python_callable=partial(self.task_callable, func),
                                          queue=self.queue, default_args=self.default_args, provide_context=True)
                tasks.append(task)
            chain(*tasks)

        return self.dag

    def check_dependencies(self, **kwargs) -> bool:
        """ Checks the 'telescope' attributes, airflow variables & connections and possibly additional custom checks.
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        # check that vars and connections are available
        vars_valid = True
        conns_valid = True
        if self.airflow_vars:
            vars_valid = check_variables(*self.airflow_vars)
        if self.airflow_conns:
            conns_valid = check_connections(*self.airflow_conns)

        if not vars_valid or not conns_valid:
            raise AirflowException('Required variables or connections are missing')

        return True


class AbstractRelease(ABC):
    """ The abstract release interface """

    @property
    @abstractmethod
    def download_bucket(self):
        """ The download bucket name.
        :return: the download bucket name.
        """
        pass

    @property
    @abstractmethod
    def transform_bucket(self):
        """ The transform bucket name.
        :return: the transform bucket name.
        """
        pass

    @property
    @abstractmethod
    def download_folder(self) -> str:
        """ The download folder path for the release, e.g. /path/to/telescopes/download/{dag_id}/{release_id}/
        :return: the download folder path.
        """
        pass

    @property
    @abstractmethod
    def extract_folder(self) -> str:
        """ The extract folder path for the release, e.g. /path/to/telescopes/extract/{dag_id}/{release_id}/
        :return: the extract folder path.
        """
        pass

    @property
    @abstractmethod
    def transform_folder(self) -> str:
        """ The transform folder path for the release, e.g. /path/to/telescopes/transform/{dag_id}/{release_id}/
        :return: the transform folder path.
        """
        pass

    @property
    @abstractmethod
    def download_files(self) -> List[str]:
        """ List all files downloaded as a part of this release.
        :return: list of downloaded files.
        """
        pass

    @property
    @abstractmethod
    def extract_files(self) -> List[str]:
        """ List all files extracted as a part of this release.
        :return: list of extracted files.
        """
        pass

    @property
    @abstractmethod
    def transform_files(self) -> List[str]:
        """ List all files transformed as a part of this release.
        :return: list of transformed files.
        """
        pass

    @abstractmethod
    def cleanup(self) -> None:
        """ Delete all files and folders associated with this release.
        :return: None.
        """

        pass


class Release(AbstractRelease):
    """ Used to store info on a given release"""

    def __init__(self, dag_id: str, release_id: str, download_files_regex: str = None, extract_files_regex: str = None,
                 transform_files_regex: str = None):
        """ Construct a Release instance
        :param dag_id: the id of the DAG.
        :param release_id: the id of the release.
        :param download_files_regex: regex pattern that is used to find files in download folder
        :param extract_files_regex: regex pattern that is used to find files in extract folder
        :param transform_files_regex: regex pattern that is used to find files in transform folder
        """
        self.dag_id = dag_id
        self.release_id = release_id
        self.download_files_regex = download_files_regex
        self.extract_files_regex = extract_files_regex
        self.transform_files_regex = transform_files_regex

    @property
    def download_folder(self) -> str:
        """ The download folder path for the release, e.g. /path/to/telescopes/download/{dag_id}/{release_id}/
        :return: the download folder path.
        """
        return telescope_path(SubFolder.downloaded.value, self.dag_id, self.release_id)

    @property
    def extract_folder(self) -> str:
        """ The extract folder path for the release, e.g. /path/to/telescopes/extract/{dag_id}/{release_id}/
        :return: the extract folder path.
        """
        return telescope_path(SubFolder.extracted.value, self.dag_id, self.release_id)

    @property
    def transform_folder(self) -> str:
        """ The transform folder path for the release, e.g. /path/to/telescopes/transform/{dag_id}/{release_id}/
        :return: the transform folder path.
        """
        return telescope_path(SubFolder.transformed.value, self.dag_id, self.release_id)

    @property
    def download_files(self) -> List[str]:
        """ List all files downloaded as a part of this release.
        :return: list of downloaded files.
        """
        return list_files(self.download_folder, self.download_files_regex)

    @property
    def extract_files(self) -> List[str]:
        """ List all files extracted as a part of this release.
        :return: list of extracted files.
        """
        return list_files(self.extract_folder, self.extract_files_regex)

    @property
    def transform_files(self) -> List[str]:
        """ List all files transformed as a part of this release.
        :return: list of transformed files.
        """
        return list_files(self.transform_folder, self.transform_files_regex)

    @property
    def download_bucket(self):
        """ The download bucket name.
        :return: the download bucket name.
        """
        return Variable.get(AirflowVars.DOWNLOAD_BUCKET)

    @property
    def transform_bucket(self):
        """ The transform bucket name.
        :return: the transform bucket name.
        """
        return Variable.get(AirflowVars.TRANSFORM_BUCKET)

    def cleanup(self) -> None:
        """ Delete all files and folders associated with this release.
        :return: None.
        """
        for path in [self.download_folder, self.extract_folder, self.transform_folder]:
            try:
                shutil.rmtree(path)
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {path}: {e}")
