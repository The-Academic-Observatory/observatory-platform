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

import contextlib
import copy
import logging
import shutil
from abc import ABC, abstractmethod
from functools import partial
from typing import Any, Callable, Dict, List, Union

try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import chain
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BaseOperator
from observatory.platform.utils.airflow_utils import (
    AirflowVars,
    check_connections,
    check_variables,
)
from observatory.platform.utils.file_utils import list_files
from observatory.platform.utils.workflow_utils import SubFolder, on_failure_callback, workflow_path


class ReleaseFunction(Protocol):
    def __call__(self, release: "AbstractRelease", **kwargs: Any) -> Any:
        ...

    """ 
    :param release: A single instance of an AbstractRelease
    :param kwargs: the context passed from the PythonOperator. See 
    https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to 
    this argument.
    :return: Any.
    """


class ListReleaseFunction(Protocol):
    def __call__(self, releases: List["AbstractRelease"], **kwargs: Any) -> Any:
        ...

    """ 
    :param releases: A list of AbstractRelease instances
    :param kwargs: the context passed from the PythonOperator. See 
    https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to 
    this argument.
    :return: Any.
    """


WorkflowFunction = Union[ReleaseFunction, ListReleaseFunction]


class AbstractWorkflow(ABC):
    @abstractmethod
    def add_setup_task(self, func: Callable):
        """Add a setup task, which is used to run tasks before 'Release' objects are created, e.g. checking
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
        """Add a list of setup tasks, which are used to run tasks before 'Release' objects are created, e.g. checking
        dependencies, fetching available releases etc. (See add_setup_task for more info.)

        :param funcs: The list of functions that will be called by the ShortCircuitOperator task.
        :return: None.
        """
        pass

    @abstractmethod
    def add_operator(self, operator: BaseOperator):
        """Add an Apache Airflow operator.

        :param operator: the Apache Airflow operator.
        """
        pass

    @abstractmethod
    def add_operator_chain(self, operators: List[BaseOperator]):
        """Add a list of Apache Airflow operators.

        :param operators: the list of Apache Airflow operator.
        """
        pass

    @abstractmethod
    def add_task(self, func: Callable):
        """Add a task, which is used to process releases. A task has the following properties:

        - Has one of the following signatures 'def func(self, release: Release, **kwargs)' or 'def func(self, releases: List[Release], **kwargs)'
        - kwargs is the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to this argument.
        - Run by a PythonOperator.

        :param func: the function that will be called by the PythonOperator task.
        :return: None.
        """
        pass

    @abstractmethod
    def add_task_chain(self, funcs: List[Callable]):
        """Add a list of tasks, which are used to process releases. (See add_task for more info.)

        :param funcs: The list of functions that will be called by the PythonOperator task.
        :return: None.
        """
        pass

    @abstractmethod
    def task_callable(self, func: WorkflowFunction, **kwargs) -> Any:
        """Invoke a task callable. Creates a Release instance or Release instances and calls the given task method.

        :param func: the task method.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: Any.
        """
        pass

    @abstractmethod
    def make_release(self, **kwargs) -> Union["AbstractRelease", List["AbstractRelease"]]:
        """Make a release instance. The release is passed as an argument to the function (WorkflowFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: A release instance or list of release instances
        """
        pass

    @abstractmethod
    def make_dag(self) -> DAG:
        """Make an Airflow DAG for a workflow.

        :return: the DAG object.
        """
        pass


def make_task_id(func: Callable, kwargs: Dict) -> str:
    """Set a task_id from a func or kwargs.

    :param func: the task function.
    :param kwargs: the task kwargs parameter.
    :return: the task id.
    """

    task_id_key = "task_id"

    if task_id_key in kwargs:
        task_id = kwargs["task_id"]
    else:
        task_id = func.__name__

    return task_id


class Workflow(AbstractWorkflow):
    RELEASE_INFO = "releases"

    def __init__(
        self,
        dag_id: str,
        start_date: pendulum.DateTime,
        schedule_interval: str,
        catchup: bool = False,
        queue: str = "default",
        max_retries: int = 3,
        max_active_runs: int = 1,
        airflow_vars: list = None,
        airflow_conns: list = None,
    ):
        """Construct a Workflow instance.

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

        self.dag_id = dag_id
        self.start_date = start_date
        self.schedule_interval = schedule_interval
        self.catchup = catchup
        self.queue = queue
        self.max_retries = max_retries
        self.max_active_runs = max_active_runs
        self.airflow_vars = airflow_vars
        self.airflow_conns = airflow_conns
        self._parallel_tasks = False

        self.operators = []
        self.default_args = {
            "owner": "airflow",
            "start_date": self.start_date,
            "on_failure_callback": on_failure_callback,
            "retries": self.max_retries,
        }
        self.description = self.__doc__
        self.dag = DAG(
            dag_id=self.dag_id,
            schedule_interval=self.schedule_interval,
            default_args=self.default_args,
            catchup=self.catchup,
            max_active_runs=self.max_active_runs,
            doc_md=self.__doc__,
        )

    def add_operator(self, operator: BaseOperator):
        """Add an Apache Airflow operator.

        :param operator: the Apache Airflow operator.
        :return: None.
        """

        # Update operator settings
        operator.start_date = self.start_date
        operator.dag = self.dag
        operator.queue = self.queue
        operator.__dict__.update(self.default_args)

        # Add list of tasks while parallel_tasks is set
        if self._parallel_tasks:
            if len(self.operators) == 0 or not isinstance(self.operators[-1], List):
                self.operators.append([operator])
            else:
                self.operators[-1].append(operator)
        # Add single task to the end of the list
        else:
            self.operators.append(operator)

    def add_operator_chain(self, operators: List[BaseOperator]):
        """Add a list of Apache Airflow operators.

        :param operators: the list of Apache Airflow operator.
        """

        for operator in operators:
            self.add_operator(operator)

    def add_setup_task(self, func: Callable, **kwargs):
        """Add a setup task, which is used to run tasks before 'Release' objects are created, e.g. checking
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

        kwargs_ = copy.copy(kwargs)
        kwargs_["task_id"] = make_task_id(func, kwargs)
        op = ShortCircuitOperator(python_callable=func, **kwargs_)
        self.add_operator(op)

    def add_setup_task_chain(self, funcs: List[Callable], **kwargs):
        """Add a list of setup tasks, which are used to run tasks before 'Release' objects are created, e.g. checking
        dependencies, fetching available releases etc. (See add_setup_task for more info.)

        :param funcs: The list of functions that will be called by the ShortCircuitOperator task.
        :return: None.
        """

        for func in funcs:
            self.add_setup_task(func, **copy.copy(kwargs))

    def add_task(
        self,
        func: Callable,
        **kwargs,
    ):
        """Add a task, which is used to process releases. A task has the following properties:

        - Has one of the following signatures 'def func(self, release: Release, **kwargs)' or 'def func(self,
        releases: List[Release], **kwargs)'
        - kwargs is the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        - Run by a PythonOperator.

        :param func: the function that will be called by the PythonOperator task.
        :return: None.
        """

        kwargs_ = copy.copy(kwargs)
        kwargs_["task_id"] = make_task_id(func, kwargs)
        op = PythonOperator(python_callable=partial(self.task_callable, func), **kwargs_)
        self.add_operator(op)

    def add_task_chain(self, funcs: List[Callable], **kwargs):
        """Add a list of tasks, which are used to process releases. (See add_task for more info.)

        :param funcs: The list of functions that will be called by the PythonOperator task.
        :return: None.
        """

        for func in funcs:
            self.add_task(func, **copy.copy(kwargs))

    @contextlib.contextmanager
    def parallel_tasks(self):
        """When called, all tasks added to the workflow within the `with` block will run in parallel.
        add_task and add_task_chain can be used with this function.

        :return: None.
        """

        try:
            self._parallel_tasks = True
            yield
        finally:
            self._parallel_tasks = False

    def task_callable(self, func: WorkflowFunction, **kwargs) -> Any:
        """Invoke a task callable. Creates a Release instance and calls the given task method. The result can be
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
        """Make an Airflow DAG for a workflow.

        :return: the DAG object.
        """

        with self.dag:
            chain(*self.operators)

        return self.dag

    def check_dependencies(self, **kwargs) -> bool:
        """Checks the 'workflow' attributes, airflow variables & connections and possibly additional custom checks.

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
            raise AirflowException("Required variables or connections are missing")

        return True


class AbstractRelease(ABC):
    """The abstract release interface"""

    @property
    @abstractmethod
    def download_bucket(self):
        """The download bucket name.

        :return: the download bucket name.
        """
        pass

    @property
    @abstractmethod
    def transform_bucket(self):
        """The transform bucket name.

        :return: the transform bucket name.
        """
        pass

    @property
    @abstractmethod
    def download_folder(self) -> str:
        """The download folder path for the release, e.g. /path/to/workflows/download/{dag_id}/{release_id}/

        :return: the download folder path.
        """
        pass

    @property
    @abstractmethod
    def extract_folder(self) -> str:
        """The extract folder path for the release, e.g. /path/to/workflows/extract/{dag_id}/{release_id}/

        :return: the extract folder path.
        """
        pass

    @property
    @abstractmethod
    def transform_folder(self) -> str:
        """The transform folder path for the release, e.g. /path/to/workflows/transform/{dag_id}/{release_id}/

        :return: the transform folder path.
        """
        pass

    @property
    @abstractmethod
    def download_files(self) -> List[str]:
        """List all files downloaded as a part of this release.

        :return: list of downloaded files.
        """
        pass

    @property
    @abstractmethod
    def extract_files(self) -> List[str]:
        """List all files extracted as a part of this release.

        :return: list of extracted files.
        """
        pass

    @property
    @abstractmethod
    def transform_files(self) -> List[str]:
        """List all files transformed as a part of this release.

        :return: list of transformed files.
        """
        pass

    @abstractmethod
    def cleanup(self) -> None:
        """Delete all files and folders associated with this release.

        :return: None.
        """

        pass


class Release(AbstractRelease):
    """Used to store info on a given release"""

    def __init__(
        self,
        dag_id: str,
        release_id: str,
        download_files_regex: str = None,
        extract_files_regex: str = None,
        transform_files_regex: str = None,
    ):
        """Construct a Release instance

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
        """The download folder path for the release, e.g. /path/to/workflows/download/{dag_id}/{release_id}/

        :return: the download folder path.
        """
        return workflow_path(SubFolder.downloaded.value, self.dag_id, self.release_id)

    @property
    def extract_folder(self) -> str:
        """The extract folder path for the release, e.g. /path/to/workflows/extract/{dag_id}/{release_id}/

        :return: the extract folder path.
        """
        return workflow_path(SubFolder.extracted.value, self.dag_id, self.release_id)

    @property
    def transform_folder(self) -> str:
        """The transform folder path for the release, e.g. /path/to/workflows/transform/{dag_id}/{release_id}/

        :return: the transform folder path.
        """
        return workflow_path(SubFolder.transformed.value, self.dag_id, self.release_id)

    @property
    def download_files(self) -> List[str]:
        """List all files downloaded as a part of this release.

        :return: list of downloaded files.
        """
        return list_files(self.download_folder, self.download_files_regex)

    @property
    def extract_files(self) -> List[str]:
        """List all files extracted as a part of this release.

        :return: list of extracted files.
        """
        return list_files(self.extract_folder, self.extract_files_regex)

    @property
    def transform_files(self) -> List[str]:
        """List all files transformed as a part of this release.

        :return: list of transformed files.
        """
        return list_files(self.transform_folder, self.transform_files_regex)

    @property
    def download_bucket(self):
        """The download bucket name.

        :return: the download bucket name.
        """
        return Variable.get(AirflowVars.DOWNLOAD_BUCKET)

    @property
    def transform_bucket(self):
        """The transform bucket name.

        :return: the transform bucket name.
        """
        return Variable.get(AirflowVars.TRANSFORM_BUCKET)

    def cleanup(self) -> None:
        """Delete all files and folders associated with this release.

        :return: None.
        """
        for path in [self.download_folder, self.extract_folder, self.transform_folder]:
            try:
                shutil.rmtree(path)
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {path}: {e}")
