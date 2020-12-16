import os
from typing import Union, List
from observatory_platform.utils.config_utils import SubFolder
from observatory_platform.utils.config_utils import telescope_path
from abc import ABC, abstractmethod
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.helpers import chain
import pendulum
import logging
import datetime
from typing import Callable, Type, List, Any
from functools import partial
from observatory_platform.utils.config_utils import check_variables, check_connections
from typing_extensions import Protocol
from collections import defaultdict
import functools


class UserFunction(Protocol):
    def __call__(self, release: 'AbstractTelescopeRelease', **kwargs: Any) -> Any: ...


def on_failure_callback(kwargs):
    logging.info("ON FAILURE CALLBACK")


class AbstractTelescope(ABC):
    @abstractmethod
    def add_before_subdag_task(self, func: Callable):
        pass

    @abstractmethod
    def add_before_subdag_chain(self, funcs: List[Callable]):
        pass

    @abstractmethod
    def add_release_info_task(self, func: Callable):
        pass

    @abstractmethod
    def add_release_info_chain(self, funcs: List[Callable]):
        pass

    @abstractmethod
    def add_extract_task(self, func: Callable):
        pass

    @abstractmethod
    def add_extract_chain(self, funcs: List[Callable]):
        pass

    @abstractmethod
    def add_transform_task(self, func: Callable):
        pass

    @abstractmethod
    def add_transform_chain(self, funcs: List[Callable]):
        pass

    @abstractmethod
    def add_load_task(self, func: Callable):
        pass

    @abstractmethod
    def add_load_chain(self, funcs: List[Callable]):
        pass

    @abstractmethod
    def task_callable(self, func: UserFunction, **kwargs):
        pass

    @abstractmethod
    def make_subdag_instance(self, subdag_id: str) -> DAG:
        pass

    def make_dag(self) -> DAG:
        pass

    @abstractmethod
    def check_dependencies(self, **kwargs):
        pass

    @abstractmethod
    def cleanup(self, release: Union['TelescopeRelease', List['TelescopeRelease']], **kwargs):
        pass


class Telescope(AbstractTelescope):
    RELEASE_INFO = 'releases'

    def __init__(self, release_cls: Type['TelescopeRelease'], start_date: datetime, dag_id: str,
                 subdag_ids: list, schedule_interval: str,
                 catchup: bool, queue: str, max_retries: int, description: str, airflow_vars: list,
                 airflow_conns: list):
        self.release_cls = release_cls
        self.start_date = start_date
        self.dag_id = dag_id
        self.subdag_ids = subdag_ids
        self.schedule_interval = schedule_interval
        self.catchup = catchup
        self.queue = queue
        self.max_retries = max_retries
        self.description = description
        self.airflow_vars = airflow_vars
        self.airflow_conns = airflow_conns

        self.before_subdag_tasks = defaultdict(list)
        self.release_info_tasks = defaultdict(list)
        self.extract_tasks = defaultdict(list)
        self.transform_tasks = defaultdict(list)
        self.load_tasks = defaultdict(list)

        self.default_args = {
            "owner": "airflow",
            "start_date": self.start_date,
            'on_failure_callback': on_failure_callback
        }
        self.dag = DAG(dag_id=self.dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args,
                       catchup=self.catchup, max_active_runs=1, doc_md=self.__doc__)

    def _add_tasks_to_dict(self, funcs: Union[Callable, List[Callable]], tasks_dictionary):
        dag_ids = self.subdag_ids
        if tasks_dictionary == self.before_subdag_tasks:
            dag_ids = self.dag_id
        funcs = funcs if isinstance(funcs, list) else [funcs]
        for dag_id in dag_ids:
            tasks_dictionary[dag_id] = tasks_dictionary[dag_id] + funcs

    def add_before_subdag_task(self, func: Callable):
        self._add_tasks_to_dict(func, self.before_subdag_tasks)

    def add_before_subdag_chain(self, funcs: List[Callable]):
        self._add_tasks_to_dict(funcs, self.before_subdag_tasks)

    def add_release_info_task(self, func: Callable):
        self._add_tasks_to_dict(func, self.release_info_tasks)

    def add_release_info_chain(self, funcs: List[Callable]):
        self._add_tasks_to_dict(funcs, self.release_info_tasks)

    def add_extract_task(self, func: Callable):
        self._add_tasks_to_dict(func, self.extract_tasks)

    def add_extract_chain(self, funcs: List[Callable]):
        self._add_tasks_to_dict(funcs, self.extract_tasks)

    def add_transform_task(self, func: Callable):
        self._add_tasks_to_dict(func, self.transform_tasks)

    def add_transform_chain(self, funcs: List[Callable]):
        self._add_tasks_to_dict(funcs, self.transform_tasks)

    def add_load_task(self, func: Callable):
        self._add_tasks_to_dict(func, self.load_tasks)

    def add_load_chain(self, funcs: List[Callable]):
        self._add_tasks_to_dict(funcs, self.load_tasks)

    def check_dependencies(self, **kwargs):
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

    def make_subdag_instance(self, subdag_id: str) -> DAG:
        subdag = DAG(dag_id=f"{self.dag_id}.{subdag_id}", schedule_interval=self.schedule_interval,
                     default_args=self.default_args)
        return subdag

    def make_dag(self) -> DAG:
        tasks = []
        with self.dag:
            for func in self.before_subdag_tasks[self.dag_id]:
                if isinstance(func, functools.partial):
                    task = func()
                else:
                    task = PythonOperator(task_id=func.__name__, provide_context=True, python_callable=func,
                                          queue=self.queue)
                tasks.append(task)

            if self.subdag_ids:
                subdag_tasks = []
                for subdag_id in self.subdag_ids:
                    subdag = self.make_subdag_instance(subdag_id)
                    task = SubDagOperator(task_id=subdag_id,
                                          subdag=self._attach_tasks_to_dag(subdag))
                    subdag_tasks.append(task)
                tasks.append(subdag_tasks)
            chain(*tasks)
        self._attach_tasks_to_dag(self.dag)
        return self.dag

    def _attach_tasks_to_dag(self, dag: DAG) -> DAG:
        check_dependencies_tasks = [self.check_dependencies]
        cleanup_tasks = [self.cleanup]
        if dag.dag_id == self.dag_id and self.subdag_ids:
            dag_id = self.dag_id
            check_dependencies_tasks = []
            cleanup_tasks = []
        elif dag.dag_id == self.dag_id:
            dag_id = self.dag_id
        else:
            dag_id = dag.dag_id[len(f"{self.dag_id}."):]

        tasks = []
        with dag:
            # split tasks based on whether they require 'release' instance as param in python callable
            for func in check_dependencies_tasks + self.release_info_tasks[dag_id]:
                if isinstance(func, functools.partial):
                    task = func()
                else:
                    task = PythonOperator(task_id=func.__name__, provide_context=True, python_callable=func,
                                          queue=self.queue, default_args=self.default_args)
                tasks.append(task)

            for func in self.extract_tasks[dag_id] + self.transform_tasks[dag_id] + self.load_tasks[dag_id] + cleanup_tasks:
                if isinstance(func, functools.partial):
                    task = func()
                else:
                    trigger_rule = 'all_success'
                    if func == self.cleanup:
                        trigger_rule = 'none_failed'
                    task = PythonOperator(task_id=func.__name__, provide_context=True,
                                          python_callable=partial(self.task_callable, func), queue=self.queue,
                                          trigger_rule=trigger_rule, default_args=self.default_args)
                tasks.append(task)
            chain(*tasks)
        return dag


class AbstractTelescopeRelease(ABC):
    # @staticmethod
    # @abstractmethod
    # def make_release(telescope) -> Union['AbstractTelescopeRelease', List['AbstractTelescopeRelease']]:
    #     pass

    @property
    @abstractmethod
    def date_str(self) -> str:
        pass

    @property
    @abstractmethod
    def blob_dir(self) -> str:
        pass

    @property
    @abstractmethod
    def extract_dir(self) -> str:
        pass

    @property
    @abstractmethod
    def extract_paths(self) -> list:
        pass

    @property
    @abstractmethod
    def transform_paths(self) -> list:
        pass

    @property
    @abstractmethod
    def transform_dir(self) -> str:
        pass

    @abstractmethod
    def subdir(self, sub_folder: SubFolder) -> str:
        pass

    @abstractmethod
    def create_path(self, sub_folder: SubFolder, file_name: str, ext: str) -> str:
        pass

    @abstractmethod
    def get_blob_name(self, path: str) -> str:
        pass

    @abstractmethod
    def is_file_required(self, path: str, required_files: list) -> bool:
        pass


class TelescopeRelease(AbstractTelescopeRelease):
    """ Used to store info on a given release"""

    def __init__(self, dag_id: str):
        self.dag_id = dag_id

    @property
    def blob_dir(self) -> str:
        return f'telescopes/{self.dag_id}'

    @property
    def extract_dir(self) -> str:
        return self.subdir(SubFolder.extracted)

    @property
    def transform_dir(self) -> str:
        return self.subdir(SubFolder.transformed)

    @property
    def extract_paths(self) -> list:
        extract_paths = []
        for root, dirs, files in os.walk(self.extract_dir):
            for file in files:
                extract_paths.append(os.path.join(root, file))
        return extract_paths

    @property
    def transform_paths(self) -> list:
        transform_paths = []
        for root, dirs, files in os.walk(self.transform_dir):
            for file in files:
                transform_paths.append(os.path.join(root, file))
        return transform_paths

    def subdir(self, sub_folder: SubFolder) -> str:
        """ Path to subdirectory of a specific release for either downloaded/extracted/transformed files.
        Will also create the directory if it doesn't exist yet.
        :param sub_folder: Name of the subfolder
        :return: Path to the directory
        """
        subdir = os.path.join(telescope_path(sub_folder, self.dag_id), self.date_str)
        if not os.path.exists(subdir):
            os.makedirs(subdir, exist_ok=True)
        return subdir

    def create_path(self, sub_folder: Union[SubFolder, str], file_name: str, ext: str) -> str:
        """
        Gets path to file based on subfolder, file name and extension.
        :param sub_folder: Name of the subfolder
        :param file_name: Name of the file
        :param ext: The extension of the file
        :return: The file path.
        """
        path = os.path.join(self.subdir(sub_folder), f"{file_name}_{self.date_str}.{ext}")
        return path

    def get_blob_name(self, path: str) -> str:
        file_name, ext = os.path.splitext(os.path.basename(path))
        blob_name = os.path.join(self.blob_dir, f"{file_name}_{self.date_str}.{ext}")
        return blob_name

    def is_file_required(self, path: str, required_files: list) -> bool:
        file_name, ext = os.path.splitext(os.path.basename(path))
        return True if file_name in required_files else False
