# Copyright 2019, 2020 Curtin University
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

# Author: James Diprose, Aniek Roelofs, Tuan Chien


from __future__ import annotations

import base64
import binascii
import json
import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, ClassVar, Dict, List, TextIO, Tuple, Union, Optional

import cerberus.validator
import yaml
from cerberus import Validator
from cryptography.fernet import Fernet

from observatory.platform.cli.cli_utils import (
    INDENT1,
    INDENT3,
    comment,
    indent,
)
from observatory.platform.config import (
    observatory_home as default_observatory_home,
)
from observatory.platform.terraform.terraform_api import TerraformVariable


def generate_fernet_key() -> str:
    """Generate a Fernet key.

    :return: A newly generated Fernet key.
    """

    return Fernet.generate_key().decode("utf8")


def generate_secret_key(length: int = 30) -> str:
    """Generate a secret key for the Flask Airflow Webserver.

    :param length: the length of the key to generate.
    :return: the random key.
    """

    return binascii.b2a_hex(os.urandom(length)).decode("utf-8")


def save_yaml(file_path: str, dict_: Dict):
    """Save a yaml file from a dictionary.

    :param file_path: the path to the file to save.
    :param dict_: the dictionary.
    :return: None.
    """

    with open(file_path, "w") as yaml_file:
        yaml.dump(dict_, yaml_file, default_flow_style=False)


def to_hcl(value: Dict) -> str:
    """Convert a Python dictionary into HCL.

    :param value: the dictionary.
    :return: the HCL string.
    """

    return json.dumps(value, separators=(",", "="))


def from_hcl(string: str) -> Dict:
    """Convert an HCL string into a Dict.

    :param string: the HCL string.
    :return: the dict.
    """

    return json.loads(string.replace('"=', '":'))


class BackendType(Enum):
    """The type of backend"""

    local = "local"
    terraform = "terraform"


class Environment(Enum):
    """The environment being used"""

    develop = "develop"
    staging = "staging"
    production = "production"


@dataclass
class Backend:
    """The backend settings for the Observatory Platform.

    Attributes:
        type: the type of backend being used (local environment or Terraform).
        environment: what type of environment is being deployed (develop, staging or production).
    """

    type: BackendType = BackendType.local
    environment: Environment = Environment.develop

    @staticmethod
    def from_dict(dict_: Dict) -> Backend:
        """Constructs a Backend instance from a dictionary.

        :param dict_: the dictionary.
        :return: the Backend instance.
        """

        backend_type = BackendType(dict_.get("type"))
        environment = Environment(dict_.get("environment"))

        return Backend(
            backend_type,
            environment,
        )


@dataclass
class PythonPackage:
    name: str
    host_package: str
    docker_package: str
    type: str


@dataclass
class Observatory:
    """The Observatory settings for the Observatory Platform.

    Attributes:
        :param package: the observatory platform package, either a local path to a Python source package (editable type),
        path to a sdist (sdist) or a PyPI package name and version (pypi).
        :param package_type: the package type, editable, sdist, pypi.
        :param airflow_fernet_key: the Fernet key.
        :param airflow_secret_key: the secret key used to run the flask app.
        :param airflow_ui_user_password: the password for the Apache Airflow UI admin user.
        :param airflow_ui_user_email: the email address for the Apache Airflow UI admin user.
        :param observatory_home: The observatory home folder.
        :param postgres_password: the Postgres SQL password.
        :param redis_port: The host Redis port number.
        :param flower_ui_port: The host's Flower UI port number.
        :param airflow_ui_port: The host's Apache Airflow UI port number.
        :param docker_network_name: The Docker Network name, used to specify a custom Docker Network.
        :param docker_network_is_external: whether the docker network is external or not.
        :param docker_compose_project_name: The namespace for the Docker Compose containers: https://docs.docker.com/compose/reference/envvars/#compose_project_name.
        :param api_port: Port for accessing the API locally (exposed by docker container).
    """

    package: str = "observatory-platform"
    package_type: str = "pypi"
    airflow_fernet_key: str = field(default_factory=generate_fernet_key)
    airflow_secret_key: str = field(default_factory=generate_secret_key)
    airflow_ui_user_email: str = "airflow@airflow.com"
    airflow_ui_user_password: str = "airflow"
    observatory_home: str = default_observatory_home()
    postgres_password: str = "postgres"
    redis_port: int = 6379
    flower_ui_port: int = 5555
    airflow_ui_port: int = 8080
    docker_network_name: str = "observatory-network"
    docker_network_is_external: bool = False
    docker_compose_project_name: str = "observatory"
    api_package: str = "observatory-api"
    api_package_type: str = "pypi"
    api_port: int = 5002

    def to_hcl(self):
        return to_hcl(
            {
                "airflow_fernet_key": self.airflow_fernet_key,
                "airflow_secret_key": self.airflow_secret_key,
                "airflow_ui_user_password": self.airflow_ui_user_password,
                "airflow_ui_user_email": self.airflow_ui_user_email,
                "postgres_password": self.postgres_password,
            }
        )

    @property
    def host_package(self):
        return os.path.normpath(self.package)

    @staticmethod
    def from_dict(dict_: Dict) -> Observatory:
        """Constructs an Airflow instance from a dictionary.

        :param dict_: the dictionary.
        :return: the Airflow instance.
        """

        package = dict_.get("package")
        package_type = dict_.get("package_type")
        airflow_fernet_key = dict_.get("airflow_fernet_key")
        airflow_secret_key = dict_.get("airflow_secret_key")
        airflow_ui_user_email = dict_.get("airflow_ui_user_email", Observatory.airflow_ui_user_email)
        airflow_ui_user_password = dict_.get("airflow_ui_user_password", Observatory.airflow_ui_user_password)
        observatory_home = dict_.get("observatory_home", Observatory.observatory_home)
        postgres_password = dict_.get("postgres_password", Observatory.postgres_password)
        redis_port = dict_.get("redis_port", Observatory.redis_port)
        flower_ui_port = dict_.get("flower_ui_port", Observatory.flower_ui_port)
        airflow_ui_port = dict_.get("airflow_ui_port", Observatory.airflow_ui_port)
        docker_network_name = dict_.get("docker_network_name", Observatory.docker_network_name)
        docker_network_is_external = dict_.get("docker_network_is_external", Observatory.docker_network_is_external)
        docker_compose_project_name = dict_.get("docker_compose_project_name", Observatory.docker_compose_project_name)
        api_package = dict_.get("api_package", Observatory.api_package)
        api_package_type = dict_.get("api_package_type", Observatory.api_package_type)
        api_port = dict_.get("api_port", Observatory.api_port)

        return Observatory(
            package,
            package_type,
            airflow_fernet_key,
            airflow_secret_key,
            airflow_ui_user_password=airflow_ui_user_password,
            airflow_ui_user_email=airflow_ui_user_email,
            observatory_home=observatory_home,
            postgres_password=postgres_password,
            redis_port=redis_port,
            flower_ui_port=flower_ui_port,
            airflow_ui_port=airflow_ui_port,
            docker_network_name=docker_network_name,
            docker_network_is_external=docker_network_is_external,
            docker_compose_project_name=docker_compose_project_name,
            api_package=api_package,
            api_package_type=api_package_type,
            api_port=api_port,
        )


@dataclass
class CloudStorageBucket:
    """Represents a Google Cloud Storage Bucket.

    Attributes:
        id: the id of the bucket (which gets set as an Airflow variable).
        name: the name of the Google Cloud storage bucket.
    """

    id: str
    name: str

    @staticmethod
    def parse_buckets(buckets: Dict) -> List[CloudStorageBucket]:
        return parse_dict_to_list(buckets, CloudStorageBucket)


@dataclass
class GoogleCloud:
    """The Google Cloud settings for the Observatory Platform.

    Attributes:
        project_id: the Google Cloud project id.
        credentials: the path to the Google Cloud credentials.
        region: the Google Cloud region.
        zone: the Google Cloud zone.
        data_location: the data location for storing buckets.
    """

    project_id: str = None
    credentials: str = None
    region: str = None
    zone: str = None
    data_location: str = None

    def read_credentials(self) -> str:
        with open(self.credentials, "r") as f:
            data = f.read()
        return data

    def to_hcl(self):
        return to_hcl(
            {
                "project_id": self.project_id,
                "credentials": self.read_credentials(),
                "region": self.region,
                "zone": self.zone,
                "data_location": self.data_location,
            }
        )

    @staticmethod
    def from_dict(dict_: Dict) -> GoogleCloud:
        """Constructs a GoogleCloud instance from a dictionary.

        :param dict_: the dictionary.
        :return: the GoogleCloud instance.
        """

        project_id = dict_.get("project_id")
        credentials = dict_.get("credentials")
        region = dict_.get("region")
        zone = dict_.get("zone")
        data_location = dict_.get("data_location")

        return GoogleCloud(
            project_id=project_id, credentials=credentials, region=region, zone=zone, data_location=data_location
        )


class CloudWorkspace:
    def __init__(
        self,
        *,
        project_id: str,
        download_bucket: str,
        transform_bucket: str,
        data_location: str,
        output_project_id: Optional[str] = None,
    ):
        """The CloudWorkspace settings used by workflows.

        project_id: the Google Cloud project id. input_project_id is an alias for project_id.
        download_bucket: the Google Cloud Storage bucket where downloads will be stored.
        transform_bucket: the Google Cloud Storage bucket where transformed data will be stored.
        data_location: the data location for storing information, e.g. where BigQuery datasets should be located.
        output_project_id: an optional Google Cloud project id when the outputs of a workflow should be stored in a
        different project to the inputs. If an output_project_id is not supplied, the project_id will be used.
        """

        self._project_id = project_id
        self._download_bucket = download_bucket
        self._transform_bucket = transform_bucket
        self._data_location = data_location
        self._output_project_id = output_project_id

    @property
    def project_id(self) -> str:
        return self._project_id

    @project_id.setter
    def project_id(self, project_id: str):
        self._project_id = project_id

    @property
    def download_bucket(self) -> str:
        return self._download_bucket

    @download_bucket.setter
    def download_bucket(self, download_bucket: str):
        self._download_bucket = download_bucket

    @property
    def transform_bucket(self) -> str:
        return self._transform_bucket

    @transform_bucket.setter
    def transform_bucket(self, transform_bucket: str):
        self._transform_bucket = transform_bucket

    @property
    def data_location(self) -> str:
        return self._data_location

    @data_location.setter
    def data_location(self, data_location: str):
        self._data_location = data_location

    @property
    def input_project_id(self) -> str:
        return self._project_id

    @input_project_id.setter
    def input_project_id(self, project_id: str):
        self._project_id = project_id

    @property
    def output_project_id(self) -> Optional[str]:
        if self._output_project_id is None:
            return self._project_id
        return self._output_project_id

    @output_project_id.setter
    def output_project_id(self, output_project_id: Optional[str]):
        self._output_project_id = output_project_id

    @staticmethod
    def from_dict(dict_: Dict) -> CloudWorkspace:
        """Constructs a CloudWorkspace instance from a dictionary.

        :param dict_: the dictionary.
        :return: the Workflow instance.
        """

        project_id = dict_.get("project_id")
        download_bucket = dict_.get("download_bucket")
        transform_bucket = dict_.get("transform_bucket")
        data_location = dict_.get("data_location")
        output_project_id = dict_.get("output_project_id")

        return CloudWorkspace(
            project_id=project_id,
            download_bucket=download_bucket,
            transform_bucket=transform_bucket,
            data_location=data_location,
            output_project_id=output_project_id,
        )

    def to_dict(self) -> Dict:
        """CloudWorkspace instance to dictionary.

        :return: the dictionary.
        """

        return dict(
            project_id=self._project_id,
            download_bucket=self._download_bucket,
            transform_bucket=self._transform_bucket,
            data_location=self._data_location,
            output_project_id=self.output_project_id,
        )

    @staticmethod
    def parse_cloud_workspaces(list: List) -> List[CloudWorkspace]:
        """Parse the cloud workspaces list object into a list of CloudWorkspace instances.

        :param list: the list.
        :return: a list of CloudWorkspace instances.
        """

        return [CloudWorkspace.from_dict(dict_) for dict_ in list]


@dataclass
class Workflow:
    """A Workflow configuration.

    Attributes:
        dag_id: the Airflow DAG identifier for the workflow.
        name: a user-friendly name for the workflow.
        class_name: the fully qualified class name for the workflow class.
        cloud_workspace: the Cloud Workspace to use when running the workflow.
        kwargs: a dictionary containing optional keyword arguments that are injected into the workflow constructor.
    """

    dag_id: str = None
    name: str = None
    class_name: str = None
    cloud_workspace: CloudWorkspace = None
    kwargs: Optional[Dict] = field(default_factory=lambda: dict())

    def to_dict(self) -> Dict:
        """Workflow instance to dictionary.

        :return: the dictionary.
        """

        cloud_workspace = self.cloud_workspace
        if self.cloud_workspace is not None:
            cloud_workspace = self.cloud_workspace.to_dict()

        return dict(
            dag_id=self.dag_id,
            name=self.name,
            class_name=self.class_name,
            cloud_workspace=cloud_workspace,
            kwargs=self.kwargs,
        )

    @staticmethod
    def from_dict(dict_: Dict) -> Workflow:
        """Constructs a Workflow instance from a dictionary.

        :param dict_: the dictionary.
        :return: the Workflow instance.
        """

        dag_id = dict_.get("dag_id")
        name = dict_.get("name")
        class_name = dict_.get("class_name")

        cloud_workspace = dict_.get("cloud_workspace")
        if cloud_workspace is not None:
            cloud_workspace = CloudWorkspace.from_dict(cloud_workspace)

        kwargs = dict_.get("kwargs", dict())

        return Workflow(dag_id, name, class_name, cloud_workspace, kwargs)

    @staticmethod
    def parse_workflows(list: List) -> List[Workflow]:
        """Parse the workflows list object into a list of Workflow instances.

        :param list: the list.
        :return: a list of Workflow instances.
        """

        return [Workflow.from_dict(dict_) for dict_ in list]


def parse_dict_to_list(dict_: Dict, cls: ClassVar) -> List[Any]:
    """Parse the key, value pairs in a dictionary into a list of class instances.

    :param dict_: the dictionary.
    :param cls: the type of class to construct.
    :return: a list of class instances.
    """

    parsed_items = []
    for key, val in dict_.items():
        parsed_items.append(cls(key, val))
    return parsed_items


@dataclass
class WorkflowsProject:
    """Represents a project that contains DAGs to load.

    Attributes:
        :param package_name: the package name.
        :param package: the observatory platform package, either a local path to a Python source package (editable type),
        path to a sdist (sdist) or a PyPI package name and version (pypi).
        :param package_type: the package type, editable, sdist, pypi.
        dags_module: the Python import path to the module that contains the Apache Airflow DAGs to load.
    """

    package_name: str
    package: str
    package_type: str
    dags_module: str

    @staticmethod
    def parse_workflows_projects(list: List) -> List[WorkflowsProject]:
        """Parse the workflows_projects list object into a list of WorkflowsProject instances.

        :param list: the list.
        :return: a list of WorkflowsProject instances.
        """

        parsed_items = []
        for item in list:
            package_name = item["package_name"]
            package = item["package"]
            package_type = item["package_type"]
            dags_module = item["dags_module"]
            parsed_items.append(WorkflowsProject(package_name, package, package_type, dags_module))
        return parsed_items


@dataclass
class Terraform:
    """The Terraform settings for the Observatory Platform.

    Attributes:
        organization: the Terraform Organisation name.
    """

    organization: str

    @staticmethod
    def from_dict(dict_: Dict) -> Terraform:
        """Constructs a Terraform instance from a dictionary.

        :param dict_: the dictionary.
        """

        organization = dict_.get("organization")
        return Terraform(organization)


@dataclass
class CloudSqlDatabase:
    """The Google Cloud SQL database settings for the Observatory Platform.

    Attributes:
        tier: the database machine tier.
        backup_start_time: the start time for backups in HH:MM format.
    """

    tier: str
    backup_start_time: str

    def to_hcl(self):
        return to_hcl({"tier": self.tier, "backup_start_time": self.backup_start_time})

    @staticmethod
    def from_dict(dict_: Dict) -> CloudSqlDatabase:
        """Constructs a CloudSqlDatabase instance from a dictionary.

        :param dict_: the dictionary.
        :return: the CloudSqlDatabase instance.
        """

        tier = dict_.get("tier")
        backup_start_time = dict_.get("backup_start_time")
        return CloudSqlDatabase(tier, backup_start_time)


@dataclass
class VirtualMachine:
    """A Google Cloud virtual machine.

    Attributes:
        machine_type: the type of Google Cloud virtual machine.
        disk_size: the size of the disk in GB.
        disk_type: the disk type; pd-standard or pd-ssd.
        create: whether to create the VM or not.
    """

    machine_type: str
    disk_size: int
    disk_type: str
    create: bool

    def to_hcl(self):
        return to_hcl(
            {
                "machine_type": self.machine_type,
                "disk_size": self.disk_size,
                "disk_type": self.disk_type,
                "create": self.create,
            }
        )

    @staticmethod
    def from_hcl(string: str) -> VirtualMachine:
        return VirtualMachine.from_dict(from_hcl(string))

    @staticmethod
    def from_dict(dict_: Dict) -> VirtualMachine:
        """Constructs a VirtualMachine instance from a dictionary.

        :param dict_: the dictionary.
        :return: the VirtualMachine instance.
        """

        machine_type = dict_.get("machine_type")
        disk_size = dict_.get("disk_size")
        disk_type = dict_.get("disk_type")
        create = str(dict_.get("create")).lower() == "true"
        return VirtualMachine(machine_type, disk_size, disk_type, create)


def is_base64(text: bytes) -> bool:
    """Check if the string is base64.
    :param text: Text to check.
    :return: Whether it is base64.
    """

    try:
        base64.decodebytes(text)
    except:
        return False

    return True


def is_secret_key(key: str) -> Tuple[bool, Union[str, None]]:
    """Check if the Airflow Flask webserver secret key is valid.
    :param key: Key to check.
    :return: Validity, and an error message if not valid.
    """

    key_bytes = bytes(key, "utf-8")
    message = None

    key_length = len(key_bytes)
    if key_length < 16:
        message = f"Secret key should be length >=16, but is length {key_length}."
        return False, message

    return True, message


def is_fernet_key(key: str) -> Tuple[bool, Union[str, None]]:
    """Check if the Fernet key is valid.
    :param key: Key to check.
    :return: Validity, and an error message if not valid.
    """

    key_bytes = bytes(key, "utf-8")

    try:
        decoded_key = base64.urlsafe_b64decode(key_bytes)
    except:
        message = f"Key {key} could not be urlsafe b64decoded."
        return False, message

    key_length = len(decoded_key)
    if key_length != 32:
        message = f"Decoded Fernet key should be length 32, but is length {key_length}."
        return False, message

    message = None
    return True, message


def check_schema_field_fernet_key(field: str, value: str, error: Callable):
    """
    :param field: Field name.
    :param value: Field value.
    :param error: Error handler passed in by Cerberus.
    """

    valid, message = is_fernet_key(value)

    if not valid:
        error(field, f"is not a valid Fernet key. Reason: {message}")


def check_schema_field_secret_key(field: str, value: str, error: Callable):
    """
    :param field: Field name.
    :param value: Field value.
    :param error: Error handler passed in by Cerberus.
    """

    valid, message = is_secret_key(value)

    if not valid:
        error(field, f"is not a valid secret key. Reason: {message}")


def customise_pointer(field, value, error):
    """Throw an error when a field contains the value ' <--' which means that the user should customise the
    value in the config file.

    :param field: the field.
    :param value: the value.
    :param error: ?
    :return: None.
    """

    if isinstance(value, str) and value.endswith(" <--"):
        error(field, "Customise value ending with ' <--'")


class ObservatoryConfigValidator(Validator):
    """Custom config Validator"""

    def _validate_google_application_credentials(self, google_application_credentials, field, value):
        """Validate that the Google Application Credentials file exists.
        The rule's arguments are validated against this schema: {'type': 'boolean'}
        """
        if (
            google_application_credentials
            and value is not None
            and isinstance(value, str)
            and not os.path.isfile(value)
        ):
            self._error(
                field,
                f"the file {value} does not exist. See "
                f"https://cloud.google.com/docs/authentication/getting-started for instructions on "
                f"how to create a service account and save the JSON key to your workstation.",
            )


@dataclass
class ValidationError:
    """A validation error found when parsing a config file.

    Attributes:
        key: the key in the config file.
        value: the error.
    """

    key: str
    value: Any


class ObservatoryConfig:
    def __init__(
        self,
        backend: Backend = None,
        observatory: Observatory = None,
        google_cloud: GoogleCloud = None,
        terraform: Terraform = None,
        cloud_workspaces: List[CloudWorkspace] = None,
        workflows: List[Workflow] = None,
        workflows_projects: List[WorkflowsProject] = None,
        validator: ObservatoryConfigValidator = None,
    ):
        """Create an ObservatoryConfig instance.

        :param backend: the backend config.
        :param observatory: the Observatory config.
        :param google_cloud: the Google Cloud config.
        :param terraform: the Terraform config.
        :param cloud_workspaces: the CloudWorkspaces.
        :param workflows: the workflows to create in Airflow.
        :param workflows_projects: a list of DAGs projects.
        :param validator: an ObservatoryConfigValidator instance.
        """

        self.backend = backend if backend is not None else Backend()
        self.observatory = observatory if observatory is not None else Observatory()
        self.google_cloud = google_cloud
        self.terraform = terraform

        self.cloud_workspaces = cloud_workspaces
        if cloud_workspaces is None:
            self.cloud_workspaces = []

        self.workflows = workflows
        if workflows is None:
            self.workflows = []

        self.workflows_projects = workflows_projects
        if workflows_projects is None:
            self.workflows_projects = []

        self.validator = validator

        self.schema = make_schema(self.backend.type)

    @property
    def is_valid(self) -> bool:
        """Checks whether the config is valid or not.

        :return: whether the config is valid or not.
        """

        return self.validator is None or not len(self.validator._errors)

    @property
    def errors(self) -> List[ValidationError]:
        """Returns a list of ValidationError instances that were created when parsing the config file.

        :return: the list of ValidationError instances.
        """

        errors = []
        for key, values in self.validator.errors.items():
            for value in values:
                if type(value) is dict:
                    for nested_key, nested_value in value.items():
                        errors.append(ValidationError(f"{key}.{nested_key}", *nested_value))
                else:
                    errors.append(ValidationError(key, *values))

        return errors

    @property
    def python_packages(self) -> List[PythonPackage]:
        """Make a list of Python Packages to build or include in the observatory.
        :return: the list of Python packages.
        """

        # observatory-api should be installed first so that observatory-platform install doesn't try to install
        # observatory-api from PyPI
        packages = [
            PythonPackage(
                name="observatory-api",
                type=self.observatory.api_package_type,
                host_package=self.observatory.api_package,
                docker_package=os.path.basename(self.observatory.api_package),
            ),
            PythonPackage(
                name="observatory-platform",
                type=self.observatory.package_type,
                host_package=self.observatory.package,
                docker_package=os.path.basename(self.observatory.package),
            ),
        ]

        for project in self.workflows_projects:
            packages.append(
                PythonPackage(
                    name=project.package_name,
                    type=project.package_type,
                    host_package=project.package,
                    docker_package=os.path.basename(project.package),
                )
            )

        return packages

    @property
    def airflow_var_workflows(self) -> str:
        """Make the workflows Airflow variable.
        :return: the workflows Airflow variable.
        """

        return json.dumps([workflow.to_dict() for workflow in self.workflows])

    @property
    def airflow_var_dags_module_names(self):
        """Returns a list of DAG project module names.
        :return: the list of DAG project module names.
        """

        return json.dumps([project.dags_module for project in self.workflows_projects])

    @staticmethod
    def _parse_fields(
        dict_: Dict,
    ) -> Tuple[
        Backend,
        Observatory,
        GoogleCloud,
        Terraform,
        List[CloudWorkspace],
        List[Workflow],
        List[WorkflowsProject],
    ]:
        backend = Backend.from_dict(dict_.get("backend", dict()))
        observatory = Observatory.from_dict(dict_.get("observatory", dict()))
        google_cloud = GoogleCloud.from_dict(dict_.get("google_cloud", dict()))
        terraform = Terraform.from_dict(dict_.get("terraform", dict()))
        cloud_workspaces = CloudWorkspace.parse_cloud_workspaces(dict_.get("cloud_workspaces", list()))
        workflows = Workflow.parse_workflows(dict_.get("workflows", list()))
        workflows_projects = WorkflowsProject.parse_workflows_projects(dict_.get("workflows_projects", list()))

        return backend, observatory, google_cloud, terraform, cloud_workspaces, workflows, workflows_projects

    @classmethod
    def from_dict(cls, dict_: Dict) -> ObservatoryConfig:
        """Constructs an ObservatoryConfig instance from a dictionary.

        If the dictionary is invalid, then an ObservatoryConfig instance will be returned with no properties set,
        except for the validator, which contains validation errors.

        :param dict_: the input dictionary.
        :return: the ObservatoryConfig instance.
        """

        schema = make_schema(BackendType.local)
        validator = ObservatoryConfigValidator()
        is_valid = validator.validate(dict_, schema)

        if is_valid:
            (
                backend,
                observatory,
                google_cloud,
                terraform,
                cloud_workspaces,
                workflows,
                workflows_projects,
            ) = ObservatoryConfig._parse_fields(dict_)

            return ObservatoryConfig(
                backend,
                observatory,
                google_cloud=google_cloud,
                terraform=terraform,
                cloud_workspaces=cloud_workspaces,
                workflows=workflows,
                workflows_projects=workflows_projects,
                validator=validator,
            )
        else:
            return ObservatoryConfig(validator=validator)

    @classmethod
    def load(cls, path: str):
        """Load a configuration file.

        :return: the ObservatoryConfig instance (or a subclass of ObservatoryConfig)
        """

        dict_ = dict()

        try:
            with open(path, "r") as f:
                dict_ = yaml.safe_load(f)
        except yaml.YAMLError:
            print(f"Error parsing {path}")
        except FileNotFoundError:
            print(f"No such file or directory: {path}")
        except cerberus.validator.DocumentError as e:
            print(f"cerberus.validator.DocumentError: {e}")

        return cls.from_dict(dict_)

    def get_requirement_string(self, section: str) -> str:
        """Query the schema to see whether a section is required.

        :param section: Section to query.
        :return: String indicating whether the section is required or optional.
        """

        if self.schema[section]["required"]:
            return "Required"

        return "Optional"

    def save(self, path: str):
        """Save the observatory configuration parameters to a config file.

        :param path: Configuration file path.
        """

        with open(path, "w") as f:
            self.save_backend(f)
            self.save_observatory(f)
            self.save_terraform(f)
            self.save_google_cloud(f)
            self.save_workflows_projects(f)

    def save_backend(self, f: TextIO):
        """Write the backend configuration section to the config file.

        :param f: File object for the config file.
        """

        requirement = self.get_requirement_string("backend")
        f.write(
            (
                f"# [{requirement}] Backend settings.\n"
                "# Backend options are: local, terraform.\n"
                "# Environment options are: develop, staging, production.\n"
            )
        )
        lines = ObserveratoryConfigString.backend(self.backend)
        f.writelines(lines)
        f.write("\n")

    def save_observatory(self, f: TextIO):
        """Write the Observatory configuration section to the config file.

        :param f: File object for the config file.
        """

        requirement = self.get_requirement_string("observatory")
        f.write(
            (
                f"# [{requirement}] Observatory settings\n"
                "# If you did not supply your own Fernet and secret keys, then those fields are autogenerated.\n"
                "# Passwords are in plaintext.\n"
                "# If the package type is editable, the 'package' should be the path location of your package\n"
                "# If the package type is PyPi, the 'package' should be its name on PyPi\n"
                "# observatory_home is where the observatory metadata is stored.\n"
            )
        )

        lines = ObserveratoryConfigString.observatory(self.observatory)
        f.writelines(lines)
        f.write("\n")

    def save_terraform(self, f: TextIO):
        """Write the Terraform configuration section to the config file.

        :param f: File object for the config file.
        """

        requirement = self.get_requirement_string("terraform")
        f.write(f"# [{requirement}] Terraform settings\n")

        lines = ObserveratoryConfigString.terraform(self.terraform)
        output = map(comment, lines) if self.terraform is None and requirement == "Optional" else lines

        f.writelines(output)
        f.write("\n")

    def save_google_cloud(self, f: TextIO):
        """Write the Google Cloud configuration section to the config file.

        :param f: File object for the config file.
        """

        requirement = self.get_requirement_string("google_cloud")
        f.write(
            (
                f"# [{requirement}] Google Cloud settings\n"
                "# If you use any Google Cloud service functions, you will need to configure this.\n"
            )
        )

        lines = ObserveratoryConfigString.google_cloud(google_cloud=self.google_cloud, backend=self.backend)
        output = map(comment, lines) if self.google_cloud is None and requirement == "Optional" else lines

        f.writelines(output)
        f.write("\n")

    def save_workflows_projects(self, f: TextIO):
        """Write the DAGs projects configuration section to the config file.

        :param f: File object for the config file.
        """

        requirement = self.get_requirement_string("workflows_projects")
        f.write(f"# [{requirement}] User defined Observatory DAGs projects:\n")

        lines = ObserveratoryConfigString.workflows_projects(workflows_projects=self.workflows_projects)
        output = map(comment, lines) if len(self.workflows_projects) == 0 and requirement == "Optional" else lines

        f.writelines(output)
        f.write("\n")


class TerraformConfig(ObservatoryConfig):
    WORKSPACE_PREFIX = "observatory-"

    def __init__(
        self,
        backend: Backend = None,
        observatory: Observatory = None,
        google_cloud: GoogleCloud = None,
        terraform: Terraform = None,
        cloud_workspaces: List[CloudWorkspace] = None,
        workflows: List[Workflow] = None,
        workflows_projects: List[WorkflowsProject] = None,
        cloud_sql_database: CloudSqlDatabase = None,
        airflow_main_vm: VirtualMachine = None,
        airflow_worker_vm: VirtualMachine = None,
        validator: ObservatoryConfigValidator = None,
    ):
        """Create a TerraformConfig instance.

        :param backend: the backend config.
        :param observatory: the Observatory config.
        :param google_cloud: the Google Cloud config.
        :param terraform: the Terraform config.
        :param cloud_workspaces: the CloudWorkspaces.
        :param workflows: the workflows to create in Airflow.
        :param workflows_projects: a list of DAGs projects.
        :param cloud_sql_database: a Google Cloud SQL database config.
        :param airflow_main_vm: the Airflow Main VM config.
        :param airflow_worker_vm: the Airflow Worker VM config.
        :param validator: an ObservatoryConfigValidator instance.
        """

        if backend is None:
            backend = Backend(type=BackendType.terraform)

        super().__init__(
            backend=backend,
            observatory=observatory,
            google_cloud=google_cloud,
            terraform=terraform,
            cloud_workspaces=cloud_workspaces,
            workflows=workflows,
            workflows_projects=workflows_projects,
            validator=validator,
        )
        self.cloud_sql_database = cloud_sql_database
        self.airflow_main_vm = airflow_main_vm
        self.airflow_worker_vm = airflow_worker_vm

    @property
    def terraform_workspace_id(self):
        """The Terraform workspace id.

        :return: the terraform workspace id.
        """

        return TerraformConfig.WORKSPACE_PREFIX + self.backend.environment.value

    def terraform_variables(self) -> List[TerraformVariable]:
        """Create a list of TerraformVariable instances from the Terraform Config.

        :return: a list of TerraformVariable instances.
        """

        return [
            TerraformVariable("environment", self.backend.environment.value),
            TerraformVariable("observatory", self.observatory.to_hcl(), sensitive=True, hcl=True),
            TerraformVariable("google_cloud", self.google_cloud.to_hcl(), sensitive=True, hcl=True),
            TerraformVariable("cloud_sql_database", self.cloud_sql_database.to_hcl(), hcl=True),
            TerraformVariable("airflow_main_vm", self.airflow_main_vm.to_hcl(), hcl=True),
            TerraformVariable("airflow_worker_vm", self.airflow_worker_vm.to_hcl(), hcl=True),
            TerraformVariable("airflow_var_workflows", self.airflow_var_workflows),
            TerraformVariable("airflow_var_dags_module_names", self.airflow_var_dags_module_names),
        ]

    @classmethod
    def from_dict(cls, dict_: Dict) -> TerraformConfig:
        """Make an TerraformConfig instance from a dictionary.

        If the dictionary is invalid, then an ObservatoryConfig instance will be returned with no properties set,
        except for the validator, which contains validation errors.

        :param dict_: the input dictionary that has been read via yaml.safe_load.
        :return: the TerraformConfig instance.
        """

        schema = make_schema(BackendType.terraform)
        validator = ObservatoryConfigValidator()
        is_valid = validator.validate(dict_, schema)

        if is_valid:
            (
                backend,
                observatory,
                google_cloud,
                terraform,
                cloud_workspaces,
                workflows,
                workflows_projects,
            ) = ObservatoryConfig._parse_fields(dict_)

            cloud_sql_database = CloudSqlDatabase.from_dict(dict_.get("cloud_sql_database", dict()))
            airflow_main_vm = VirtualMachine.from_dict(dict_.get("airflow_main_vm", dict()))
            airflow_worker_vm = VirtualMachine.from_dict(dict_.get("airflow_worker_vm", dict()))

            return TerraformConfig(
                backend,
                observatory,
                google_cloud=google_cloud,
                terraform=terraform,
                cloud_workspaces=cloud_workspaces,
                workflows=workflows,
                workflows_projects=workflows_projects,
                cloud_sql_database=cloud_sql_database,
                airflow_main_vm=airflow_main_vm,
                airflow_worker_vm=airflow_worker_vm,
                validator=validator,
            )
        else:
            return TerraformConfig(validator=validator)

    def save(self, path: str):
        """Save the configuration to a config file in YAML format.

        :param path: Config file path.
        """

        # Save common config
        super().save(path)

        # Save Terraform specific sections
        with open(path, "a") as f:
            self.save_cloud_sql_database(f)
            self.save_airflow_main_vm(f)
            self.save_airflow_worker_vm(f)

    def save_cloud_sql_database(self, f: TextIO):
        """Write the cloud SQL database configuration section to the config file.

        :param f: File object for the config file.
        """

        requirement = self.get_requirement_string("cloud_sql_database")
        f.write(f"# [{requirement}] Google Cloud CloudSQL database settings\n")
        lines = ObserveratoryConfigString.cloud_sql_database(self.cloud_sql_database)
        f.writelines(lines)
        f.write("\n")

    def save_airflow_main_vm(self, f: TextIO):
        """Write the Airflow main VM configuration section to the config file.

        :param f: File object for the config file.
        """

        requirement = self.get_requirement_string("airflow_main_vm")
        f.write(f"# [{requirement}] Settings for the main VM that runs the Airflow cheduler and webserver\n")
        lines = ObserveratoryConfigString.airflow_main_vm(self.airflow_main_vm)
        f.writelines(lines)
        f.write("\n")

    def save_airflow_worker_vm(self, f: TextIO):
        """Write the Airflow worker VM configuration section to the config file.

        :param f: File object for the config file.
        """

        requirement = self.get_requirement_string("airflow_worker_vm")
        f.write(f"# [{requirement}] Settings for the weekly on-demand VM that runs arge tasks\n")
        lines = ObserveratoryConfigString.airflow_worker_vm(self.airflow_worker_vm)
        f.writelines(lines)
        f.write("\n")


Config = Union[ObservatoryConfig, TerraformConfig]


def make_schema(backend_type: BackendType) -> Dict:
    """Make a schema for an Observatory or Terraform config file.

    :param backend_type: the type of backend, local or terraform.
    :return: a dictionary containing the schema.
    """

    schema = dict()
    is_backend_terraform = backend_type == BackendType.terraform

    # Backend settings
    schema["backend"] = {
        "required": True,
        "type": "dict",
        "schema": {
            "type": {"required": True, "type": "string", "allowed": [backend_type.value]},
            "environment": {"required": True, "type": "string", "allowed": ["develop", "staging", "production"]},
        },
    }

    # Terraform settings
    schema["terraform"] = {
        "required": is_backend_terraform,
        "type": "dict",
        "schema": {"organization": {"required": True, "type": "string", "check_with": customise_pointer}},
    }

    # Google Cloud settings
    schema["google_cloud"] = {
        "required": is_backend_terraform,
        "type": "dict",
        "schema": {
            "project_id": {"required": is_backend_terraform, "type": "string", "check_with": customise_pointer},
            "credentials": {
                "required": is_backend_terraform,
                "type": "string",
                "check_with": customise_pointer,
                "google_application_credentials": True,
            },
            "region": {
                "required": is_backend_terraform,
                "type": "string",
                "regex": r"^\w+\-\w+\d+$",
                "check_with": customise_pointer,
            },
            "zone": {
                "required": is_backend_terraform,
                "type": "string",
                "regex": r"^\w+\-\w+\d+\-[a-z]{1}$",
                "check_with": customise_pointer,
            },
            "data_location": {"required": is_backend_terraform, "type": "string", "check_with": customise_pointer},
        },
    }

    # Observatory settings
    package_types = ["editable", "sdist", "pypi"]
    schema["observatory"] = {
        "required": True,
        "type": "dict",
        "schema": {
            "package": {"required": True, "type": "string"},
            "package_type": {"required": True, "type": "string", "allowed": package_types},
            "airflow_fernet_key": {"required": True, "type": "string", "check_with": check_schema_field_fernet_key},
            "airflow_secret_key": {"required": True, "type": "string", "check_with": check_schema_field_secret_key},
            "airflow_ui_user_password": {"required": is_backend_terraform, "type": "string"},
            "airflow_ui_user_email": {"required": is_backend_terraform, "type": "string"},
            "observatory_home": {"required": False, "type": "string"},
            "postgres_password": {"required": is_backend_terraform, "type": "string"},
            "redis_port": {"required": False, "type": "integer"},
            "flower_ui_port": {"required": False, "type": "integer"},
            "airflow_ui_port": {"required": False, "type": "integer"},
            "docker_network_name": {"required": False, "type": "string"},
            "docker_network_is_external": {"required": False, "type": "boolean"},
            "docker_compose_project_name": {"required": False, "type": "string"},
            "api_package": {"required": False, "type": "string"},
            "api_package_type": {"required": False, "type": "string", "allowed": package_types},
            "api_port": {"required": False, "type": "integer"},
        },
    }

    # Database settings
    if is_backend_terraform:
        schema["cloud_sql_database"] = {
            "required": True,
            "type": "dict",
            "schema": {
                "tier": {"required": True, "type": "string"},
                "backup_start_time": {"required": True, "type": "string", "regex": r"^\d{2}:\d{2}$"},
            },
        }

    # VM schema
    vm_schema = {
        "required": True,
        "type": "dict",
        "schema": {
            "machine_type": {
                "required": True,
                "type": "string",
            },
            "disk_size": {"required": True, "type": "integer", "min": 1},
            "disk_type": {"required": True, "type": "string", "allowed": ["pd-standard", "pd-ssd"]},
            "create": {"required": True, "type": "boolean"},
        },
    }

    # Airflow main and worker VM
    if is_backend_terraform:
        schema["airflow_main_vm"] = vm_schema
        schema["airflow_worker_vm"] = vm_schema

    # Workflow configuration
    cloud_workspace_schema = {
        "project_id": {"required": True, "type": "string"},
        "download_bucket": {"required": True, "type": "string"},
        "transform_bucket": {"required": True, "type": "string"},
        "data_location": {"required": True, "type": "string"},
        "output_project_id": {"required": False, "type": "string"},
    }

    schema["cloud_workspaces"] = {
        "required": False,
        "type": "list",
        "schema": {
            "type": "dict",
            "schema": {"workspace": {"required": True, "type": "dict", "schema": cloud_workspace_schema}},
        },
    }

    schema["workflows"] = {
        "required": False,
        "dependencies": "cloud_workspaces",  # cloud_workspaces must be specified when workflows are defined
        "type": "list",
        "schema": {
            "type": "dict",
            "schema": {
                "dag_id": {"required": True, "type": "string"},
                "name": {"required": True, "type": "string"},
                "class_name": {"required": True, "type": "string"},
                "cloud_workspace": {"required": False, "type": "dict", "schema": cloud_workspace_schema},
                "kwargs": {"required": False, "type": "dict"},
            },
        },
    }

    schema["workflows_projects"] = {
        "required": False,
        "type": "list",
        "schema": {
            "type": "dict",
            "schema": {
                "package_name": {
                    "required": True,
                    "type": "string",
                },
                "package": {"required": True, "type": "string"},
                "package_type": {"required": True, "type": "string", "allowed": package_types},
                "dags_module": {
                    "required": True,
                    "type": "string",
                },
            },
        },
    }

    return schema


class ObserveratoryConfigString:
    """This class contains methods to construct config file sections."""

    @staticmethod
    def backend(backend: Backend) -> List[str]:
        """Constructs the backend section string.

        :param backend: Backend configuration object.
        :return: List of strings for the section, including the section heading."
        """

        lines = [
            "backend:\n",
            indent(f"type: {backend.type.name}\n", INDENT1),
            indent(f"environment: {backend.environment.name}\n", INDENT1),
        ]

        return lines

    @staticmethod
    def observatory(observatory: Observatory) -> List[str]:
        """Constructs the observatory section string.

        :param observatory: Observatory configuration object.
        :return: List of strings for the section, including the section heading."
        """

        lines = [
            "observatory:\n",
            indent(f"package: {observatory.package}\n", INDENT1),
            indent(f"package_type: {observatory.package_type}\n", INDENT1),
            indent(f"airflow_fernet_key: {observatory.airflow_fernet_key}\n", INDENT1),
            indent(f"airflow_secret_key: {observatory.airflow_secret_key}\n", INDENT1),
            indent(f"airflow_ui_user_email: {observatory.airflow_ui_user_email}\n", INDENT1),
            indent(f"airflow_ui_user_password: {observatory.airflow_ui_user_password}\n", INDENT1),
            indent(f"observatory_home: {observatory.observatory_home}\n", INDENT1),
            indent(f"postgres_password: {observatory.postgres_password}\n", INDENT1),
            indent(f"redis_port: {observatory.redis_port}\n", INDENT1),
            indent(f"flower_ui_port: {observatory.flower_ui_port}\n", INDENT1),
            indent(f"airflow_ui_port: {observatory.airflow_ui_port}\n", INDENT1),
            indent(f"docker_network_name: {observatory.docker_network_name}\n", INDENT1),
            indent(f"docker_network_is_external: {observatory.docker_network_is_external}\n", INDENT1),
            indent(f"docker_compose_project_name: {observatory.docker_compose_project_name}\n", INDENT1),
            indent(f"api_package: {observatory.api_package}\n", INDENT1),
            indent(f"api_package_type: {observatory.api_package_type}\n", INDENT1),
            indent(f"api_port: {observatory.api_port}\n", INDENT1),
        ]

        return lines

    @staticmethod
    def terraform(terraform: Terraform) -> List[str]:
        """Constructs the terraform section string.

        :param observatory: Terraform configuration object.
        :return: List of strings for the section, including the section heading."
        """

        if terraform is None:
            terraform = Terraform(organization="my-terraform-org-name")

        lines = [
            "terraform:\n",
            indent(f"organization: {terraform.organization}\n", INDENT1),
        ]

        return lines

    @staticmethod
    def google_cloud(*, google_cloud: GoogleCloud, backend: Backend) -> List[str]:
        """Constructs the Google Cloud section string.

        :param google_cloud: Google Cloud configuration object.
        :param backend: Backend configuration object.
        :return: List of strings for the section, including the section heading."
        """

        if google_cloud is None:
            google_cloud = GoogleCloud(
                project_id="my-gcp-id",
                credentials="/path/to/credentials.json",
                data_location="us",
                region="us-west1",
                zone="us-west1-a",
            )

        lines = [
            "google_cloud:\n",
            indent(f"project_id: {google_cloud.project_id}\n", INDENT1),
            indent(f"credentials: {google_cloud.credentials}\n", INDENT1),
            indent(f"data_location: {google_cloud.data_location}\n", INDENT1),
        ]

        # Is region and zone something we should be putting in the local config too?
        if backend.type == BackendType.terraform:
            lines.append(indent(f"region: {google_cloud.region}\n", INDENT1))
            lines.append(indent(f"zone: {google_cloud.zone}\n", INDENT1))

        return lines

    @staticmethod
    def workflows_projects(*, workflows_projects: List[WorkflowsProject] = None) -> List[str]:
        """Constructs the DAGs projects section string.

        :param workflows_projects: List of DAGs project configuration objects.
        :return: List of strings for the section, including the section heading."
        """

        projects = workflows_projects.copy()

        if len(projects) == 0:
            projects.append(
                WorkflowsProject(
                    package_name="observatory-dags",
                    package="/path/to/dags_project",
                    package_type="editable",
                    dags_module="observatory.dags.dags",
                )
            )

        lines = ["workflows_projects:\n"]
        for project in projects:
            lines.append(indent(f"- package_name: {project.package_name}\n", INDENT1))
            lines.append(indent(f"package: {project.package}\n", INDENT3))
            lines.append(indent(f"package_type: {project.package_type}\n", INDENT3))
            lines.append(indent(f"dags_module: {project.dags_module}\n", INDENT3))

        return lines

    @staticmethod
    def cloud_sql_database(cloud_sql_database: CloudSqlDatabase) -> List[str]:
        """Constructs the cloud SQL database section string.

        :param cloud_sql_database: Cloud SQL configuration object.
        :return: List of strings for the section, including the section heading."
        """

        if cloud_sql_database is None:
            cloud_sql_database = CloudSqlDatabase(
                tier="db-custom-2-7680",
                backup_start_time="23:00",
            )

        lines = [
            "cloud_sql_database:\n",
            indent(f"tier: {cloud_sql_database.tier}\n", INDENT1),
            indent(f"backup_start_time: '{cloud_sql_database.backup_start_time}'\n", INDENT1),
        ]

        return lines

    @staticmethod
    def airflow_vm_lines_(*, vm: VirtualMachine, vm_type) -> List[str]:
        """Constructs the virtual machine section string.

        :param vm: Virtual machine configuration object.
        :param vm_type: Type of vm being configured.
        :return: List of strings for the section, including the section heading."
        """
        lines = [
            f"{vm_type}:\n",
            indent(f"machine_type: {vm.machine_type}\n", INDENT1),
            indent(f"disk_size: {vm.disk_size}\n", INDENT1),
            indent(f"disk_type: {vm.disk_type}\n", INDENT1),
            indent(f"create: {vm.create}\n", INDENT1),
        ]

        return lines

    @staticmethod
    def airflow_main_vm(vm: VirtualMachine) -> List[str]:
        """Constructs the main virtual machine section string.

        :param vm: Virtual machine configuration object.
        :return: List of strings for the section, including the section heading."
        """

        if vm is None:
            vm = VirtualMachine(
                machine_type="n2-standard-2",
                disk_size=50,
                disk_type="pd-ssd",
                create=True,
            )

        lines = ObserveratoryConfigString.airflow_vm_lines_(vm=vm, vm_type="airflow_main_vm")
        return lines

    @staticmethod
    def airflow_worker_vm(vm: VirtualMachine) -> List[str]:
        """Constructs the worker virtual machine section string.

        :param vm: Virtual machine configuration object.
        :return: List of strings for the section, including the section heading."
        """

        if vm is None:
            vm = VirtualMachine(
                machine_type="n1-standard-8",
                disk_size=3000,
                disk_type="pd-standard",
                create=False,
            )

        lines = ObserveratoryConfigString.airflow_vm_lines_(vm=vm, vm_type="airflow_worker_vm")
        return lines
