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
import re
from dataclasses import dataclass, field, fields
from enum import Enum
from typing import Any, Callable, ClassVar, Dict, List, Tuple, Union, Optional

import cerberus.validator
import yaml
from abc import ABC, abstractmethod

from cerberus import Validator
from cryptography.fernet import Fernet
from observatory.platform.cli.click_utils import (
    INDENT1,
    INDENT2,
    INDENT3,
    comment,
    indent,
)
from observatory.platform.terraform_api import TerraformVariable
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.config_utils import (
    observatory_home as default_observatory_home,
)


def generate_fernet_key() -> str:
    """Generate a Fernet key.

    :return: A newly generated Fernet key.
    """

    return str(Fernet.generate_key().decode("utf8"))


def generate_secret_key(length: int = 30) -> str:
    """Generate a secret key for the Flask Airflow Webserver.

    :param length: the length of the key to generate.
    :return: the random key.
    """

    return str(binascii.b2a_hex(os.urandom(length)).decode("utf8"))


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


class BackendType(Enum):
    """The type of backend"""

    local = "local"
    terraform = "terraform"
    terraform_api = "terraform-api"


class Environment(Enum):
    """The environment being used"""

    develop = "develop"
    staging = "staging"
    production = "production"


class ApiTypes(Enum):
    """The API types"""

    data_api = "data_api"
    observatory_api = "observatory_api"


class ConfigSection(ABC):
    @abstractmethod
    def to_string(self, requirement: str, comment_out: bool, backend_type: BackendType) -> List[str]:
        pass

    @staticmethod
    @abstractmethod
    def from_dict(dict_: Dict):
        pass

    @staticmethod
    def get_lines(
        section: Optional[ConfigSection],
        default: ConfigSection,
        config: [ObservatoryConfig, TerraformConfig, TerraformAPIConfig],
    ) -> List[str]:
        set_default = False
        if section is None:
            section = default
            set_default = True

        section_name = re.sub("(?!^)([A-Z]+)", r"_\1", section.__class__.__name__).lower()
        if config.schema[section_name]["required"]:
            requirement = "Required"
        else:
            requirement = "Optional"

        if requirement == "Optional" and set_default:
            comment_out = True
        else:
            comment_out = False

        lines = section.to_string(requirement, comment_out, config.backend.type)
        return lines + ["\n"]


@dataclass
class Backend(ConfigSection):
    """The backend settings for the Observatory Platform.

    Attributes:
        type: the type of backend being used (local environment or Terraform).
        environment: what type of environment is being deployed (develop, staging or production).
    """

    type: BackendType
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

    def to_string(self, requirement: str, comment_out: bool, backend_type: BackendType) -> List[str]:
        """Constructs the backend section string.

        :param requirement:
        :param comment_out:
        :param backend_type:
        :return: List of strings for the section, including the section heading."
        """
        description = [
            f"# [{requirement}] Backend settings.\n",
            f"# Backend options are: {BackendType.local.value}, {BackendType.terraform.value}, {BackendType.terraform_api.value}.\n",
            f"# Environment options are: {Environment.develop.value}, {Environment.staging.value}, {Environment.production.value}.\n",
        ]
        lines = [
            "backend:\n",
            indent(f"type: {self.type.value}\n", INDENT1),
            indent(f"environment: {self.environment.value}\n", INDENT1),
        ]

        return description + lines


@dataclass
class PythonPackage:
    name: str
    host_package: str
    docker_package: str
    type: str


@dataclass
class Observatory(ConfigSection):
    """The Observatory settings for the Observatory Platform.

    Attributes:
        :param package: the observatory platform package, either a local path to a Python source package
        (editable type), path to a sdist (sdist) or a PyPI package name and version (pypi).
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
        :param elastic_port: The host's Elasticsearch port number.
        :param kibana_port: The host's Kibana port number.
        :param docker_network_name: The Docker Network name, used to specify a custom Docker Network.
        :param docker_network_is_external: whether the docker network is external or not.
        :param docker_compose_project_name: The namespace for the Docker Compose containers:
        https://docs.docker.com/compose/reference/envvars/#compose_project_name.
        :param enable_elk: whether to enable the elk stack or not.
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
    elastic_port: int = 9200
    kibana_port: int = 5601
    docker_network_name: str = "observatory-network"
    docker_network_is_external: bool = False
    docker_compose_project_name: str = "observatory"
    enable_elk: bool = True

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

    def to_string(self, requirement: str, comment_out: bool, backend_type: BackendType) -> List[str]:
        description = [
            f"# [{requirement}] Observatory settings\n",
            "# If you did not supply your own Fernet and secret keys, then those fields are autogenerated.\n",
            "# Passwords are in plaintext.\n",
            "# observatory_home is where the observatory metadata is stored.\n",
        ]
        lines = ["observatory:\n"]
        for variable in fields(self):
            value = getattr(self, variable.name)
            lines.append(indent(f"{variable.name}: {value}\n", INDENT1))
        lines = map(comment, lines) if comment_out else lines
        return description + list(lines)

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
        elastic_port = dict_.get("elastic_port", Observatory.elastic_port)
        kibana_port = dict_.get("kibana_port", Observatory.kibana_port)
        docker_network_name = dict_.get("docker_network_name", Observatory.docker_network_name)
        docker_network_is_external = dict_.get("docker_network_is_external", Observatory.docker_network_is_external)
        docker_compose_project_name = dict_.get("docker_compose_project_name", Observatory.docker_compose_project_name)
        enable_elk = dict_.get("enable_elk", Observatory.enable_elk)

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
            elastic_port=elastic_port,
            kibana_port=kibana_port,
            docker_network_name=docker_network_name,
            docker_network_is_external=docker_network_is_external,
            docker_compose_project_name=docker_compose_project_name,
            enable_elk=enable_elk,
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
class GoogleCloud(ConfigSection):
    """The Google Cloud settings for the Observatory Platform.

    Attributes:
        project_id: the Google Cloud project id.
        credentials: the path to the Google Cloud credentials.
        region: the Google Cloud region.
        zone: the Google Cloud zone.
        data_location: the data location for storing buckets.
        buckets: a list of the Google Cloud buckets.
    """

    project_id: str = "my-gcp-id"
    credentials: str = "/path/to/credentials.json"
    region: Optional[str] = "us-west1"
    zone: Optional[str] = "us-west1-a"
    data_location: str = "us"
    buckets: List[CloudStorageBucket] = field(
        default_factory=lambda: [
            CloudStorageBucket(
                id="download_bucket",
                name="my-download-bucket-name",
            ),
            CloudStorageBucket(
                id="transform_bucket",
                name="my-transform-bucket-name",
            ),
        ]
    )

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
                "buckets": [bucket.name for bucket in self.buckets],
            }
        )

    def to_string(self, requirement: str, comment_out: bool, backend_type: BackendType) -> List[str]:
        description = [
            f"# [{requirement}] Google Cloud settings\n",
            "# If you use any Google Cloud service functions, you will need to configure this.\n",
        ]

        lines = [
            "google_cloud:\n",
            indent(f"project_id: {self.project_id}\n", INDENT1),
            indent(f"credentials: {self.credentials}\n", INDENT1),
            indent(f"data_location: {self.data_location}\n", INDENT1),
        ]

        if backend_type == BackendType.terraform or backend_type == BackendType.terraform_api:
            lines.append(indent(f"region: {self.region}\n", INDENT1))
            lines.append(indent(f"zone: {self.zone}\n", INDENT1))
        else:
            lines.append(indent("buckets:\n", INDENT1))
            for bucket in self.buckets:
                lines.append(indent(f"{bucket.id}: {bucket.name}\n", INDENT3))

        lines = map(comment, lines) if comment_out else lines
        return description + list(lines)

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
        buckets = CloudStorageBucket.parse_buckets(dict_.get("buckets", dict()))

        return GoogleCloud(
            project_id=project_id,
            credentials=credentials,
            region=region,
            zone=zone,
            data_location=data_location,
            buckets=buckets,
        )


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
        :param package: the observatory platform package, either a local path to a Python source package
        (editable type), path to a sdist (sdist) or a PyPI package name and version (pypi).
        :param package_type: the package type, editable, sdist, pypi.
        dags_module: the Python import path to the module that contains the Apache Airflow DAGs to load.
    """

    package_name: str = "observatory-dags"
    package: str = "/path/to/dags_project"
    package_type: str = "editable"
    dags_module: str = "observatory.dags.dags"


@dataclass
class WorkflowsProjects(ConfigSection):
    """A list of Workflows Projects.
    Attributes:
        workflows_projects: A list of Workflows Projects.
    """

    workflows_projects: List[WorkflowsProject] = field(default_factory=lambda: [WorkflowsProject()])

    def to_string(self, requirement: str, comment_out: bool, backend_type: BackendType) -> List:
        description = [f"# [{requirement}] User defined Observatory Workflows projects:\n"]
        lines = ["workflows_projects:\n"]
        for project in self.workflows_projects:
            line = [
                indent(f"- package_name: {project.package_name}\n", INDENT1),
                indent(f"  package: {project.package}\n", INDENT1),
                indent(f"  package_type: {project.package_type}\n", INDENT1),
                indent(f"  dags_module: {project.dags_module}\n", INDENT1),
            ]
            lines += line
        lines = map(comment, lines) if comment_out else lines
        return description + list(lines)

    @staticmethod
    def from_dict(projects: List[dict]) -> WorkflowsProjects:
        """Constructs a AirflowVariables instance from a dictionary.
        :param projects: List of projects, each project is a dictionary with info.
        """
        workflows_projects = []
        for project in projects:
            workflows_project = WorkflowsProject(
                package_name=project["package_name"],
                package=project["package"],
                package_type=project["package_type"],
                dags_module=project["dags_module"],
            )
            workflows_projects.append(workflows_project)
        return WorkflowsProjects(workflows_projects)


@dataclass
class AirflowConnection:
    """An Airflow Connection.

    Attributes:
        name: the name of the Airflow Connection.
        value: the value of the Airflow Connection.
    """

    name: str = "my_connection"
    value: str = "http://my-username:my-password@"

    @property
    def conn_name(self) -> str:
        """The Airflow Connection environment variable name, which is required to set the connection from an
        environment variable.

        :return: the Airflow Connection environment variable name.
        """

        return f"AIRFLOW_CONN_{self.name.upper()}"


@dataclass
class AirflowConnections(ConfigSection):
    """A list of Airflow Connections.
    Attributes:
        airflow_connections: A list of Airflow Connections.
    """

    airflow_connections: List[AirflowConnection] = field(default_factory=lambda: [AirflowConnection()])

    def to_string(self, requirement: str, comment_out: bool, backend_type: BackendType) -> List:
        description = [f"# [{requirement}] User defined Apache Airflow Connections:\n"]

        lines = ["airflow_connections:\n"]
        variables = self.airflow_connections.copy()
        for variable in variables:
            lines.append(indent(f"{variable.name}: {variable.value}\n", INDENT1))
        lines = map(comment, lines) if comment_out else lines

        return description + list(lines)

    @staticmethod
    def from_dict(dict_: Dict) -> AirflowConnections:
        """Parse the airflow_connections dictionary object into a list of AirflowConnection instances.
        :param dict_: the dictionary.
        """
        airflow_connections = parse_dict_to_list(dict_, AirflowConnection)
        return AirflowConnections(airflow_connections)


@dataclass
class AirflowVariable:
    """An Airflow Variable.

    Attributes:
        name: the name of the Airflow Variable.
        value: the value of the Airflow Variable.
    """

    name: str = "my_variable_name"
    value: str = "my-variable-value"

    @property
    def env_var_name(self):
        """The Airflow Variable environment variable name, which is required to set the variable from an
        environment variable.

        :return: the Airflow Variable environment variable name.
        """

        return f"AIRFLOW_VAR_{self.name.upper()}"


@dataclass
class AirflowVariables(ConfigSection):
    """A list of Airflow Variables.
    Attributes:
        airflow_variables: list of Airflow Variables
    """

    airflow_variables: List[AirflowVariable] = field(default_factory=lambda: [AirflowVariable()])

    def to_string(self, requirement: str, comment_out: bool, backend_type: BackendType) -> List[str]:
        description = [f"# [{requirement}] User defined Apache Airflow variables:\n"]

        lines = ["airflow_variables:\n"]
        variables = self.airflow_variables.copy()
        for variable in variables:
            lines.append(indent(f"{variable.name}: {variable.value}\n", INDENT1))
        lines = map(comment, lines) if comment_out else lines

        return description + list(lines)

    @staticmethod
    def from_dict(dict_: Dict) -> AirflowVariables:
        """Constructs a AirflowVariables instance from a dictionary.
        :param dict_: the dictionary.
        """

        airflow_variables = parse_dict_to_list(dict_, AirflowVariable)
        return AirflowVariables(airflow_variables)


@dataclass
class Terraform(ConfigSection):
    """The Terraform settings for the Observatory Platform.

    Attributes:
        organization: the Terraform Organisation name.
    """

    organization: Optional[str] = "my-terraform-org-name"

    @staticmethod
    def from_dict(dict_: Dict) -> Terraform:
        """Constructs a Terraform instance from a dictionary.

        :param dict_: the dictionary.
        """

        organization = dict_.get("organization")
        return Terraform(organization)

    def to_string(self, requirement: str, comment_out: bool, backend_type: BackendType):
        description = [f"# [{requirement}] Terraform settings\n"]

        lines = [
            "terraform:\n",
            indent(f"organization: {self.organization}\n", INDENT1),
        ]
        lines = map(comment, lines) if comment_out else lines
        lines = description + list(lines)

        return lines


@dataclass
class CloudSqlDatabase(ConfigSection):
    """The Google Cloud SQL database settings for the Observatory Platform.

    Attributes:
        tier: the database machine tier.
        backup_start_time: the start time for backups in HH:MM format.
    """

    tier: str = "db-custom-2-7680"
    backup_start_time: str = "23:00"

    def to_hcl(self):
        return to_hcl({"tier": self.tier, "backup_start_time": self.backup_start_time})

    def to_string(self, requirement: str, comment_out: bool, backend_type: BackendType):
        description = [f"# [{requirement}] Google Cloud CloudSQL database settings.\n"]
        lines = [
            "cloud_sql_database:\n",
            indent(f"tier: {self.tier}\n", INDENT1),
            indent(f'backup_start_time: "{self.backup_start_time}"\n', INDENT1),
        ]
        lines = map(comment, lines) if comment_out else lines
        return description + list(lines)

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
class AirflowMainVm(ConfigSection):
    """A Google Cloud main virtual machine.
    Attributes:
        machine_type: the type of Google Cloud virtual machine.
        disk_size: the size of the disk in GB.
        disk_type: the disk type; pd-standard or pd-ssd.
        create: whether to create the VM or not.
    """

    machine_type: str = "n2-standard-2"
    disk_size: int = 50
    disk_type: str = "pd-ssd"
    create: bool = True

    def to_hcl(self):
        return to_hcl(
            {
                "machine_type": self.machine_type,
                "disk_size": self.disk_size,
                "disk_type": self.disk_type,
                "create": self.create,
            }
        )

    def to_string(self, requirement: str, comment_out: bool, backend_type: BackendType):
        description = [f"# [{requirement}] Settings for the main VM that runs the Airflow scheduler and webserver.\n"]
        lines = ["airflow_main_vm:\n"]
        for variable in fields(self):
            value = getattr(self, variable.name)
            lines.append(indent(f"{variable.name}: {value}\n", INDENT1))
        lines = map(comment, lines) if comment_out else lines
        return description + list(lines)

    @staticmethod
    def from_dict(dict_: Dict) -> AirflowMainVm:
        """Constructs a VirtualMachine instance from a dictionary.
        :param dict_: the dictionary.
        :return: the VirtualMachine instance.
        """

        machine_type = dict_.get("machine_type")
        disk_size = dict_.get("disk_size")
        disk_type = dict_.get("disk_type")
        create = str(dict_.get("create")).lower() == "true"
        return AirflowMainVm(machine_type, disk_size, disk_type, create)


@dataclass
class AirflowWorkerVm(ConfigSection):
    """A Google Cloud worker virtual machine.
    Attributes:
        machine_type: the type of Google Cloud virtual machine.
        disk_size: the size of the disk in GB.
        disk_type: the disk type; pd-standard or pd-ssd.
        create: whether to create the VM or not.
    """

    machine_type: str = "n1-standard-8"
    disk_size: int = 3000
    disk_type: str = "pd-standard"
    create: bool = False

    def to_hcl(self):
        return to_hcl(
            {
                "machine_type": self.machine_type,
                "disk_size": self.disk_size,
                "disk_type": self.disk_type,
                "create": self.create,
            }
        )

    def to_string(self, requirement: str, comment_out: bool, backend_type: BackendType):
        description = [f"# [{requirement}] Settings for the weekly on-demand VM that runs larger " f"tasks.\n"]
        lines = ["airflow_worker_vm:\n"]
        for variable in fields(self):
            value = getattr(self, variable.name)
            lines.append(indent(f"{variable.name}: {value}\n", INDENT1))
        lines = map(comment, lines) if comment_out else lines
        return description + list(lines)

    @staticmethod
    def from_dict(dict_: Dict) -> AirflowWorkerVm:
        """Constructs a VirtualMachine instance from a dictionary.
        :param dict_: the dictionary.
        :return: the VirtualMachine instance.
        """

        machine_type = dict_.get("machine_type")
        disk_size = dict_.get("disk_size")
        disk_type = dict_.get("disk_type")
        create = str(dict_.get("create")).lower() == "true"
        return AirflowWorkerVm(machine_type, disk_size, disk_type, create)


def list_to_hcl(items: List[Union[AirflowConnection, AirflowVariable]]) -> str:
    """Convert a list of AirflowConnection or AirflowVariable instances into HCL.
    :param items: the list of AirflowConnection or AirflowVariable instances.
    :return: the HCL string.
    """

    dict_ = dict()
    for item in items:
        dict_[item.name] = item.value
    return to_hcl(dict_)


@dataclass
class Api(ConfigSection):
    """The API settings.

    Attributes:
        domain_name: the custom domain name of the API
        subdomain: the subdomain of the API, can be either based on the google project id or the environment. When
        based on the environment, there is no subdomain for the production environment.
    """

    name: str = "my-api"
    package: str = "/path/to/package/my_api"
    domain_name: str = "api.observatory.academy"
    subdomain: str = "project_id"
    image_tag: str = "2021.09.01"
    auth0_client_id: str = "auth0 application client id"
    auth0_client_secret: str = "auth0 application client secret"
    session_secret_key: str = os.urandom(24).hex()

    def to_hcl(self):
        return to_hcl(
            {
                "name": self.name,
                "package": self.package,
                "domain_name": self.domain_name,
                "subdomain": self.subdomain,
                "image_tag": self.image_tag,
                "auth0_client_id": self.auth0_client_id,
                "auth0_client_secret": self.auth0_client_secret,
                "session_secret_key": self.session_secret_key,
            }
        )

    def to_string(self, requirement: str, comment_out: bool, backend_type: BackendType):
        description = [
            f"# [{requirement}] Settings for the API \n",
            "# If you did not supply your own secret key, then this is autogenerated\n",
        ]
        lines = ["api:\n"]
        for variable in fields(self):
            value = getattr(self, variable.name)
            lines.append(indent(f"{variable.name}: {value}\n", INDENT1))
        lines = map(comment, lines) if comment_out else lines
        return description + list(lines)

    @staticmethod
    def from_dict(dict_: Dict) -> Api:
        """Constructs a CloudSqlDatabase instance from a dictionary.

        :param dict_: the dictionary.
        :return: the CloudSqlDatabase instance.
        """

        name = dict_.get("name")
        package = dict_.get("package")
        domain_name = dict_.get("domain_name")
        subdomain = dict_.get("subdomain")
        image_tag = dict_.get("image_tag", "")
        auth0_client_id = dict_.get("auth0_client_id")
        auth0_client_secret = dict_.get("auth0_client_secret")
        session_secret_key = dict_.get("session_secret_key")

        return Api(
            name, package, domain_name, subdomain, image_tag, auth0_client_id, auth0_client_secret, session_secret_key
        )


@dataclass
class ApiType(ConfigSection):
    """Settings related to a specific API type.

    Attributes:
        observatory_organization:
        observatory_workspace:
        elasticsearch_host:
        elasticsearch_api_key:
    """

    type: ApiTypes = ApiTypes.data_api
    observatory_organization: str = None
    observatory_workspace: str = None
    elasticsearch_host: str = "https://address.region.gcp.cloud.es.io:port"
    elasticsearch_api_key: str = "API_KEY"

    def to_hcl(self):
        return to_hcl(
            {
                "type": self.type.value,
                "observatory_organization": self.observatory_organization,
                "observatory_workspace": self.observatory_workspace,
                "elasticsearch_host": self.elasticsearch_host,
                "elasticsearch_api_key": self.elasticsearch_api_key,
            }
        )

    def to_string(self, requirement: str, comment_out: bool, backend_type: BackendType):
        description = [
            f"# [{requirement}] Settings related to the API type. Elasticsearch details "
            f"are only required if the type is 'data_api'\n",
        ]

        lines = ["api_type:\n"]
        lines.append(indent(f"type: {self.type.value}\n", INDENT1))
        comm = "# " if self.type == ApiTypes.observatory_api or comment_out else ""
        lines.append(indent(f"{comm}elasticsearch_host: {self.elasticsearch_host}\n", INDENT1))
        lines.append(indent(f"{comm}elasticsearch_api_key: {self.elasticsearch_api_key}\n", INDENT1))

        return description + list(lines)

    @staticmethod
    def from_dict(dict_: Dict) -> ApiType:
        """ """
        api_type = ApiTypes(dict_.get("type"))
        elasticsearch_host = dict_.get("elasticsearch_host", "")
        elasticsearch_api_key = dict_.get("elasticsearch_api_key", "")
        return ApiType(
            type=api_type, elasticsearch_host=elasticsearch_host, elasticsearch_api_key=elasticsearch_api_key
        )


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


def path_exists(field, value, error):
    """Throw an error when the given value does not exist as a path

    :param field: the field.
    :param value: the value.
    :param error: ?
    :return: None.
    """
    if not os.path.exists(value):
        error(field, f"Given path '{value}' does not exist")


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
        airflow_variables: AirflowVariables = None,
        airflow_connections: AirflowConnections = None,
        workflows_projects: WorkflowsProjects = None,
        validator: ObservatoryConfigValidator = None,
    ):
        """Create an ObservatoryConfig instance.

        :param backend: the backend config.
        :param observatory: the Observatory config.
        :param google_cloud: the Google Cloud config.
        :param terraform: the Terraform config.
        :param airflow_variables: a list of Airflow variables.
        :param airflow_connections: a list of Airflow connections.
        :param workflows_projects: a list of DAGs projects.
        :param validator: an ObservatoryConfigValidator instance.
        """

        self.backend = backend if backend is not None else Backend(type=BackendType.local)
        self.observatory = observatory if observatory is not None else Observatory()
        self.google_cloud = google_cloud
        self.terraform = terraform
        self.airflow_variables = airflow_variables
        self.airflow_connections = airflow_connections
        self.workflows_projects = workflows_projects

        self.validator = validator

        self.schema = make_schema(self.backend.type)

    @property
    def is_valid(self) -> bool:
        """Checks whether the config is valid or not.

        :return: whether the config is valid or not.
        """

        return self.validator is not None and not len(self.validator._errors)

    @property
    def errors(self) -> List[ValidationError]:
        """Returns a list of ValidationError instances that were created when parsing the config file.

        :return: the list of ValidationError instances.
        """

        errors = []
        if self.validator is None:
            return [ValidationError("Validator", "Validator is None")]
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

        packages = [
            PythonPackage(
                name="observatory-platform",
                type=self.observatory.package_type,
                host_package=self.observatory.package,
                docker_package=os.path.basename(self.observatory.package),
            ),
        ]
        if self.workflows_projects:
            for project in self.workflows_projects.workflows_projects:
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
    def dags_module_names(self):
        """Returns a list of DAG project module names.
        :return: the list of DAG project module names.
        """
        if self.workflows_projects:
            return (
                f"'{str(json.dumps([project.dags_module for project in self.workflows_projects.workflows_projects]))}'"
            )
        else:
            return None

    def make_airflow_variables(self) -> List[AirflowVariable]:
        """Make all airflow variables for the observatory platform.

        :return: a list of AirflowVariable objects.
        """

        # Create airflow variables from fixed config file values
        variables = [AirflowVariable(AirflowVars.ENVIRONMENT, self.backend.environment.value)]

        if self.google_cloud is not None:
            if self.google_cloud.project_id is not None:
                variables.append(AirflowVariable(AirflowVars.PROJECT_ID, self.google_cloud.project_id))

            if self.google_cloud.data_location:
                variables.append(AirflowVariable(AirflowVars.DATA_LOCATION, self.google_cloud.data_location))

            # Create airflow variables from bucket names
            for bucket in self.google_cloud.buckets:
                variables.append(AirflowVariable(bucket.id, bucket.name))

        if self.terraform is not None:
            if self.terraform.organization is not None:
                variables.append(AirflowVariable(AirflowVars.TERRAFORM_ORGANIZATION, self.terraform.organization))

        workflows_projects = self.workflows_projects.workflows_projects if self.workflows_projects else []
        variables.append(
            AirflowVariable(
                AirflowVars.DAGS_MODULE_NAMES, json.dumps([proj.dags_module for proj in workflows_projects])
            )
        )

        if self.airflow_variables and self.airflow_variables.airflow_variables:
            # Add user defined variables to list
            variables += self.airflow_variables.airflow_variables

        return variables

    @staticmethod
    def _parse_fields(
        dict_: Dict,
    ) -> Tuple[Backend, Observatory, GoogleCloud, Terraform, AirflowVariables, AirflowConnections, WorkflowsProjects,]:
        backend = Backend.from_dict(dict_.get("backend", dict()))
        observatory = Observatory.from_dict(dict_.get("observatory", dict()))
        google_cloud = GoogleCloud.from_dict(dict_.get("google_cloud", dict()))
        terraform = Terraform.from_dict(dict_.get("terraform", dict()))
        airflow_variables = AirflowVariables.from_dict(dict_.get("airflow_variables", dict()))
        airflow_connections = AirflowConnections.from_dict(dict_.get("airflow_connections", dict()))
        workflows_projects = WorkflowsProjects.from_dict(dict_.get("workflows_projects", list()))

        return backend, observatory, google_cloud, terraform, airflow_variables, airflow_connections, workflows_projects

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
                airflow_variables,
                airflow_connections,
                workflows_projects,
            ) = ObservatoryConfig._parse_fields(dict_)

            return ObservatoryConfig(
                backend,
                observatory,
                google_cloud=google_cloud,
                terraform=terraform,
                airflow_variables=airflow_variables,
                airflow_connections=airflow_connections,
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

    def save(self, path: str):
        """Save the observatory configuration parameters to a config file.

        :param path: Configuration file path.
        """
        config_lines = []
        for section, default in [
            (self.backend, Backend(self.backend.type)),
            (self.observatory, Observatory()),
            (self.google_cloud, GoogleCloud()),
            (self.terraform, Terraform()),
            (self.airflow_variables, AirflowVariables()),
            (self.airflow_connections, AirflowConnections()),
            (self.workflows_projects, WorkflowsProjects()),
        ]:
            lines = ConfigSection.get_lines(section, default, self)
            config_lines += lines

        with open(path, "w") as f:
            f.writelines(config_lines)


class TerraformConfig(ObservatoryConfig):
    WORKSPACE_PREFIX = "observatory-"

    def __init__(
        self,
        backend: Backend = None,
        observatory: Observatory = None,
        google_cloud: GoogleCloud = None,
        terraform: Terraform = None,
        airflow_variables: AirflowVariables = None,
        airflow_connections: AirflowConnections = None,
        workflows_projects: WorkflowsProjects = None,
        cloud_sql_database: CloudSqlDatabase = None,
        airflow_main_vm: AirflowMainVm = None,
        airflow_worker_vm: AirflowWorkerVm = None,
        validator: ObservatoryConfigValidator = None,
    ):
        """Create a TerraformConfig instance.

        :param backend: the backend config.
        :param observatory: the Observatory config.
        :param google_cloud: the Google Cloud config.
        :param terraform: the Terraform config.
        :param airflow_variables: a list of Airflow variables.
        :param airflow_connections: a list of Airflow connections.
        :param workflows_projects: a list of DAGs projects.
        :param cloud_sql_database: a Google Cloud SQL database config.
        :param airflow_main_vm: the Airflow Main VM config.
        :param airflow_worker_vm: the Airflow Worker VM config.
        :param validator: an ObservatoryConfigValidator instance.
        """

        backend = backend if backend is not None else Backend(type=BackendType.terraform)

        super().__init__(
            backend=backend,
            observatory=observatory,
            google_cloud=google_cloud,
            terraform=terraform,
            airflow_variables=airflow_variables,
            airflow_connections=airflow_connections,
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

    def make_airflow_variables(self) -> List[AirflowVariable]:
        """Make all airflow variables for the Observatory Platform.

        :return: a list of AirflowVariable objects.
        """

        # Create airflow variables from fixed config file values
        variables = [AirflowVariable(AirflowVars.ENVIRONMENT, self.backend.environment.value)]

        if self.google_cloud.project_id is not None:
            variables.append(AirflowVariable(AirflowVars.PROJECT_ID, self.google_cloud.project_id))
            variables.append(AirflowVariable(AirflowVars.DOWNLOAD_BUCKET, f"{self.google_cloud.project_id}-download"))
            variables.append(AirflowVariable(AirflowVars.TRANSFORM_BUCKET, f"{self.google_cloud.project_id}-transform"))

        if self.google_cloud.data_location:
            variables.append(AirflowVariable(AirflowVars.DATA_LOCATION, self.google_cloud.data_location))

        if self.terraform.organization is not None:
            variables.append(AirflowVariable(AirflowVars.TERRAFORM_ORGANIZATION, self.terraform.organization))

        variables.append(
            AirflowVariable(
                AirflowVars.DAGS_MODULE_NAMES,
                json.dumps([proj.dags_module for proj in self.workflows_projects.workflows_projects]),
            )
        )

        if self.airflow_variables.airflow_variables:
            # Add user defined variables to list
            variables += self.airflow_variables.airflow_variables

        return variables

    @property
    def terraform_variables(self) -> List[TerraformVariable]:
        """Create a list of TerraformVariable instances from the Terraform Config.

        :return: a list of TerraformVariable instances.
        """

        sensitive = True
        return [
            TerraformVariable("environment", self.backend.environment.value),
            TerraformVariable("observatory", self.observatory.to_hcl(), sensitive=sensitive, hcl=True),
            TerraformVariable("google_cloud", self.google_cloud.to_hcl(), sensitive=sensitive, hcl=True),
            TerraformVariable("cloud_sql_database", self.cloud_sql_database.to_hcl(), hcl=True),
            TerraformVariable("airflow_main_vm", self.airflow_main_vm.to_hcl(), hcl=True),
            TerraformVariable("airflow_worker_vm", self.airflow_worker_vm.to_hcl(), hcl=True),
            TerraformVariable(
                "airflow_variables", list_to_hcl(self.make_airflow_variables()), hcl=True, sensitive=False
            ),
            TerraformVariable(
                "airflow_connections",
                list_to_hcl(self.airflow_connections.airflow_connections),
                hcl=True,
                sensitive=sensitive,
            ),
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
                airflow_variables,
                airflow_connections,
                workflows_projects,
            ) = ObservatoryConfig._parse_fields(dict_)

            cloud_sql_database = CloudSqlDatabase.from_dict(dict_.get("cloud_sql_database", dict()))
            airflow_main_vm = AirflowMainVm.from_dict(dict_.get("airflow_main_vm", dict()))
            airflow_worker_vm = AirflowWorkerVm.from_dict(dict_.get("airflow_worker_vm", dict()))

            return TerraformConfig(
                backend,
                observatory,
                google_cloud=google_cloud,
                terraform=terraform,
                airflow_variables=airflow_variables,
                airflow_connections=airflow_connections,
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

        config_lines = []
        for section, default in [
            (self.backend, Backend(self.backend.type)),
            (self.observatory, Observatory()),
            (self.terraform, Terraform()),
            (self.google_cloud, GoogleCloud()),
            (self.cloud_sql_database, CloudSqlDatabase()),
            (self.airflow_main_vm, AirflowMainVm()),
            (self.airflow_worker_vm, AirflowWorkerVm()),
            (self.airflow_variables, AirflowVariables()),
            (self.airflow_connections, AirflowConnections()),
            (self.workflows_projects, WorkflowsProjects()),
        ]:
            lines = ConfigSection.get_lines(section, default, self)
            config_lines += lines

        with open(path, "w") as f:
            f.writelines(config_lines)


class TerraformAPIConfig(ObservatoryConfig):
    WORKSPACE_PREFIX = "observatory-"

    def __init__(
        self,
        backend: Backend = None,
        google_cloud: GoogleCloud = None,
        terraform: Terraform = None,
        api: Api = None,
        api_type: ApiType = None,
        validator: ObservatoryConfigValidator = None,
    ):
        """Create a TerraformConfig instance.
        :param backend: the backend config.
        :param google_cloud: the Google Cloud config.
        :param terraform: the Terraform config.
        :param validator: an ObservatoryConfigValidator instance.
        """

        backend = backend if backend is not None else Backend(type=BackendType.terraform_api)

        super().__init__(
            backend=backend,
            google_cloud=google_cloud,
            terraform=terraform,
            validator=validator,
        )
        self.api = api
        self.api_type = api_type

    @property
    def terraform_workspace_id(self):
        """The Terraform workspace id.
        :return: the terraform workspace id.
        """

        return TerraformAPIConfig.WORKSPACE_PREFIX + self.api.name + "-" + self.backend.environment.value

    @property
    def terraform_variables(self) -> List[TerraformVariable]:
        """Create a list of TerraformVariable instances from the TerraformAPIConfig.
        :return: a list of TerraformVariable instances.
        """

        # TODO change back
        sensitive = False
        return [
            TerraformVariable("environment", self.backend.environment.value),
            TerraformVariable("google_cloud", self.google_cloud.to_hcl(), sensitive=sensitive, hcl=True),
            TerraformVariable("api", self.api.to_hcl(), hcl=True),
            TerraformVariable("api_type", self.api_type.to_hcl(), sensitive=sensitive, hcl=True),
        ]

    @classmethod
    def from_dict(cls, dict_: Dict) -> TerraformAPIConfig:
        """Make an TerraformAPIConfig instance from a dictionary.
        If the dictionary is invalid, then an ObservatoryConfig instance will be returned with no properties set,
        except for the validator, which contains validation errors.
        :param dict_: the input dictionary that has been read via yaml.safe_load.
        :return: the TerraformConfig instance.
        """

        schema = make_schema(BackendType.terraform_api)
        validator = ObservatoryConfigValidator()
        is_valid = validator.validate(dict_, schema)

        if is_valid:
            (
                backend,
                _,
                google_cloud,
                terraform,
                _,
                _,
                _,
            ) = ObservatoryConfig._parse_fields(dict_)

            api = Api.from_dict(dict_.get("api", dict()))
            api_type = ApiType.from_dict(dict_.get("api_type", dict()))

            organization = terraform.organization
            workspace = TerraformAPIConfig.WORKSPACE_PREFIX + backend.environment.value
            api_type.observatory_organization = organization if api_type == ApiTypes.observatory_api else ""
            api_type.observatory_workspace = workspace if api_type == ApiTypes.observatory_api else ""

            return TerraformAPIConfig(
                backend,
                google_cloud=google_cloud,
                terraform=terraform,
                api=api,
                api_type=api_type,
                validator=validator,
            )
        else:
            return TerraformAPIConfig(validator=validator)

    def save(self, path: str):
        """Save the observatory configuration parameters to a config file.

        :param path: Configuration file path.
        """

        config_lines = []
        for section, default in [
            (self.backend, Backend(self.backend.type)),
            (self.terraform, Terraform()),
            (self.google_cloud, GoogleCloud()),
            (self.api, Api()),
            (self.api_type, ApiType()),
        ]:
            lines = ConfigSection.get_lines(section, default, self)
            config_lines += lines

        with open(path, "w") as f:
            f.writelines(config_lines)


def make_schema(backend_type: BackendType) -> Dict:
    """Make a schema for an Observatory or Terraform config file.

    :param backend_type: the type of backend, local or terraform.
    :return: a dictionary containing the schema.
    """

    schema = dict()
    is_backend_local = backend_type == BackendType.local
    is_backend_terraform = backend_type == BackendType.terraform
    is_backend_terraform_api = backend_type == BackendType.terraform_api
    is_backend_terraform_or_api = backend_type == BackendType.terraform or backend_type == BackendType.terraform_api

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
        "required": is_backend_terraform_or_api,
        "type": "dict",
        "schema": {"organization": {"required": True, "type": "string"}},
    }

    # Google Cloud settings
    schema["google_cloud"] = {
        "required": is_backend_terraform_or_api,
        "type": "dict",
        "schema": {
            "project_id": {"required": True, "type": "string"},
            "credentials": {
                "required": True,
                "type": "string",
                "google_application_credentials": True,
            },
            "region": {
                "required": is_backend_terraform_or_api,
                "type": "string",
                "regex": r"^\w+\-\w+\d+$",
            },
            "zone": {
                "required": is_backend_terraform_or_api,
                "type": "string",
                "regex": r"^\w+\-\w+\d+\-[a-z]{1}$",
            },
            "data_location": {
                "required": True,
                "type": "string",
            },
            "buckets": {
                "required": is_backend_local,
                "type": "dict",
                "keysrules": {"type": "string"},
                "valuesrules": {"type": "string"},
            },
        },
    }

    # Observatory settings
    package_types = ["editable", "sdist", "pypi"]
    schema["observatory"] = {
        "required": is_backend_local or is_backend_terraform,
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
            "elastic_port": {"required": False, "type": "integer"},
            "kibana_port": {"required": False, "type": "integer"},
            "docker_network_name": {"required": False, "type": "string"},
            "docker_network_is_external": {"required": False, "type": "boolean"},
            "docker_compose_project_name": {"required": False, "type": "string"},
            "enable_elk": {"required": False, "type": "boolean"},
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

    # Airflow variables
    schema["airflow_variables"] = {
        "required": False,
        "type": "dict",
        "keysrules": {"type": "string"},
        "valuesrules": {"type": "string"},
    }

    # Airflow connections
    schema["airflow_connections"] = {
        "required": False,
        "type": "dict",
        "keysrules": {"type": "string"},
        "valuesrules": {"type": "string", "regex": r"\S*:\/\/\S*:\S*@\S*$|google-cloud-platform:\/\/\S*$"},
    }

    # Dags projects
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

    if is_backend_terraform_api:
        schema["api"] = {
            "required": True,
            "type": "dict",
            "schema": {
                "name": {
                    "required": True,
                    "type": "string",
                    "maxlength": 15,
                },
                "package": {
                    "required": True,
                    "type": "string",
                    "check_with": path_exists,
                },
                "domain_name": {"required": True, "type": "string"},
                "subdomain": {"required": True, "type": "string", "allowed": ["project_id", "environment"]},
                "image_tag": {"required": True, "type": "string"},
                "auth0_client_id": {"required": True, "type": "string"},
                "auth0_client_secret": {"required": True, "type": "string"},
                "session_secret_key": {"required": True, "type": "string"},
            },
        }
        schema["api_type"] = {
            "required": True,
            "type": "dict",
            "schema": {
                "type": {"required": True, "type": "string", "allowed": ["observatory_api", "data_api"]},
                "elasticsearch_host": {"required": False, "dependencies": {"type": "data_api"}},
                "elasticsearch_api_key": {"required": False, "dependencies": {"type": "data_api"}},
            },
        }

    return schema
