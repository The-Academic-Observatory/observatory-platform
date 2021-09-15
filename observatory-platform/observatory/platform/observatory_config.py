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

# Author: James Diprose, Aniek Roelofs


from __future__ import annotations

import binascii
import json
import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, ClassVar, Dict, List, Tuple, Union

import cerberus.validator
import yaml
from cerberus import Validator
from cryptography.fernet import Fernet

from observatory.platform.terraform_api import TerraformVariable
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.config_utils import module_file_path
from observatory.platform.utils.config_utils import observatory_home as default_observatory_home
from observatory.platform.utils.jinja2_utils import render_template


def generate_secret_key(length: int = 30) -> str:
    """Generate a secret key for the Flask Airflow Webserver.

    :param length: the length of the key to generate.
    :return: the random key.
    """

    return str(binascii.b2a_hex(os.urandom(length)))


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

    type: BackendType
    environment: Environment

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
        :param elastic_port: The host's Elasticsearch port number.
        :param kibana_port: The host's Kibana port number.
        :param docker_network_name: The Docker Network name, used to specify a custom Docker Network.
        :param docker_network_is_external: whether the docker network is external or not.
        :param docker_compose_project_name: The namespace for the Docker Compose containers: https://docs.docker.com/compose/reference/envvars/#compose_project_name.
        :param enable_elk: whether to enable the elk stack or not.
    """

    package: str
    package_type: str
    airflow_fernet_key: str
    airflow_secret_key: str
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
    api_package: str = "observatory-api"
    api_package_type: str = "pypi"

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
        elastic_port = dict_.get("elastic_port", Observatory.elastic_port)
        kibana_port = dict_.get("kibana_port", Observatory.kibana_port)
        docker_network_name = dict_.get("docker_network_name", Observatory.docker_network_name)
        docker_network_is_external = dict_.get("docker_network_is_external", Observatory.docker_network_is_external)
        docker_compose_project_name = dict_.get("docker_compose_project_name", Observatory.docker_compose_project_name)
        enable_elk = dict_.get("enable_elk", Observatory.enable_elk)
        api_package = dict_.get("api_package", Observatory.api_package)
        api_package_type = dict_.get("api_package_type", Observatory.api_package_type)

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
            api_package=api_package,
            api_package_type=api_package_type,
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
        buckets: a list of the Google Cloud buckets.
    """

    project_id: str = None
    credentials: str = None
    region: str = None
    zone: str = None
    data_location: str = None
    buckets: List[CloudStorageBucket] = field(default_factory=lambda: [])

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
    def parse_dags_projects(list: List) -> List[WorkflowsProject]:
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
class AirflowConnection:
    """An Airflow Connection.

    Attributes:
        name: the name of the Airflow Connection.
        value: the value of the Airflow Connection.
    """

    name: str
    value: str

    @property
    def conn_name(self) -> str:
        """The Airflow Connection environment variable name, which is required to set the connection from an
        environment variable.

        :return: the Airflow Connection environment variable name.
        """

        return f"AIRFLOW_CONN_{self.name.upper()}"

    @staticmethod
    def parse_airflow_connections(dict_: Dict) -> List[AirflowConnection]:
        """Parse the airflow_connections dictionary object into a list of AirflowConnection instances.

        :param dict_: the dictionary.
        :return: a list of AirflowConnection instances.
        """

        return parse_dict_to_list(dict_, AirflowConnection)


@dataclass
class AirflowVariable:
    """An Airflow Variable.

    Attributes:
        name: the name of the Airflow Variable.
        value: the value of the Airflow Variable.
    """

    name: str
    value: str

    @property
    def env_var_name(self):
        """The Airflow Variable environment variable name, which is required to set the variable from an
        environment variable.

        :return: the Airflow Variable environment variable name.
        """

        return f"AIRFLOW_VAR_{self.name.upper()}"

    @staticmethod
    def parse_airflow_variables(dict_: Dict) -> List[AirflowVariable]:
        """Parse the airflow_variables dictionary object into a list of AirflowVariable instances.

        :param dict_: the dictionary.
        :return: a list of AirflowVariable instances.
        """

        return parse_dict_to_list(dict_, AirflowVariable)


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


@dataclass
class ElasticSearch:
    """The elasticsearch settings for the Observatory Platform API.

    Attributes:
        host: the address of the elasticsearch host
        api_key: the api key to use the elasticsearch API.
    """

    host: str
    api_key: str

    def to_hcl(self):
        return to_hcl(
            {
                "host": self.host,
                "api_key": self.api_key,
            }
        )

    @staticmethod
    def from_dict(dict_: Dict) -> ElasticSearch:
        """Constructs a CloudSqlDatabase instance from a dictionary.

        :param dict_: the dictionary.
        :return: the CloudSqlDatabase instance.
        """

        host = dict_.get("host")
        api_key = dict_.get("api_key")
        return ElasticSearch(host, api_key)


@dataclass
class Api:
    """The API domain name for the Observatory Platform API.

    Attributes:
        domain_name: the custom domain name of the API
        subdomain: the subdomain of the API, can be either based on the google project id or the environment. When
        based on the environment, there is no subdomain for the production environment.
    """

    domain_name: str
    subdomain: str

    def to_hcl(self):
        return to_hcl({"domain_name": self.domain_name, "subdomain": self.subdomain})

    @staticmethod
    def from_dict(dict_: Dict) -> Api:
        """Constructs a CloudSqlDatabase instance from a dictionary.

        :param dict_: the dictionary.
        :return: the CloudSqlDatabase instance.
        """

        domain_name = dict_.get("domain_name")
        subdomain = dict_.get("subdomain")
        return Api(domain_name, subdomain)


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
        airflow_variables: List[AirflowVariable] = None,
        airflow_connections: List[AirflowConnection] = None,
        workflows_projects: List[WorkflowsProject] = None,
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

        self.backend = backend
        self.observatory = observatory
        self.google_cloud = google_cloud
        self.terraform = terraform

        self.airflow_variables = airflow_variables
        if airflow_variables is None:
            self.airflow_variables = []

        self.airflow_connections = airflow_connections
        if airflow_variables is None:
            self.airflow_connections = []

        self.workflows_projects = workflows_projects
        if workflows_projects is None:
            self.workflows_projects = []

        self.validator = validator

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
    def dags_module_names(self):
        """Returns a list of DAG project module names.
        :return: the list of DAG project module names.
        """

        return f"'{str(json.dumps([project.dags_module for project in self.workflows_projects]))}'"

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

        # Add user defined variables to list
        variables += self.airflow_variables

        return variables

    @staticmethod
    def _parse_fields(
        dict_: Dict,
    ) -> Tuple[
        Backend,
        Observatory,
        GoogleCloud,
        Terraform,
        List[AirflowVariable],
        List[AirflowConnection],
        List[WorkflowsProject],
    ]:
        backend = Backend.from_dict(dict_.get("backend", dict()))
        observatory = Observatory.from_dict(dict_.get("observatory", dict()))
        google_cloud = GoogleCloud.from_dict(dict_.get("google_cloud", dict()))
        terraform = Terraform.from_dict(dict_.get("terraform", dict()))
        airflow_variables = AirflowVariable.parse_airflow_variables(dict_.get("airflow_variables", dict()))
        airflow_connections = AirflowConnection.parse_airflow_connections(dict_.get("airflow_connections", dict()))
        workflows_projects = WorkflowsProject.parse_dags_projects(dict_.get("workflows_projects", list()))

        return backend, observatory, google_cloud, terraform, airflow_variables, airflow_connections, workflows_projects

    @staticmethod
    def save_default(config_path: str):
        """Save a default config file.

        :param config_path: the path where the config file should be saved.
        :return: None.
        """

        ObservatoryConfig._save_default(config_path, "config.yaml.jinja2")

    @staticmethod
    def _save_default(config_path: str, template_file_name: str):
        """Save a default config file.

        :param config_path: the path where the config file should be saved.
        :param template_file_name: the name of the template file name.
        :return: None.
        """

        # Render template
        template_path = os.path.join(module_file_path("observatory.platform"), template_file_name)
        airflow_fernet_key = Fernet.generate_key()
        airflow_secret_key = generate_secret_key()

        render = render_template(
            template_path, airflow_fernet_key=airflow_fernet_key, airflow_secret_key=airflow_secret_key
        )

        # Save file
        with open(config_path, "w") as f:
            f.write(render)

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


class TerraformConfig(ObservatoryConfig):
    WORKSPACE_PREFIX = "observatory-"

    def __init__(
        self,
        backend: Backend = None,
        observatory: Observatory = None,
        google_cloud: GoogleCloud = None,
        terraform: Terraform = None,
        airflow_variables: List[AirflowVariable] = None,
        airflow_connections: List[AirflowConnection] = None,
        workflows_projects: List[WorkflowsProject] = None,
        cloud_sql_database: CloudSqlDatabase = None,
        airflow_main_vm: VirtualMachine = None,
        airflow_worker_vm: VirtualMachine = None,
        elasticsearch: ElasticSearch = None,
        api: Api = None,
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
        self.elasticsearch = elasticsearch
        self.api = api

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
                AirflowVars.DAGS_MODULE_NAMES, json.dumps([proj.dags_module for proj in self.workflows_projects])
            )
        )

        # Add user defined variables to list
        variables += self.airflow_variables

        return variables

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
                "airflow_connections", list_to_hcl(self.airflow_connections), hcl=True, sensitive=sensitive
            ),
            TerraformVariable("elasticsearch", self.elasticsearch.to_hcl(), sensitive=sensitive, hcl=True),
            TerraformVariable("api", self.api.to_hcl(), hcl=True),
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
            airflow_main_vm = VirtualMachine.from_dict(dict_.get("airflow_main_vm", dict()))
            airflow_worker_vm = VirtualMachine.from_dict(dict_.get("airflow_worker_vm", dict()))
            elasticsearch = ElasticSearch.from_dict(dict_.get("elasticsearch", dict()))
            api = Api.from_dict(dict_.get("api", dict()))

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
                elasticsearch=elasticsearch,
                api=api,
                validator=validator,
            )
        else:
            return TerraformConfig(validator=validator)

    @staticmethod
    def save_default(config_path: str):
        """Save a default TerraformConfig file.

        :param config_path: the path where the config file should be saved.
        :return: None.
        """

        ObservatoryConfig._save_default(config_path, "config-terraform.yaml.jinja2")


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

    if not is_backend_terraform:
        schema["google_cloud"]["schema"]["buckets"] = {
            "required": False,
            "type": "dict",
            "keysrules": {"type": "string"},
            "valuesrules": {"type": "string"},
        }

    # Observatory settings
    package_types = ["editable", "sdist", "pypi"]
    schema["observatory"] = {
        "required": True,
        "type": "dict",
        "schema": {
            "package": {"required": True, "type": "string"},
            "package_type": {"required": True, "type": "string", "allowed": package_types},
            "airflow_fernet_key": {"required": True, "type": "string"},
            "airflow_secret_key": {"required": True, "type": "string"},
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
            "api_package": {"required": False, "type": "string"},
            "api_package_type": {"required": False, "type": "string", "allowed": package_types},
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

    # Key value string pair schema
    key_val_schema = {
        "required": False,
        "type": "dict",
        "keysrules": {"type": "string"},
        "valuesrules": {"type": "string"},
    }

    # Airflow variables
    schema["airflow_variables"] = key_val_schema

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

    if is_backend_terraform:
        schema["elasticsearch"] = {
            "required": True,
            "type": "dict",
            "schema": {
                "host": {"required": True, "type": "string"},
                "api_key": {
                    "required": True,
                    "type": "string",
                },
            },
        }

        schema["api"] = {
            "required": True,
            "type": "dict",
            "schema": {
                "domain_name": {"required": True, "type": "string"},
                "subdomain": {"required": True, "type": "string", "allowed": ["project_id", "environment"]},
            },
        }

    return schema
