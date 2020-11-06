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

import json
import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Tuple, List, ClassVar, Any, Union

import cerberus.validator
import yaml
from airflow.configuration import generate_fernet_key
from cerberus import Validator

from observatory.platform.terraform_api import TerraformVariable
from observatory.platform.utils.airflow_utils import AirflowVariable
from observatory.platform.utils.config_utils import AirflowVars, module_file_path
from observatory.platform.utils.jinja2_utils import render_template


def to_hcl(value: Dict) -> str:
    """ Convert a Python dictionary into HCL.

    :param value: the dictionary.
    :return: the HCL string.
    """

    return json.dumps(value, separators=(',', '='))


def from_hcl(string: str) -> Dict:
    """ Convert an HCL string into a Dict.

    :param string: the HCL string.
    :return: the dict.
    """

    return json.loads(string, separators=(',', '='))


class BackendType(Enum):
    """ The type of backend """

    local = 'local'
    terraform = 'terraform'


class Environment(Enum):
    """ The environment being used """

    develop = 'develop'
    staging = 'staging'
    production = 'production'


@dataclass
class Backend:
    """ The backend settings for the Observatory Platform.

    Attributes:
        type: the type of backend being used (local environment or Terraform).
        environment: what type of environment is being deployed (develop, staging or production).
     """

    type: BackendType
    environment: Environment

    @staticmethod
    def from_dict(dict_: Dict) -> Backend:
        """ Constructs a Backend instance from a dictionary.

        :param dict_: the dictionary.
        :return: the Backend instance.
        """

        type = BackendType(dict_.get('type'))
        environment = Environment(dict_.get('environment'))
        return Backend(type, environment)


@dataclass
class Airflow:
    """ The Apache Airflow settings for the Observatory Platform.

    Attributes:
        fernet_key: the Fernet key.
        ui_user_password: the password for the Apache Airflow UI admin user.
        ui_user_email: the email address for the Apache Airflow UI admin user.
     """

    fernet_key: str
    ui_user_password: str = None
    ui_user_email: str = None

    def to_hcl(self):
        return to_hcl({
            'fernet_key': self.fernet_key,
            'ui_user_password': self.ui_user_password,
            'ui_user_email': self.ui_user_email
        })

    @staticmethod
    def from_dict(dict_: Dict) -> Airflow:
        """ Constructs an Airflow instance from a dictionary.

        :param dict_: the dictionary.
        :return: the Airflow instance.
        """

        fernet_key = dict_.get('fernet_key')
        ui_user_password = dict_.get('ui_user_password')
        ui_user_email = dict_.get('ui_user_email')
        return Airflow(fernet_key,
                       ui_user_password=ui_user_password,
                       ui_user_email=ui_user_email)


@dataclass
class CloudStorageBucket:
    """ Represents a Google Cloud Storage Bucket.

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
    """ The Google Cloud settings for the Observatory Platform.

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
        with open(self.credentials, 'r') as f:
            data = f.read()
        return data

    def to_hcl(self):
        return to_hcl({
            'project_id': self.project_id,
            'credentials': self.read_credentials(),
            'region': self.region,
            'zone': self.zone,
            'data_location': self.data_location,
            'buckets': [bucket.name for bucket in self.buckets]
        })

    @staticmethod
    def from_dict(dict_: Dict) -> GoogleCloud:
        """ Constructs a GoogleCloud instance from a dictionary.

        :param dict_: the dictionary.
        :return: the GoogleCloud instance.
        """

        project_id = dict_.get('project_id')
        credentials = dict_.get('credentials')
        region = dict_.get('region')
        zone = dict_.get('zone')
        data_location = dict_.get('data_location')
        buckets = CloudStorageBucket.parse_buckets(dict_.get('buckets', dict()))

        return GoogleCloud(
            project_id=project_id,
            credentials=credentials,
            region=region,
            zone=zone,
            data_location=data_location,
            buckets=buckets)


def parse_dict_to_list(dict_: Dict, cls: ClassVar) -> List[Any]:
    """ Parse the key, value pairs in a dictionary into a list of class instances.

    :param dict_: the dictionary.
    :param cls: the type of class to construct.
    :return: a list of class instances.
    """

    parsed_items = []
    for key, val in dict_.items():
        parsed_items.append(cls(key, val))
    return parsed_items


@dataclass
class DagsProject:
    """ Represents a project that contains DAGs to load.

    Attributes:
        package_name: the package name.
        path: the path to the DAGs folder. TODO: check
        dags_module: the Python import path to the module that contains the Apache Airflow DAGs to load.
     """

    package_name: str
    path: str
    dags_module: str

    @property
    def type(self) -> str:
        """ The type of DAGs project: local, Github or PyPI.

        :return: the type of DAGs project.
        """

        return 'local'

    @staticmethod
    def parse_dags_projects(list: List) -> List[DagsProject]:
        """ Parse the dags_projects list object into a list of DagsProject instances.

        :param list: the list.
        :return: a list of DagsProject instances.
        """

        parsed_items = []
        for item in list:
            package_name = item['package_name']
            path = item['path']
            dags_module = item['dags_module']
            parsed_items.append(DagsProject(package_name, path, dags_module))
        return parsed_items

    @staticmethod
    def dags_projects_to_str(dags_projects: List[DagsProject]):
        return f"'{str(json.dumps([project.dags_module for project in dags_projects]))}'"


def list_to_hcl(items: List[Union[AirflowConnection, AirflowVariable]]) -> str:
    """ Convert a list of AirflowConnection or AirflowVariable instances into HCL.

    :param items: the list of AirflowConnection or AirflowVariable instances.
    :return: the HCL string.
    """

    dict_ = dict()
    for item in items:
        dict_[item.name] = item.value
    return to_hcl(dict_)


@dataclass
class AirflowConnection:
    """ An Airflow Connection.

    Attributes:
        name: the name of the Airflow Connection.
        value: the value of the Airflow Connection.
     """

    name: str
    value: str

    @property
    def conn_name(self) -> str:
        """ The Airflow Connection environment variable name, which is required to set the connection from an
        environment variable.

        :return: the Airflow Connection environment variable name.
        """

        return f'AIRFLOW_CONN_{self.name.upper()}'

    @staticmethod
    def parse_airflow_connections(dict_: Dict) -> List[AirflowConnection]:
        """ Parse the airflow_connections dictionary object into a list of AirflowConnection instances.

        :param dict_: the dictionary.
        :return: a list of AirflowConnection instances.
        """

        return parse_dict_to_list(dict_, AirflowConnection)


@dataclass
class AirflowVariable:
    """ An Airflow Variable.

    Attributes:
        name: the name of the Airflow Variable.
        value: the value of the Airflow Variable.
     """

    name: str
    value: str

    @property
    def env_var_name(self):
        """ The Airflow Variable environment variable name, which is required to set the variable from an
        environment variable.

        :return: the Airflow Variable environment variable name.
        """

        return f'AIRFLOW_VAR_{self.name.upper()}'

    @staticmethod
    def parse_airflow_variables(dict_: Dict) -> List[AirflowVariable]:
        """ Parse the airflow_variables dictionary object into a list of AirflowVariable instances.

        :param dict_: the dictionary.
        :return: a list of AirflowVariable instances.
        """

        return parse_dict_to_list(dict_, AirflowVariable)


@dataclass
class Terraform:
    """ The Terraform settings for the Observatory Platform.

    Attributes:
        organization: the Terraform Organisation name.
        workspace_prefix: the Terraform workspace prefix.
     """

    organization: str
    workspace_prefix: str

    @staticmethod
    def from_dict(dict_: Dict) -> Terraform:
        """ Constructs a Terraform instance from a dictionary.

        :param dict_: the dictionary.
        :return: the Terraform instance.
        """

        organization = dict_.get('organization')
        workspace_prefix = dict_.get('workspace_prefix')
        return Terraform(organization, workspace_prefix)


@dataclass
class CloudSqlDatabase:
    """ The Google Cloud SQL database settings for the Observatory Platform.

    Attributes:
        tier: the database machine tier.
        backup_start_time: the start time for backups in HH:MM format.
        postgres_password: the Postgres SQL password.
     """

    tier: str
    backup_start_time: str
    postgres_password: str

    def to_hcl(self):
        return to_hcl({
            'tier': self.tier,
            'backup_start_time': self.backup_start_time,
            'postgres_password': self.postgres_password
        })

    @staticmethod
    def from_dict(dict_: Dict) -> CloudSqlDatabase:
        """ Constructs a CloudSqlDatabase instance from a dictionary.

        :param dict_: the dictionary.
        :return: the CloudSqlDatabase instance.
        """

        tier = dict_.get('tier')
        backup_start_time = dict_.get('backup_start_time')
        postgres_password = dict_.get('postgres_password')
        return CloudSqlDatabase(tier, backup_start_time, postgres_password)


@dataclass
class VirtualMachine:
    """ A Google Cloud virtual machine.

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
        return to_hcl({
            'machine_type': self.machine_type,
            'disk_size': self.disk_size,
            'disk_type': self.disk_type,
            'create': self.create
        })

    @staticmethod
    def from_hcl(string: str) -> VirtualMachine:
        return VirtualMachine.from_dict(from_hcl(string))

    @staticmethod
    def from_dict(dict_: Dict) -> VirtualMachine:
        """ Constructs a VirtualMachine instance from a dictionary.

        :param dict_: the dictionary.
        :return: the VirtualMachine instance.
        """

        machine_type = dict_.get('machine_type')
        disk_size = dict_.get('disk_size')
        disk_type = dict_.get('disk_type')
        create = dict_.get('create')
        return VirtualMachine(machine_type, disk_size, disk_type, create)


def customise_pointer(field, value, error):
    """ Throw an error when a field contains the value ' <--' which means that the user should customise the
    value in the config file.

    :param field: the field.
    :param value: the value.
    :param error: ?
    :return: None.
    """

    if isinstance(value, str) and value.endswith(' <--'):
        error(field, "Customise value ending with ' <--'")


class ObservatoryConfigValidator(Validator):
    """ Custom config Validator"""

    def _validate_google_application_credentials(self, google_application_credentials, field, value):
        """ Validate that the Google Application Credentials file exists.
        The rule's arguments are validated against this schema: {'type': 'boolean'}
        """
        if google_application_credentials and value is not None and isinstance(value, str) and not os.path.isfile(
                value):
            self._error(field, f"the file {value} does not exist. See "
                               f"https://cloud.google.com/docs/authentication/getting-started for instructions on "
                               f"how to create a service account and save the JSON key to your workstation.")


@dataclass
class ValidationError:
    """ A validation error found when parsing a config file.

    Attributes:
        key: the key in the config file.
        value: the error.
     """

    key: str
    value: Any


class ObservatoryConfig:

    def __init__(self,
                 backend: Backend = None,
                 airflow: Airflow = None,
                 google_cloud: GoogleCloud = None,
                 terraform: Terraform = None,
                 airflow_variables: List[AirflowVariable] = None,
                 airflow_connections: List[AirflowConnection] = None,
                 dags_projects: List[DagsProject] = None,
                 validator: ObservatoryConfigValidator = None):
        """ Create an ObservatoryConfig instance.

        :param backend: the backend config.
        :param airflow: the Airflow config.
        :param google_cloud: the Google Cloud config.
        :param terraform: the Terraform config.
        :param airflow_variables: a list of Airflow variables.
        :param airflow_connections: a list of Airflow connections.
        :param dags_projects: a list of DAGs projects.
        :param validator: an ObservatoryConfigValidator instance.
        """

        self.backend = backend
        self.airflow = airflow
        self.google_cloud = google_cloud
        self.terraform = terraform

        self.airflow_variables = airflow_variables
        if airflow_variables is None:
            self.airflow_variables = []

        self.airflow_connections = airflow_connections
        if airflow_variables is None:
            self.airflow_connections = []

        self.dags_projects = dags_projects
        if dags_projects is None:
            self.airflow_connections = []

        self.validator = validator

    @property
    def is_valid(self) -> bool:
        """ Checks whether the config is valid or not.

        :return: whether the config is valid or not.
        """

        return self.validator is None or not len(self.validator._errors)

    @property
    def errors(self) -> List[ValidationError]:
        """ Returns a list of ValidationError instances that were created when parsing the config file.

        :return: the list of ValidationError instances.
        """

        errors = []
        for key, values in self.validator.errors.items():
            for value in values:
                if type(value) is dict:
                    for nested_key, nested_value in value.items():
                        errors.append(ValidationError(f'{key}.{nested_key}', *nested_value))
                else:
                    errors.append(ValidationError(key, *values))

        return errors

    def make_airflow_variables(self) -> List[AirflowVariable]:
        """ Make all airflow variables for the observatory platform.

        :return: a list of AirflowVariable objects.
        """

        # Create airflow variables from fixed config file values
        variables = [
            AirflowVariable(AirflowVars.ENVIRONMENT, self.backend.environment.value)
        ]

        if self.google_cloud.project_id is not None:
            variables.append(AirflowVariable(AirflowVars.PROJECT_ID, self.google_cloud.project_id))

        if self.google_cloud.data_location:
            variables.append(AirflowVariable(AirflowVars.DATA_LOCATION, self.google_cloud.data_location))

        if self.terraform.organization is not None:
            variables.append(AirflowVariable(AirflowVars.TERRAFORM_ORGANIZATION, self.terraform.organization))

        if self.terraform.workspace_prefix is not None:
            variables.append(AirflowVariable(AirflowVars.TERRAFORM_PREFIX, self.terraform.workspace_prefix))

        # Create airflow variables from bucket names
        for bucket in self.google_cloud.buckets:
            variables.append(AirflowVariable(bucket.id, bucket.name))

        # Add user defined variables to list
        variables += self.airflow_variables

        return variables

    @staticmethod
    def _parse_fields(dict_: Dict) -> Tuple[Backend, Airflow, GoogleCloud, Terraform,
                                            List[AirflowVariable], List[AirflowConnection],
                                            List[DagsProject]]:
        backend = Backend.from_dict(dict_.get('backend', dict()))
        airflow = Airflow.from_dict(dict_.get('airflow', dict()))
        google_cloud = GoogleCloud.from_dict(dict_.get('google_cloud', dict()))
        terraform = Terraform.from_dict(dict_.get('terraform', dict()))
        airflow_variables = AirflowVariable.parse_airflow_variables(dict_.get('airflow_variables', dict()))
        airflow_connections = AirflowConnection.parse_airflow_connections(dict_.get('airflow_connections', dict()))
        dags_projects = DagsProject.parse_dags_projects(dict_.get('dags_projects', list()))

        return backend, airflow, google_cloud, terraform, airflow_variables, airflow_connections, dags_projects

    @staticmethod
    def save_default(config_path: str):
        """ Save a default config file.

        :param config_path: the path where the config file should be saved.
        :return: None.
        """

        ObservatoryConfig._save_default(config_path, 'config.yaml.jinja2')

    @staticmethod
    def _save_default(config_path: str, template_file_name: str):
        """ Save a default config file.

        :param config_path: the path where the config file should be saved.
        :param template_file_name: the name of the template file name.
        :return: None.
        """

        # Render template
        template_path = os.path.join(module_file_path('observatory.platform'), template_file_name)
        fernet_key = generate_fernet_key()

        try:
            observatory_dags_path = module_file_path('observatory.dags', nav_back_steps=-3)
        except ModuleNotFoundError:
            observatory_dags_path = '/path/to/observatory-platform/observatory-dags'

        render = render_template(template_path, fernet_key=fernet_key, observatory_dags_path=observatory_dags_path)

        # Save file
        with open(config_path, 'w') as f:
            f.write(render)

    @classmethod
    def from_dict(cls, dict_: Dict) -> ObservatoryConfig:
        """ Constructs an ObservatoryConfig instance from a dictionary.

        If the dictionary is invalid, then an ObservatoryConfig instance will be returned with no properties set,
        except for the validator, which contains validation errors.

        :param dict_: the input dictionary.
        :return: the ObservatoryConfig instance.
        """

        schema = make_schema(BackendType.local)
        validator = ObservatoryConfigValidator()
        is_valid = validator.validate(dict_, schema)

        if is_valid:
            (backend, airflow, google_cloud, terraform,
             airflow_variables, airflow_connections, dags_projects) = ObservatoryConfig._parse_fields(dict_)

            return ObservatoryConfig(backend,
                                     airflow,
                                     google_cloud=google_cloud,
                                     terraform=terraform,
                                     airflow_variables=airflow_variables,
                                     airflow_connections=airflow_connections,
                                     dags_projects=dags_projects,
                                     validator=validator)
        else:
            return ObservatoryConfig(validator=validator)

    @classmethod
    def load(cls, path: str):
        """ Load a configuration file.

        :return: the ObservatoryConfig instance (or a subclass of ObservatoryConfig)
        """

        dict_ = dict()

        try:
            with open(path, 'r') as f:
                dict_ = yaml.safe_load(f)
        except yaml.YAMLError:
            print(f'Error parsing {path}')
        except FileNotFoundError:
            print(f'No such file or directory: {path}')
        except cerberus.validator.DocumentError as e:
            print(f'cerberus.validator.DocumentError: {e}')

        return cls.from_dict(dict_)


class TerraformConfig(ObservatoryConfig):
    def __init__(self,
                 backend: Backend = None,
                 airflow: Airflow = None,
                 google_cloud: GoogleCloud = None,
                 terraform: Terraform = None,
                 airflow_variables: List[AirflowVariable] = None,
                 airflow_connections: List[AirflowConnection] = None,
                 dags_projects: List[DagsProject] = None,
                 cloud_sql_database: CloudSqlDatabase = None,
                 airflow_main_vm: VirtualMachine = None,
                 airflow_worker_vm: VirtualMachine = None,
                 validator: ObservatoryConfigValidator = None):
        """ Create a TerraformConfig instance.

        :param backend: the backend config.
        :param airflow: the Airflow config.
        :param google_cloud: the Google Cloud config.
        :param terraform: the Terraform config.
        :param airflow_variables: a list of Airflow variables.
        :param airflow_connections: a list of Airflow connections.
        :param dags_projects: a list of DAGs projects.
        :param cloud_sql_database: a Google Cloud SQL database config.
        :param airflow_main_vm: the Airflow Main VM config.
        :param airflow_worker_vm: the Airflow Worker VM config.
        :param validator: an ObservatoryConfigValidator instance.
        """

        super().__init__(backend=backend,
                         airflow=airflow,
                         google_cloud=google_cloud,
                         terraform=terraform,
                         airflow_variables=airflow_variables,
                         airflow_connections=airflow_connections,
                         dags_projects=dags_projects,
                         validator=validator)
        self.cloud_sql_database = cloud_sql_database
        self.airflow_main_vm = airflow_main_vm
        self.airflow_worker_vm = airflow_worker_vm

    @property
    def terraform_workspace_id(self):
        """ The Terraform workspace id.

        :return: the terraform workspace id.
        """

        return self.terraform.workspace_prefix + self.backend.environment.value

    def make_airflow_variables(self) -> List[AirflowVariable]:
        """ Make all airflow variables for the Observatory Platform.

        :return: a list of AirflowVariable objects.
        """

        # Create airflow variables from fixed config file values
        variables = [
            AirflowVariable(AirflowVars.ENVIRONMENT, self.backend.environment.value)
        ]

        if self.google_cloud.project_id is not None:
            variables.append(AirflowVariable(AirflowVars.PROJECT_ID, self.google_cloud.project_id))

        if self.google_cloud.data_location:
            variables.append(AirflowVariable(AirflowVars.DATA_LOCATION, self.google_cloud.data_location))

        if self.terraform.organization is not None:
            variables.append(AirflowVariable(AirflowVars.TERRAFORM_ORGANIZATION, self.terraform.organization))

        if self.terraform.workspace_prefix is not None:
            variables.append(AirflowVariable(AirflowVars.TERRAFORM_PREFIX, self.terraform.workspace_prefix))

        # Add user defined variables to list
        variables += self.airflow_variables

        return variables

    def terraform_variables(self) -> List[TerraformVariable]:
        """ Create a list of TerraformVariable instances from the Terraform Config.

        :return: a list of TerraformVariable instances.
        """

        sensitive = True
        return [TerraformVariable('environment', self.backend.environment.value),
                TerraformVariable('airflow', self.airflow.to_hcl(), sensitive=sensitive, hcl=True),
                TerraformVariable('google_cloud', self.google_cloud.to_hcl(), sensitive=sensitive, hcl=True),
                TerraformVariable('cloud_sql_database', self.cloud_sql_database.to_hcl(), sensitive=sensitive, hcl=True),
                TerraformVariable('airflow_main_vm', self.airflow_main_vm.to_hcl(), hcl=True),
                TerraformVariable('airflow_worker_vm', self.airflow_worker_vm.to_hcl(), hcl=True),
                TerraformVariable('airflow_variables', list_to_hcl(self.airflow_variables), hcl=True,
                                  sensitive=sensitive),
                TerraformVariable('airflow_connections', list_to_hcl(self.airflow_connections), hcl=True,
                                  sensitive=sensitive)]

    @classmethod
    def from_dict(cls, dict_: Dict) -> TerraformConfig:
        """ Make an TerraformConfig instance from a dictionary.

        If the dictionary is invalid, then an ObservatoryConfig instance will be returned with no properties set,
        except for the validator, which contains validation errors.

        :param dict_: the input dictionary that has been read via yaml.safe_load.
        :return: the TerraformConfig instance.
        """

        schema = make_schema(BackendType.terraform)
        validator = ObservatoryConfigValidator()
        is_valid = validator.validate(dict_, schema)

        if is_valid:
            (backend, airflow, google_cloud, terraform,
             airflow_variables, airflow_connections, dags_projects) = ObservatoryConfig._parse_fields(dict_)

            cloud_sql_database = CloudSqlDatabase.from_dict(dict_.get('cloud_sql_database', dict()))
            airflow_main_vm = VirtualMachine.from_dict(dict_.get('airflow_main_vm', dict()))
            airflow_worker_vm = VirtualMachine.from_dict(dict_.get('airflow_worker_vm', dict()))

            return TerraformConfig(backend,
                                   airflow,
                                   google_cloud=google_cloud,
                                   terraform=terraform,
                                   airflow_variables=airflow_variables,
                                   airflow_connections=airflow_connections,
                                   dags_projects=dags_projects,
                                   cloud_sql_database=cloud_sql_database,
                                   airflow_main_vm=airflow_main_vm,
                                   airflow_worker_vm=airflow_worker_vm,
                                   validator=validator)
        else:
            return TerraformConfig(validator=validator)

    @staticmethod
    def save_default(config_path: str):
        """ Save a default TerraformConfig file.

        :param config_path: the path where the config file should be saved.
        :return: None.
        """

        ObservatoryConfig._save_default(config_path, 'config-terraform.yaml.jinja2')


def make_schema(backend_type: BackendType) -> Dict:
    """ Make a schema for an Observatory or Terraform config file.

    :param backend_type: the type of backend, local or terraform.
    :return: a dictionary containing the schema.
    """

    schema = dict()
    is_backend_terraform = backend_type == BackendType.terraform

    # Backend settings
    schema['backend'] = {
        'required': True,
        'type': 'dict',
        'schema': {
            'type': {
                'required': True,
                'type': 'string',
                'allowed': [backend_type.value]
            },
            'environment': {
                'required': True,
                'type': 'string',
                'allowed': ['develop', 'staging', 'production']
            }
        }
    }

    # Terraform settings
    schema['terraform'] = {
        'required': is_backend_terraform,
        'type': 'dict',
        'schema': {
            'organization': {
                'required': True,
                'type': 'string',
                'check_with': customise_pointer
            },
            'workspace_prefix': {
                'required': True,
                'type': 'string',
                'check_with': customise_pointer
            }
        }
    }

    # Google Cloud settings
    schema['google_cloud'] = {
        'required': is_backend_terraform,
        'type': 'dict',
        'schema': {
            'project_id': {
                'required': is_backend_terraform,
                'type': 'string',
                'check_with': customise_pointer
            },
            'credentials': {
                'required': is_backend_terraform,
                'type': 'string',
                'check_with': customise_pointer,
                'google_application_credentials': True
            },
            'region': {
                'required': is_backend_terraform,
                'type': 'string',
                'regex': r'^\w+\-\w+\d+$',
                'check_with': customise_pointer
            },
            'zone': {
                'required': is_backend_terraform,
                'type': 'string',
                'regex': r'^\w+\-\w+\d+\-[a-z]{1}$',
                'check_with': customise_pointer
            },
            'data_location': {
                'required': is_backend_terraform,
                'type': 'string',
                'check_with': customise_pointer
            }
        }
    }

    if not is_backend_terraform:
        schema['google_cloud']['schema']['buckets'] = {
            'required': False,
            'type': 'dict',
            'keysrules': {
                'type': 'string'
            },
            'valuesrules': {
                'type': 'string'
            }
        }

    # Airflow settings
    schema['airflow'] = {
        'required': True,
        'type': 'dict',
        'schema': {
            'fernet_key': {
                'required': True,
                'type': 'string'
            },
            'ui_user_password': {
                'required': is_backend_terraform,
                'type': 'string'
            },
            'ui_user_email': {
                'required': is_backend_terraform,
                'type': 'string'
            }
        }
    }

    # Database settings
    if is_backend_terraform:
        schema['cloud_sql_database'] = {
            'required': True,
            'type': 'dict',
            'schema': {
                'tier': {
                    'required': True,
                    'type': 'string'
                },
                'backup_start_time': {
                    'required': True,
                    'type': 'string',
                    'regex': r'^\d{2}:\d{2}$'
                },
                'postgres_password': {
                    'required': True,
                    'type': 'string'
                }
            }
        }

    # VM schema
    vm_schema = {
        'required': True,
        'type': 'dict',
        'schema': {
            'machine_type': {
                'required': True,
                'type': 'string',
            },
            'disk_size': {
                'required': True,
                'type': 'integer',
                'min': 1
            },
            'disk_type': {
                'required': True,
                'type': 'string',
                'allowed': ['pd-standard', 'pd-ssd']
            },
            'create': {
                'required': True,
                'type': 'boolean'
            }
        }
    }

    # Airflow main and worker VM
    if is_backend_terraform:
        schema['airflow_main_vm'] = vm_schema
        schema['airflow_worker_vm'] = vm_schema

    # Key value string pair schema
    key_val_schema = {
        'required': False,
        'type': 'dict',
        'keysrules': {
            'type': 'string'
        },
        'valuesrules': {
            'type': 'string'
        }
    }

    # Airflow variables
    schema['airflow_variables'] = key_val_schema

    # Airflow connections
    schema['airflow_connections'] = {
        'required': False,
        'type': 'dict',
        'keysrules': {
            'type': 'string'
        },
        'valuesrules': {
            'type': 'string',
            'regex': r'\S*:\/\/\S*:\S*@\S*$'
        }
    }

    # Dags projects
    schema['dags_projects'] = {
        'required': False,
        'type': 'list',
        'schema': {
            'type': 'dict',
            'schema': {
                'package_name': {
                    'required': True,
                    'type': 'string',
                },
                'path': {
                    'required': True,
                    'type': 'string',
                },
                'dags_module': {
                    'required': True,
                    'type': 'string',
                }
            }
        }
    }

    return schema
