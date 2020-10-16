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


import glob
import itertools
import logging
import os
import pathlib
from enum import Enum
from types import SimpleNamespace
from typing import Union

import airflow
import cerberus.validator
import hcl
import pendulum
import yaml
from airflow.hooks.base_hook import BaseHook
from airflow.utils.db import create_session
from airflow.models import Connection
from cerberus import Validator
from cryptography.fernet import Fernet
from natsort import natsorted
from pendulum import Pendulum
from yaml.loader import SafeLoader
from yaml.nodes import ScalarNode

import observatory_platform.database
import observatory_platform.database.workflows.sql
import observatory_platform.database.telescopes.sql
import terraform
from observatory_platform import dags
from observatory_platform.utils.airflow_utils import AirflowVariable
from observatory_platform.utils.jinja2_utils import render_template

# The path where data is saved on the system
data_path = None


def observatory_home(*subdirs) -> str:
    """ Get the .observatory Observatory Platform home directory or subdirectory on the host machine. The home
    directory and subdirectories will be created if they do not exist.

    :param: subdirs: an optional list of subdirectories.
    :return: the path.
    """

    user_home = str(pathlib.Path.home())
    observatory_home_ = os.path.join(user_home, ".observatory", *subdirs)

    if not os.path.exists(observatory_home_):
        os.makedirs(observatory_home_, exist_ok=True)

    return observatory_home_


def workflow_templates_path() -> str:
    """ Get the path to the SQL workflows templates.

    :return: the path to SQL templates.
    """

    file_path = pathlib.Path(observatory_platform.database.workflows.sql.__file__).resolve()
    return str(pathlib.Path(*file_path.parts[:-1]).resolve())


def telescope_templates_path() -> str:
    """ Get the path to the SQL telescope templates.

    :return: the path to SQL templates.
    """
    file_path = pathlib.Path(observatory_platform.database.telescopes.sql.__file__).resolve()
    return str(pathlib.Path(*file_path.parts[:-1]).resolve())


def observatory_package_path() -> str:
    """ Get the path to the Observatory Platform package root folder.

    :return: the path to the Observatory Platform package root folder.
    """

    file_path = pathlib.Path(observatory_platform.__file__).resolve()
    path = pathlib.Path(*file_path.parts[:-2])
    return str(path.resolve())


def dags_path() -> str:
    """ Get the path to the Observatory Platform DAGs.

    :return: the path to the Observatory Platform DAGs.
    """

    # Recommended way to add non code files: https://python-packaging.readthedocs.io/en/latest/non-code-files.html

    file_path = pathlib.Path(dags.__file__).resolve()
    path = pathlib.Path(*file_path.parts[:-1])
    return str(path.resolve())


def terraform_credentials_path() -> str:
    """ Get the path to the terraform credentials file that is created with 'terraform login'.

    :return: the path to the terraform credentials file
    """

    path = os.path.join(pathlib.Path.home(), '.terraform.d/credentials.tfrc.json')
    return path


def terraform_variables_tf_path() -> str:
    """ Get the path to the terraform variables.tf file.

    :return: the path to the terraform variables file.
    """

    file_path = pathlib.Path(terraform.__file__).resolve()
    path = os.path.join(pathlib.Path(*file_path.parts[:-1]), 'variables.tf')
    return path


def schema_path(database: str) -> str:
    """ Get the absolute path to a schema folder.

    :param database: the name of the database, e.g. telescopes, platform, analysis etc.
    :return: the schema folder.
    """
    file_path = pathlib.Path(observatory_platform.database.__file__).resolve()
    return str(pathlib.Path(*file_path.parts[:-1], database, 'schema').resolve())


def find_schema(path: str, table_name: str, release_date: Pendulum, prefix: str = '', ver: str = '') -> Union[str, None]:
    """ Finds a schema file on a given path, with a particular table name, release date and optional prefix.
    If no version string is sepcified, the most recent schema with a date less than or equal to the release date of the
    dataset is returned. If a version string is specified, the most current (date) schema in that series is returned.

    Use the schema_path function to find the path to the folder containing the schemas.

    For example (grid schemas):
     - grid2015-09-22.json
     - grid2016-04-28.json
     - wos_wok5.4_2016-01-01.json  (versioned schema with version 'wok5.4')

    For GRID releases between 2015-09-22 and 2016-04-28 grid_2015-09-22.json is returned and for GRID releases or after
    2016-04-28 grid_2016-04-28.json is returned (until a new schema with a later date is added).

    Unversioned schemas are named with the following pattern: prefix + table_name + YYYY-MM-DD + .json
    Versioned schemas are named as: prefix + table_name + '_' + ver + '_' + YYYY-MM-DD + .json
    * prefix: an optional prefix for datasets with multiple tables, for instance the Microsoft Academic Graph (MAG)
    dataset schema file names are prefixed with Mag, e.g. MagAffiliations2020-05-21.json, MagAuthors2020-05-21.json.
    The GRID dataset only has one table, so there is no prefix, e.g. grid2015-09-22.json.
    * table_name: the name of the table.
    * ver: version string.
    * YYYY-MM-DD: schema file names end in the release date that the particular schema should be used from in YYYY-MM-DD
    format. For versioned schemas, this is the date of schema creation.
    * prefix and table_name follow the naming conventions of the dataset, e.g. MAG uses CamelCase for tables and fields
    so CamelCase is used. When there is no preference from the dataset then lower snake case is used.

    :param path: the path to search within.
    :param table_name: the name of the table.
    :param release_date: the release date of the table.
    :param prefix: an optional prefix.
    :param ver: Schema version.
    :return: the path to the schema or None if no schema was found.
    """

    # Make search path for schemas
    if ver != '':
        search_path = os.path.join(path, f'{prefix}{table_name}_{ver}_*.json')
    else:
        search_path = os.path.join(path, f'{prefix}{table_name}*.json')

    # Find potential schemas with a glob search and sort them naturally
    schema_paths = glob.glob(search_path)
    schema_paths = natsorted(schema_paths)

    # No schemas were found
    if len(schema_paths) == 0:
        return None

    # Deal with versioned schema first since it's simpler. Return most recent for versioned schema.
    if ver != '':
        return schema_paths[-1]

    # Get schemas with dates <= release date
    prefix_len = len(prefix + table_name)
    suffix_len = 5
    selected_paths = []
    for path in schema_paths:
        file_name = os.path.basename(path)
        schema_date = pendulum.parse(file_name[prefix_len:-suffix_len].replace('_', ''))

        if schema_date <= release_date:
            selected_paths.append(path)
        else:
            break

    # Return the schema with the most recent release date
    if len(selected_paths):
        return selected_paths[-1]

    # No schemas were found
    return None


class SubFolder(Enum):
    """ The type of subfolder to create for telescope data """

    downloaded = 'download'
    extracted = 'extract'
    transformed = 'transform'


def check_variables(*variables):
    """
    Checks whether all given airflow variables exist.

    :param variables: name of airflow variable
    :return: True if all variables are valid
    """
    is_valid = True
    for name in variables:
        try:
            AirflowVariable.get(name)
        except KeyError:
            logging.error(f"Airflow variable '{name}' not set.")
            is_valid = False
    return is_valid


def check_connections(*connections):
    """
    Checks whether all given airflow connections exist.

    :param connections: name of airflow connection
    :return: True if all connections are valid
    """
    is_valid = True
    for name in connections:
        try:
            BaseHook.get_connection(name)
        except KeyError:
            logging.error(f"Airflow connection '{name}' not set.")
            is_valid = False
    return is_valid


def list_connections(source):
    """Get a list of data source connections with name starting with <source>_, e.g., wos_curtin.

    :param source: Data source (conforming to name convention) as a string, e.g., 'wos'.
    :return: A list of connection id strings with the prefix <source>_, e.g., ['wos_curtin', 'wos_auckland'].
    """
    with create_session() as session:
        query = session.query(Connection)
        query = query.filter(Connection.conn_id.like(f'{source}_%'))
        return query.all()


def telescope_path(sub_folder: SubFolder, name: str) -> str:
    """ Return a path for saving telescope data. Create it if it doesn't exist.

    :param sub_folder: the name of the sub folder for the telescope
    :param name: the name of the telescope.
    :return: the path.
    """

    # To avoid hitting the airflow database and the secret backend unnecessarily, data path is stored as a global
    # variable and only requested once.
    global data_path
    if data_path is None:
        logging.info('telescope_path: requesting data_path variable')
        data_path = airflow.models.Variable.get(AirflowVar.data_path.get())

    # Create telescope path
    path = os.path.join(data_path, 'telescopes', sub_folder.value, name)
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

    return path


class Environment(Enum):
    """ The environment being used """

    dev = 'dev'
    test = 'test'
    prod = 'prod'


class AirflowVar(Enum):
    """
    Stores airflow variables as objects. Ech variable should contain at least
    {'name': string, 'default': string/int/None, 'schema': {}}
    'schema' is only empty if the variable is hardcoded e.g. inside the docker file (and not defined in the config
    file).
    If 'schema' isn't empty it should contain at least {'type': string, 'required': bool}
    """
    data_path = {
        'name': 'data_path',
        'default': None,
        'schema': {}
    }
    environment = {
        'name': 'environment',
        'default': Environment.dev.value,
        'schema': {
            'type': 'string',
            'allowed': ['dev', 'test', 'prod'],
            'required': True
        }
    }
    project_id = {
        'name': 'project_id',
        'default': 'your-project-id <--',
        'schema': {
            'type': 'string',
            'required': True
        }
    }
    data_location = {
        'name': 'data_location',
        'default': 'us',
        'schema': {
            'type': 'string',
            'required': True
        }
    }
    download_bucket_name = {
        'name': 'download_bucket_name',
        'default': 'your-bucket-name <--',
        'schema': {
            'type': 'string',
            'required': True
        }
    }
    transform_bucket_name = {
        'name': 'transform_bucket_name',
        'default': 'your-bucket-name <--',
        'schema': {
            'type': 'string',
            'required': True
        }
    }
    terraform_organization = {
        'name': 'terraform_organization',
        'default': 'your-organization <--',
        'schema': {
            'type': 'string',
            'required': False
        }
    }
    terraform_prefix = {
        'name': 'terraform_prefix',
        'default': 'observatory- <--',
        'schema': {
            'type': 'string',
            'required': False
        }
    }
    orcid_bucket_name = {
        'name': 'orcid_bucket_name',
        'default': 'orcid_summary_sync <--',
        'schema': {
            'type': 'string',
            'required': False
        }
    }

    def get(self):
        """Method to slightly shorten way to get name"""
        return self.value['name']


class AirflowConn(Enum):
    """
    Stores airflow connections as objects. Ech variable contains at least
    {'name': string, 'default': string/int/None, 'schema': {}}
    'schema' is only empty if the connection is hardcoded e.g. inside the docker file (and not defined in the config
    file).
    If 'schema' isn't empty it should contain at least {'type': string, 'required': bool}
    """
    crossref_metadata = {
        'name': 'crossref_metadata',
        'default': 'mysql://:crossref-token@ <--',
        'schema': {
            'type': 'string',
            'required': False
        }
    }
    crossref_events = {
        'name': 'crossref_events',
        'default': 'mysql://:name%40email.com@ <--',
        'schema': {
            'type': 'string',
            'required': False
        }
    }
    mag_releases_table = {
        'name': 'mag_releases_table',
        'default': 'mysql://azure-storage-account-name:url-encoded'
                   '-sas-token@ <--',
        'schema': {
            'type': 'string',
            'required': False
        }
    }
    mag_snapshots_container = {
        'name': 'mag_snapshots_container',
        'default': 'mysql://azure-storage-account-name:url'
                   '-encoded-sas-token@ <--',
        'schema': {
            'type': 'string',
            'required': False
        }
    }
    terraform = {
        'name': 'terraform',
        'default': 'mysql://:terraform-token@ <--',
        'schema': {
            'type': 'string',
            'required': False
        }
    }
    slack = {
        'name': 'slack',
        'default': 'https://:T00000000%2FB00000000%2FXXXXXXXXXXXXXXXXXXXXXXXX@https%3A%2F%2Fhooks.slack.com'
                   '%2Fservices <--',
        'schema': {
            'type': 'string',
            'required': False
        }
    }

    orcid = {
        'name': 'orcid',
        'default': 'aws://AKIAIOSFODNN7EXAMPLE:wJalrXUtnFEMI%2FK7MDENG%2FbPxRfiCYEXAMPLEKEY@',
        'schema': {
            'type': 'string',
            'required': False
        }
    }

    def get(self):
        """Method to slightly shorten way to get name"""
        return self.value['name']


def get_default_airflow(variable: str, airflow_class: Union[AirflowConn, AirflowVar]) -> str:
    """
    Retrieves default value of an object in airflow_class
    :param variable: Name of the variable/object of airflow class
    :param airflow_class: Either AirflowVar or AirflowConn class
    :return: Dictionary, name is key, default value is value
    """
    value = None
    if getattr(airflow_class, variable).value['schema']:
        value = getattr(airflow_class, variable).value['default']
    return value


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

    def _validate_isuri(self, isuri, field, value):
        """ Validate that the given uri does not give an error when creating a Connection object. Will give a warning
        when uri doesn't contain a 'password' or 'extra' field.

        The rule's arguments are validated against this schema:
        {'type': 'boolean'}
        """

        if isuri:
            try:
                connection = Connection(uri=value)
                password = connection.password
                if not password:
                    self._error(field, f"the given uri for '{field}' does not contain a 'password'")
            except:
                self._error(field, f"the given uri for '{field}' is not valid.")


def iterate_over_local_schema(schema: dict, tmp_schema: dict, new_schema: dict, test_config: bool) -> dict:
    """
    Takes a cerberus style (nested) schema and returns a new (nested) schema that only contains keys and default
    values of the schema (leaving out fields like 'required', 'type', and 'schema'). This can be used to generate a
    default config.
    It adds a hashtag to the beginning of each key of variables that are optional.

    :param schema: current schema
    :param tmp_schema: temporary dict to check whether there is still a nested schema to iterate over
    :param new_schema: new schema
    :param test_config: whether dict is used for test or to be printed out in config (not adding '#' to key and
    trimming '<--' from values)
    :return: the new schema
    """
    if not new_schema:
        new_schema = tmp_schema
    for k, v in schema.items():
        var_name = k if v['required'] or test_config else '#'+k

        try:
            nested_schema = v['schema']
            tmp_schema[var_name] = {}
            new_schema = iterate_over_local_schema(nested_schema, tmp_schema[var_name], new_schema, test_config)
        except KeyError:
            if k == 'backend':
                value = 'local'
            elif k == 'google_application_credentials':
                value = 'google_application_credentials.json'
            elif k == 'fernet_key':
                value = Fernet.generate_key().decode()
            else:
                # get most recently added key of the new schema and check if this is 'airflow_connections'
                if 'airflow_connections' in list(new_schema)[-1]:
                    value = get_default_airflow(k, AirflowConn)
                    value = value[:-4] if test_config and value.endswith(' <--') else value
                else:
                    value = get_default_airflow(k, AirflowVar)
                    value = value[:-4] if test_config and value.endswith(' <--') else value
            tmp_schema[var_name] = value
    return new_schema


def iterate_over_terraform_schema(schema: dict, tmp_schema: dict, new_schema: dict, test_config: bool) -> dict:
    """
    Takes a cerberus style (nested) schema and returns a new (nested) schema that only contains keys and default
    values of the schema (leaving out fields like 'required', 'type', and 'schema'). This can be used to generate a
    default config.
    It adds a hashtag to the beginning of each key of variables that are optional and a '!sensitive' tag to the relevant
    sensitive variables.

    :param schema: current schema
    :param tmp_schema: temporary dict to check whether there is still a nested schema to iterate over
    :param new_schema: new schema
    :param test_config: whether dict is used for test or to be printed out in config (not adding '#' to key and
    trimming '<--' from values)
    :return: the new schema
    """
    if not new_schema:
        new_schema = tmp_schema
    for k, v in schema.items():
        var_name = '!sensitive ' + k if k in ['google_application_credentials', 'airflow_secrets',
                                              'airflow_connections'] else k
        var_name = var_name if v['required'] or test_config else '#'+var_name

        try:
            nested_schema = v['schema']
            tmp_schema[var_name] = {}
            new_schema = iterate_over_terraform_schema(nested_schema, tmp_schema[var_name], new_schema, test_config)
        except KeyError:
            if test_config:
                value = v['meta']['default']
                try:
                    trim = value.endswith(' <--')
                    default = value[:-4] if trim else value
                    if default in ['false', 'true']:
                        default = eval(default.capitalize())
                except AttributeError:
                    default = value
            elif v['required']:
                default = v['meta']['default']
            else:
                # change all keys if default value is a dict (e.g. airflow variables)
                default = change_keys(v['meta']['default'])
            tmp_schema[var_name] = default
    return new_schema


def change_keys(obj):
    """
    Recursively goes through the dictionary obj and adds a hashtag to the beginning of each key
    """
    if isinstance(obj, (str, int, float)):
        return obj
    if isinstance(obj, dict):
        new = obj.__class__()
        for k, v in list(obj.items()):
            new['#'+k] = change_keys(v)
    elif isinstance(obj, (list, set, tuple)):
        new = obj.__class__(change_keys(v) for v in obj)
    else:
        return obj
    return new


class ObservatoryConfig:
    LOCAL_DEFAULT_PATH = os.path.join(observatory_home(), 'config.yaml')
    TERRAFORM_DEFAULT_PATH = os.path.join(observatory_home(), 'config_terraform.yaml')
    TERRAFORM_VARIABLES_PATH = terraform_variables_tf_path()

    def __init__(self, config_path: str, terraform_variables_path: str = TERRAFORM_VARIABLES_PATH):
        """
        Holds the settings for the Observatory Platform either 'local' or remote on 'terraform'.
        :param config_path: Path to the config file.
        :param terraform_variables_path: Path to the terraform variables.tf file.
        """
        self.path = config_path
        self.sensitive_variables = []
        config_dict = self.load()
        if 'terraform' in config_dict['backend']:
            self.backend = 'terraform'
        elif config_dict['backend'] == 'local':
            self.backend = 'local'
        else:
            print('backend has to be either "local" or "terraform"')
            exit(os.EX_CONFIG)
        self.schema = self.get_schema(self.backend, terraform_variables_path)
        self.validator = ObservatoryConfigValidator(self.schema)
        # fill default values for empty airflow connections
        self.dict = self.validator.normalized(config_dict)

        if self.backend == 'local':
            self.local = SimpleNamespace(**self.dict)
            self.terraform = None
        else:
            self.local = None
            self.terraform = SimpleNamespace(**self.dict)

    def yaml_constructor(self, loader: SafeLoader, node: ScalarNode) -> str:
        """
        Constructor which is used with the '!sensitive' tag. It stores the names of variables with this tag in
        a list.
        :param loader: yaml safe loader
        :param node: yaml node
        :return: scalar value
        """
        value = loader.construct_scalar(node)
        self.sensitive_variables.append(value)
        return value

    def load(self) -> dict:
        """ Load an Observatory configuration file.

        :return: a dictionary of the configuration file
        """

        dict_ = None

        try:
            with open(self.path, 'r') as f:
                yaml.SafeLoader.add_constructor('!sensitive', self.yaml_constructor)
                dict_ = yaml.safe_load(f)
        except yaml.YAMLError:
            print(f'Error parsing {self.path}')
        except FileNotFoundError:
            print(f'No such file or directory: {self.path}')
        except cerberus.validator.DocumentError as e:
            print(f'cerberus.validator.DocumentError: {e}')

        return dict_

    @property
    def is_valid(self):
        self.validator.validate(self.dict)
        return self.validator is None or not len(self.validator._errors)

    @staticmethod
    def get_schema(backend: str, terraform_variables_path: Union[str, None] = None) -> dict:
        """
        Creates a schema
        :param backend: Which type of config file, either 'local' or 'terraform'.
        :param terraform_variables_path: Path to the terraform variables.tf file.
        :return: schema
        """
        if backend == 'local':
            schema = create_local_schema()
        else:
            schema = create_terraform_schema(terraform_variables_path)

        return schema

    @staticmethod
    def save_default(backend: str, config_path: str, terraform_variables_path: Union[str, None] = None, test=False):
        """
        Writes a default config file based on the available schema. The key references to the variable name and the
        value is a list of two values. The 1st whether the variable is required, the 2nd the value of the variable.
        :param backend: Which type of config file, either 'local' or 'terraform'.
        :param config_path: Path of the default config file to write to.
        :param terraform_variables_path: Path to the terraform variables.tf file.
        :param test: Whether dict is used for test or printing out as an example
        :return: None
        """
        schema = ObservatoryConfig.get_schema(backend, terraform_variables_path)

        if backend == 'local':
            # Create default dictionary
            default_dict = iterate_over_local_schema(schema, {}, {}, test_config=test)
        else:
            # Create default dictionary and obtain default values from 'meta' data in schema.
            default_dict = iterate_over_terraform_schema(schema, {}, {}, test_config=test)

        rendered = render_template(os.path.join(os.path.dirname(__file__), 'default_config.jinja2'),
                                   default_dict=default_dict)
        with open(config_path, 'w') as config_out:
            config_out.write(rendered)


def customise_pointer(field, value, error):
    if isinstance(value, str) and value.endswith(' <--'):
        error(field, "Value should be customised, can't end with ' <--'")


def create_terraform_schema(terraform_variables_path: str) -> dict:
    """
    Creates the schema for a terraform config file
    :param terraform_variables_path: Path to the terraform variables.tf file.
    :return: schema
    """
    with open(terraform_variables_path, 'r') as variables_file:
        hcl_obj = hcl.load(variables_file)

    schema = {
        'backend': {
            'required': True,
            'type': 'dict',
            'schema': {
                'terraform': {'required': True, 'type': 'dict', 'schema': {
                    'organization': {'required': True, 'type': 'string', 'meta': {'default': 'your-organization <--'}},
                    'workspaces_prefix': {'required': True, 'type': 'string', 'meta': {'default': 'observatory- <--'}}
                },
                              }
            }
        }
    }
    for variable in hcl_obj['variable']:
        var_type = None
        default = None
        try:
            var_type = hcl_obj['variable'][variable]['type']
            default = hcl_obj['variable'][variable]['default']
        except KeyError:
            print('Variables.tf file has to contain both a "type" and "default" field')
            exit(os.EX_CONFIG)

        try:
            description = hcl_obj['variable'][variable]['description']
        except KeyError:
            description = ''

        if var_type == 'string' or var_type == 'number' or var_type == 'bool':
            # type 'bool' in terraform is 'boolean' in cerberus schema
            if var_type == 'bool':
                var_type = 'boolean'
                default = str(default).lower()
            schema[variable] = {'required': True, 'type': var_type, 'check_with': customise_pointer}
            # add description and default value to 'meta'
            schema[variable]['meta'] = {'description': description, 'default': default}
            if variable =='google_application_credentials':
                schema[variable]['google_application_credentials'] = True

        # transform terraform 'object' type to dictionary. 'object' is a dict of variable names & types
        elif var_type.startswith('object({'):
            schema[variable] = {'required': True, 'type': 'dict', 'schema': {}, 'meta': {'description': description}}
            # strip object({ and })
            sub_variables = var_type[8:-2].split(',')
            for sub_variable in sub_variables:
                sub_name = sub_variable.split(':')[0]
                sub_type = sub_variable.split(':')[1].strip('"')
                # type 'bool' in terraform is 'boolean' in cerberus schema
                if sub_type == 'bool':
                    sub_type = 'boolean'
                schema[variable]['schema'][sub_name] = {'required': True, 'type': sub_type, 'check_with': customise_pointer}
                # hardcode that each connection validates URI string
                if variable == 'airflow_connections':
                    schema[variable]['schema'][sub_name]['isuri'] = True
                    # hardcode that airflow_connections (except terraform) are optional
                    if sub_name != 'terraform':
                        schema[variable]['schema'][sub_name]['required'] = False
                        # add default, because terraform requires a value
                        schema[variable]['schema'][sub_name]['default'] = "my-conn-type://my-login:my-password@my-host"
                # generate fernet key
                elif variable == 'airflow_secrets':
                    default['fernet_key'] = Fernet.generate_key().decode()
                # assign default value per sub variable
                schema[variable]['schema'][sub_name]['meta'] = {'default': default[sub_name]}
        # terraform 'map' type means that it is a dict, but unknown which keys are in it
        elif var_type.startswith('map('):
            schema[variable] = {'required': False, 'type': 'dict', 'check_with': customise_pointer}
            schema[variable]['meta'] = {'description': description, 'default': default}

        else:
            print(f'Unknown type {var_type} for terraform variable {variable}')
            exit(os.EX_CONFIG)

    return schema


def create_local_schema() -> dict:
    """
    Creates the schema for a local config file
    :return: schema
    """
    schema = {
        'backend': {
            'required': True,
            'type': 'string',
            'check_with': customise_pointer
        },
        'fernet_key': {
            'required': True,
            'type': 'string',
            'check_with': customise_pointer
        },
        'google_application_credentials': {
            'required': True,
            'type': 'string',
            'google_application_credentials': True,
            'check_with': customise_pointer
        },
        'airflow_connections': {
            'required': False,
            'type': 'dict',
            'schema': {
            }
        },
        'airflow_variables': {
            'required': True,
            'type': 'dict',
            'schema': {
            }
        }
    }
    # Add schema's of each individual airflow connection/variable to the schema dict
    for obj in itertools.chain(AirflowConn, AirflowVar):
        if obj.value['schema']:
            obj.value['schema']['check_with'] = customise_pointer
            if type(obj) == AirflowConn:
                # add default for connections
                obj.value['schema']['default'] = "my-conn-type://my-login:my-password@my-host"
                # add custom rule to check for valid uri
                obj.value['schema']['isuri'] = True
                schema['airflow_connections']['schema'][obj.get()] = obj.value['schema']
            else:
                # add default for variables
                obj.value['schema']['default'] = "default_variable"
                schema['airflow_variables']['schema'][obj.get()] = obj.value['schema']

    return schema
