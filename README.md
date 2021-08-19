![Observatory Platform](https://raw.githubusercontent.com/The-Academic-Observatory/observatory-platform/develop/logo.jpg)

The Observatory Platform is an environment for fetching, processing and analysing data to understand how well 
universities operate as Open Knowledge Institutions. 

The Observatory Platform is built with Apache Airflow and includes DAGs (workflows) for processing: Crossref Metadata, 
Fundref, GRID, Microsoft Academic Graph (MAG) and Unpaywall.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python Version](https://img.shields.io/badge/python-3.7-blue)](https://img.shields.io/badge/python-3.7-blue)
![Python package](https://github.com/The-Academic-Observatory/observatory-platform/workflows/Python%20package/badge.svg)
[![Documentation Status](https://readthedocs.org/projects/observatory-platform/badge/?version=latest)](https://observatory-platform.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/The-Academic-Observatory/observatory-platform/branch/develop/graph/badge.svg)](https://codecov.io/gh/The-Academic-Observatory/observatory-platform)

## Documentation
For more detailed documentation about the Observatory Platform see the Read the Docs website [https://observatory-platform.readthedocs.io](https://observatory-platform.readthedocs.io)

## Installation
Observatory Platform supports Python 3.7 and above on Linux and MacOS.

Dependencies:
* Python 3.7
* pip
* virtualenv 20 or greater
* Docker Engine or Docker Desktop.

See below for more details.

### System dependencies: Ubuntu 18.04
Update packages list and install software-properties-common:
```bash
sudo apt update
sudo apt install software-properties-common
```

Add deadsnakes PPA which contains Python 3.7 for Ubuntu 18.04; press `Enter` when prompted:
```bash
sudo add-apt-repository ppa:deadsnakes/ppa
```

Install Python 3.7:
```bash
sudo apt install python3.7 python3.7-dev
```

Install pip:
```bash
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python3.7 get-pip.py
```

Install virtualenv 20 or greater:
```
pip install --upgrade virtualenv
```

Install Docker Engine:
* Following the [Install Docker Engine on Ubuntu](https://docs.docker.com/engine/install/ubuntu/) tutorial.
* Make sure that Docker can be run without sudo, e.g. `sudo usermod -aG docker your-username`

### System dependencies: MacOS
Install [Homebrew](https://brew.sh/) with the following command:
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
```

Install Python 3.7 with brew:
```bash
brew install python@3.7
```

Add Python 3.7 to path:
```bash
echo 'export PATH="/usr/local/opt/python@3.7/bin:$PATH"' >> ~/.bash_profile
```

Install virtualenv 20 or greater:
```
pip3.7 install --upgrade virtualenv
```

Install Docker Desktop:
* Follow the [Install Docker Desktop on Mac](https://docs.docker.com/docker-for-mac/install/) tutorial.


### Installing the Observatory Platform
Make sure that you have followed the above instructions for installing the observatory-platform dependencies,
for your respective platform.

Clone the project:
```bash
git clone https://github.com/The-Academic-Observatory/observatory-platform
```

Enter the `observatory-platform` directory:
```bash
cd observatory-platform
```

Checkout the develop branch:
```
git checkout develop
```

Create a virtual environment:
```bash
virtualenv -p python3.7 venv
```

Activate the virtual environment:
```bash
source venv/bin/activate
```

Install the `observatory-platform` package:
```bash
pip3 install -e observatory-platform
```

Install the `observatory-api` package (optional):
```bash
pip3 install -e observatory-api
```

Install the `observatory-dags` package (optional):
```bash
pip3 install -e observatory-dags
```

## Observatory Platform development environment

### Prepare configuration files
Generate a config.yaml file:
```bash
observatory generate config local
```

You should see the following output:
```bash
The file "/home/user/.observatory/config.yaml" exists, do you want to overwrite it? [y/N]: y
config.yaml saved to: "/home/user/.observatory/config.yaml"
Please customise the parameters with '<--' in the config file. Parameters with '#' are optional.
```

The generated file should look like (with inline comments removed):
```yaml
# The backend type: local
# The environment type: develop, staging or production
backend:
  type: local
  environment: develop

# Apache Airflow settings
airflow:
  fernet_key: wbK_uYJ-x0tnUcy_WMwee6QYzI-7Ywbf-isKCvR1sZs=

# Terraform settings: customise to use the vm_create and vm_destroy DAGs:
# terraform:
#   organization: my-terraform-org-name

# Google Cloud settings: customise to use Google Cloud services
# google_cloud:
#   project_id: my-gcp-id # the Google Cloud project identifier
#   credentials: /path/to/google_application_credentials.json # the path to the Google Cloud service account credentials
#   data_location: us # the Google Cloud region where the resources will be deployed
#   buckets:
#     download_bucket: my-download-bucket-name # the bucket where downloads are stored
#     transform_bucket: my-transform-bucket-name # the bucket where transformed files are stored

# User defined Apache Airflow variables:
# airflow_variables:
#   my_variable_name: my-variable-value

# User defined Apache Airflow Connections:
# airflow_connections:
#   my_connection: http://my-username:my-password@

# User defined Observatory DAGs projects:
# dags_projects:
#   - package_name: observatory-dags
#     path: /home/user/observatory-platform/observatory-dags
#     dags_module: observatory.dags.dags
```

See [Creating and managing projects](https://cloud.google.com/resource-manager/docs/creating-managing-projects) for more 
details on creating a project and [Getting Started with Authentication](https://cloud.google.com/docs/authentication/getting-started) for 
instructions on how to create a service account key.

Make sure that service account has roles `roles/bigquery.admin` and `roles/storagetransfer.admin` as well as 
access to the download and transform buckets.

The table below lists connections that are required for telescopes bundled with the observatory:

| Connection Key | Example | Description |
| -------- | -------- | -------- |
| crossref     | `http://myname:mypassword@myhost.com`     | Stores the token for the crossref API as a password     |
| mag_releases_table     | `http://myname:mypassword@myhost.com`     | Stores the azure-storage-account-name as a login and url-encoded-sas-token as password     |
| mag_snapshots_container     | `http://myname:mypassword@myhost.com`     | Stores the azure-storage-account-name as a login and url-encoded-sas-token as password     |
| terraform     | `mysql://:terraform-token@`     | Stores the terraform user token as a password (used to create/destroy VMs)     |
| slack     | `https://:T00000000%2FB00000000%2FXXXXXXXXXXXXXXXX XXXXXXXX@https%3A%2F%2Fhooks.slack.com%2Fservices`     | Stores the URL for the Slack webhook as a host and the token as a password     |

### Start the Observatory Platform
To start the local Observatory Platform development environment:
```bash
observatory platform start
```

You should see the following output:
```bash
Observatory Platform: all dependencies found                                    
  Docker:
   - path: /usr/bin/docker
   - running
  Host machine settings:
   - observatory home: /home/user/.observatory
   - data-path: /home/user/.observatory/data
   - dags-path: /home/user/workspace/observatory-platform/observatory_platform/dags
   - logs-path: /home/user/.observatory/logs
   - postgres-path: /home/user/.observatory/postgres
   - host-uid: 1000
  Docker Compose:
   - path: /home/user/workspace/observatory-platform/venv/bin/docker-compose
  config.yaml:
   - path: /home/user/.observatory/config.yaml
   - file valid
Observatory Platform: built                                                     
Observatory Platform: started                                                   
View the Apache Airflow UI at http://localhost:8080
```

### Viewing the Apache Airflow and Flower UIs
Once the Observatory Platform has started, the following UIs can be accessed:
* Apache Airflow UI at [http://localhost:8080](http://localhost:8080)
* Flower UI at [http://localhost:5555](http://localhost:5555)

### Stop the Observatory Platform
To stop the Observatory Platform:
```bash
observatory platform stop
```

You should see the following output:
```bash
Observatory Platform: all dependencies found                                    
...
Observatory Platform: stopped                                                   
```

### Getting help
To get help with the Observatory Platform commands:
```bash
observatory --help
```

To get help with the Observatory Platform platform command:
```bash
observatory platform --help
```

To get help with the Observatory Platform generate command:
```bash
observatory generate --help
```
