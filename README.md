![Observatory Platform](./logo.jpg?raw=true)

The Observatory Platform is an environment for fetching, processing and analysing data to understand how well 
universities operate as Open Knowledge Institutions. 

The Observatory Platform is built with Apache Airflow and includes DAGs (workflows) for processing: Crossref Metadata, 
Fundref, GRID, Microsoft Academic Graph (MAG) and Unpaywall.

[![Python Version](https://img.shields.io/badge/python-3.7-blue)](https://img.shields.io/badge/python-3.7-blue)
![Python package](https://github.com/The-Academic-Observatory/observatory-platform/workflows/Python%20package/badge.svg)
[![Documentation Status](https://readthedocs.org/projects/observatory-platform/badge/?version=latest)](https://observatory-platform.readthedocs.io/en/latest/?badge=latest)

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
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

Install Python 3.7 with brew:
```bash
brew install python3.7
```

Install pip:
```bash
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python3.7 get-pip.py --user
```

Install virtualenv 20 or greater:
```
pip install --upgrade virtualenv
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

Install the package:
```bash
pip3 install -e .
```

## Observatory Platform development environment

### Prepare configuration files
Generate a config.yaml file:
```bash
observatory generate config.yaml
```

You should see the following output:
```bash
The file "/home/user/.observatory/config.yaml" exists, do you want to overwrite it? [y/N]: y
config.yaml saved to: "/home/user/.observatory/config.yaml"
Please customise the following parameters in config.yaml:
  - project_id
  - data_location
  - download_bucket_name
  - transform_bucket_name
  - google_application_credentials
```

The following parameters in the generated config.yaml need to be customised:
* project_id: a Google Cloud project id. See 
[Creating and managing projects](https://cloud.google.com/resource-manager/docs/creating-managing-projects) for more 
details.
* data_location: the location where BigQuery data will be stored (same as the bucket location).
* download_bucket_name: the name of the Google Cloud Storage bucket where downloaded data will be stored.
* transform_bucket_name: the name of the Google Cloud Storage bucket where transformed data will be stored.
* google_application_credentials: the path to the JSON key for a Google Cloud service account that has permissions
to access the two buckets and the roles `roles/bigquery.admin` and `roles/storagetransfer.admin`.
See [Getting Started with Authentication](https://cloud.google.com/docs/authentication/getting-started) for 
instructions on how to create a service account key.

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
