# Observatory Platform Development Environment
The following is a tutorial for running the local Observatory Platform development environment.

Make sure that you have followed the installation instructions to install the Observatory Platform on your system.

## Prepare configuration files
Generate a config.yaml file:
```bash
observatory generate config local
```

You should see the following output:
```bash
The file "/home/user/.observatory/config.yaml" exists, do you want to overwrite it? [y/N]: y
config.yaml saved to: "/home/user/.observatory/config.yaml"
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

```eval_rst
+-------------------------+------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------+
| Connection Key          | Example                                                                                              | Description                                                                            |
+=========================+======================================================================================================+========================================================================================+
| crossref                | http://myname:mypassword@myhost.com                                                                  | Stores the token for the crossref API as a password                                    |
+-------------------------+------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------+
| mag_releases_table      | http://myname:mypassword@myhost.com                                                                  | Stores the azure-storage-account-name as a login and url-encoded-sas-token as password |
+-------------------------+------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------+
| mag_snapshots_container | http://myname:mypassword@myhost.com                                                                  | Stores the azure-storage-account-name as a login and url-encoded-sas-token as password |
+-------------------------+------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------+
| terraform               | mysql://:terraform-token@                                                                            |   Stores the terraform user token as a password (used to create/destroy VMs)           |
+-------------------------+------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------+
| slack                   | https://:T00000000%2FB00000000%2FXXXXXXXXXXXXXXXXXXXXXXXX@https%3A%2F%2Fhooks.slack.com%2Fservices   | Stores the URL for the Slack webhook as a host and the token as a password             |
+-------------------------+------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------+
```

## Running the local development environment
See below for instructions on how to start the Observatory Platform, view Observatory Platform UIs, stop the 
Observatory Platform and customise other settings. 

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

### Specify a config.yaml file
To specify a different config.yaml file use the `--config-path` parameter when starting the Observatory Platform:
```bash
observatory platform start --config-path /your/path/to/config.yaml
```

### Specify a DAGs folder
To specify a different dags folder use the `--dags-path` parameter when starting the Observatory Platform:
```bash
observatory platform start --dags-path /your/path/to/dags
```

### Specify a data folder
To specify a different folder to mount as the host machine's data folder, use the `--data-path` parameter when 
starting the Observatory Platform:
```bash
observatory platform start --data-path /your/path/to/data
```

### Specify a logs folder
To specify a different folder to mount as the host machine's logs folder, use the `--logs-path` parameter when 
starting the Observatory Platform:
```bash
observatory platform start --logs-path /your/path/to/logs
```

### Specify a PostgreSQL folder
To specify a different folder to mount as the host machine's PostgreSQL data folder, use the `--postgres-path` parameter 
when starting the Observatory Platform:
```bash
observatory platform start --postgres-path /your/path/to/postgres
```

### Specify a user id
To specify different user id, which is used to set the ownership of the volume mounts, use the following command
when starting the Observatory Platform:
```bash
observatory platform start --host-uid 5000
```

### Specify a group id
To specify different group id, which is used to set the ownership of the volume mounts, use the following command
when starting the Observatory Platform:
```bash
observatory platform start --host-gid 5000
```

### Override default ports
You may override the host ports for Redis, Flower UI, Airflow UI, Elasticsearch and Kibana. An example is given
below:
```bash
observatory platform start --redis-port 6380 --flower-ui-port 5556 --airflow-ui-port 8081 --elastic-port 9201 --kibana-port 5602
```

### Specify an existing Docker network
You may use an existing Docker network by supplying the network name:
```bash
observatory platform start --docker-network-name observatory-network
```

## Getting help
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