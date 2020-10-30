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
#   workspace_prefix: my-terraform-workspace-prefix-

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
#     path: /home/user/observatory-platform/observatory-dags/observatory/dags
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

# Observatory Terraform Environment
The following is a tutorial for setting up the Observatory Terraform environment.


## Prepare configuration files
Generate an individual config_terraform.yaml file for each terraform workspace:
```bash
observatory generate config terraform
```

You should see the following output:
```bash
The file "/home/user/.observatory/config_terraform.yaml" exists, do you want to overwrite it? [y/N]: y
config.yaml saved to: "/home/user/.observatory/config.yaml"
Please customise the parameters with '<--' in the config file. Parameters with '#' are optional.
```

The generated file should look like (without the inline comments):
```yaml
# The backend type: terraform
# The environment type: develop, staging or production
backend:
  type: terraform
  environment: develop

# Apache Airflow settings
airflow:
  fernet_key: dm_FeYOmZjDV3ax_FTT84s7O9SU7-LfBFexQQxZC1Ec= # the fernet key which is used to encrypt the secrets in the airflow database
  ui_user_email: my-email@example.com <-- # the email for the Apache Airflow UI's airflow user
  ui_user_password: my-password <-- # the password for the Apache Airflow UI's airflow user

# Terraform settings
terraform:
  organization: my-terraform-org-name <-- # the terraform cloud organization
  workspace_prefix: my-terraform-workspace-prefix- <-- # the terraform cloud prefix of the workspace name

# Google Cloud settings
google_cloud:
  project_id: my-gcp-id <-- # the Google Cloud project identifier
  credentials: /path/to/google_application_credentials.json <-- # the path to the Google Cloud service account credentials
  region: us-west1 <-- # the Google Cloud region where the resources will be deployed
  zone: us-west1-a <-- # the Google Cloud zone where the resources will be deployed
  data_location: us <-- # the location for storing data, including Google Cloud Storage buckets and Cloud SQL backups

# Google Cloud CloudSQL database settings
cloud_sql_database:
  tier: db-custom-2-7680 # the machine tier to use for the Observatory Platform Cloud SQL database
  backup_start_time: '23:00' # the time for Cloud SQL database backups to start in HH:MM format
  postgres_password: my-password <-- # the password for the airflow postgres database user

# Settings for the main VM that runs the Apache Airflow scheduler and webserver
airflow_main_vm:
  machine_type: n2-standard-2 # the machine type for the virtual machine
  disk_size: 50 # the disk size for the virtual machine in GB
  disk_type: pd-ssd # the disk type for the virtual machine
  create: true # determines whether virtual machine is created or destroyed
 
# Settings for the weekly on-demand VM that runs large tasks
airflow_worker_vm:
  machine_type: n1-standard-8 # the machine type for the virtual machine
  disk_size: 3000 # the disk size for the virtual machine in GB
  disk_type: pd-standard # the disk type for the virtual machine
  create: false # determines whether virtual machine is created or destroyed

# User defined Apache Airflow variables:
# airflow_variables:
#   my_variable_name: my-variable-value

# User defined Apache Airflow Connections:
# airflow_connections:
#   my_connection: http://my-username:my-password@

# User defined Observatory DAGs projects:
# dags_projects:
#   - package_name: observatory-dags
#     path: /home/user/observatory-platform/observatory-dags/observatory/dags
#     dags_module: observatory.dags.dags
```

## Creating and updating Terraform workspaces
See below for instructions on how to run observatory terraform create-workspace and update-workspace.

### Terraform credentials
To create a workspace you need a token that is linked to your Terraform Cloud user account.  
Create token and login on Terraform Cloud:
```bash
terraform login
```

This should automatically store the token in `/home/user/.terraform.d/credentials.tfrc.json`, this file is used during 
the next commands to retrieve the token.  

It's also possible to explicitly set the path to the credentials file using the option '--terraform-credentials-file'.

### Terraform variables.tf
To validate the variables in the config file they are compared with the variables in your Terraform variables.tf file.
The default path for the variables.tf file is relative to the observatory platform package location. For example if 
`/home/user/workspace/observatory-platform` is the location of your observatory platform installation, the path to variables.tf 
will be set to `/home/user/workspace/observatory-platform/terraform/variables.tf`.  

It's possible to explicitly set the path to the variables.tf file using the option '--terraform-variables-path'

### Adjusting verbosity of Terraform API
The level of logging output from the Terraform API can be set by adjusting the '-v' option from nothing to max '-vv'.

### Observatory Platform Terraform create-workspace
To create a new workspace in Terraform Cloud:
```bash
observatory terraform create-workspace /home/user/.observatory/config_terraform.yaml
```

You should see the following output:
```bash
Observatory Terraform: all dependencies found                                   
  Terraform variables.tf file:
   - path: /home/user/workspace/observatory-platform/terraform/variables.tf
  Config:
   - path: /home/user/.observatory/config_terraform.yaml
   - file valid
  Terraform credentials file:
   - path: /home/user/.terraform.d/credentials.tfrc.json

Terraform Cloud Workspace: 
  Organization: your-organization
  - Name: observatory-dev (prefix: 'observatory-' + suffix: 'dev')
  - Settings: 
   - Auto apply: True
   - Terraform Cloud Version: 0.13.0-beta3
  - Terraform Variables:
   * backend: {"terraform"={"organization"="your-organization","workspaces_prefix"="observatory-"}}
   * project_id: your-project-id
   * google_application_credentials: sensitive
   * environment: dev
   * region: us-west1
   * zone: us-west1-c
   * data_location: us
   * database: {"tier"="db-custom-4-15360","backup_start_time"="23:00"}
   * airflow_main: {"machine_type"="n2-standard-2","disk_size"=20,"disk_type"="pd-ssd"}
   * airflow_worker: {"machine_type"="n1-standard-2","disk_size"=20,"disk_type"="pd-standard"}
   * airflow_worker_create: false
   * airflow_secrets: sensitive
   * airflow_connections: sensitive
Would you like to create a workspace with these settings? [y/N]: y
Creating workspace...
Successfully created workspace
```

### Observatory Platform Terraform update-workspace
To update variables in an existing workspace in Terraform Cloud:
```bash
observatory terraform update-workspace /home/user/.observatory/config_terraform.yaml
```

Depending on which variables are updated, you should see output similar to this:
```bash
Observatory Terraform: all dependencies found                                   
  Terraform variables.tf file:
   - path: /home/user/workspace/observatory-platform/terraform/variables.tf
  Config:
   - path: /home/user/.observatory/config_terraform.yaml
   - file valid
  Terraform credentials file:
   - path: /home/user/.terraform.d/credentials.tfrc.json

Terraform Cloud Workspace: 
  Organization: your-organization
  - Name: observatory-dev (prefix: 'observatory-' + suffix: 'dev')
  - Settings: 
   - Auto apply: True
   - Terraform Cloud Version: 0.13.0-beta3
  - Terraform Variables:
  UPDATE
   * google_application_credentials: sensitive -> sensitive
   * region: us-west2 -> us-west1
   * airflow_secrets: sensitive -> sensitive
   * airflow_connections: sensitive -> sensitive
  DELETE
   * airflow_variables: {"optional"="test"}
  UNCHANGED
   * backend: {"terraform"={"organization"="your-organization","workspaces_prefix"="observatory-"}}
   * project_id: workflows-dev
   * zone: us-west1-c
   * data_location: us
   * environment: dev
   * database: {"tier"="db-custom-4-15360","backup_start_time"="23:00"}
   * airflow_main: {"machine_type"="n2-standard-2","disk_size"=20,"disk_type"="pd-ssd"}
   * airflow_worker: {"machine_type"="n1-standard-2","disk_size"=20,"disk_type"="pd-standard"}
   * airflow_worker_create: false
Would you like to update the workspace with these settings? [y/N]: y
Updating workspace...
Successfully updated workspace
```

# Getting help
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