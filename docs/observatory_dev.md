# Observatory Platform Development Environment
The following is a tutorial for running the local Observatory Platform development environment.

Make sure that you have followed the installation instructions to install the Observatory Platform on your system.

## Prepare configuration files
Generate a config.yaml file:
```bash
observatory generate config.yaml
```

You should see the following output:
```bash
The file "/home/user/.observatory/config.yaml" exists, do you want to overwrite it? [y/N]: y
config.yaml saved to: "/home/user/.observatory/config.yaml"
Please customise the parameters with '<--' in the config file. Parameters with '#' are optional.
```

See [Creating and managing projects](https://cloud.google.com/resource-manager/docs/creating-managing-projects) for more 
details on creating a project and [Getting Started with Authentication](https://cloud.google.com/docs/authentication/getting-started) for 
instructions on how to create a service account key.

Parameters explained: 
```yaml
backend: The backend of the config file, either 'local' or 'terraform'.
fernet_key: The fernet key which is used to encrypt the secrets in the airflow database 
google_application_credentials: The path to the JSON key for a Google Cloud service account that has permissions
airflow_connections:
  crossref: Stores the token for the crossref API as a password
  mag_releases_table: Stores the azure-storage-account-name as a login and url-encoded-sas-token as password
  mag_snapshots_container: Stores the azure-storage-account-name as a login and url-encoded-sas-token as password
  terraform: Stores the terraform user token as a password (used to create/destroy VMs)
  slack: Stores the URL for the Slack webhook as a host and the token as a password
airflow_variables:
  environment: The environment type, has to either be 'develop', 'test' or 'production'
  project_id: The Google Cloud project project identifier
  data_location: The location where BigQuery data will be stored (same as the bucket location)
  download_bucket_name: The name of the Google Cloud Storage bucket where downloaded data will be stored
To access the two buckets add the roles `roles/bigquery.admin` and `roles/storagetransfer.admin` to the service account connected to google_application_credentials above.
  transform_bucket_name: The name of the Google Cloud Storage bucket where transformed data will be stored
  terraform_organization: The name of the Terraform Cloud organization (used to create/destroy VMs)
  terraform_prefix: The name of the Terraform Cloud workspace prefix (used to create/destroy VMs)
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

# Observatory Terraform Environment
The following is a tutorial for setting up the Observatory Terraform environment.


## Prepare configuration files
Generate an individual config_terraform.yaml file for each terraform workspace:
```bash
observatory generate config_terraform.yaml
```

You should see the following output:
```bash
The file "/home/user/.observatory/config_terraform.yaml" exists, do you want to overwrite it? [y/N]: y
config.yaml saved to: "/home/user/.observatory/config.yaml"
Please customise the parameters with '<--' in the config file. Parameters with '#' are optional.
```

Parameters explained:
```yaml
backend:
  terraform:
    organization: The terraform cloud organization
    workspaces_prefix: The terraform cloud prefix of the workspace name
project_id: The Google Cloud project identifier
google_application_credentials: The path to the Google Cloud service account credentials
environment: The environment type
region: The Google Cloud region where the resources will be deployed
zone: The Google Cloud zone where the resources will be deployed
data_location: The location for storing data, including Google Cloud Storage buckets and Cloud SQL backups
database:
  tier: The machine tier to use for the Observatory Platform Cloud SQL database
  backup_start_time: The time for Cloud SQL database backups to start in HH:MM format
airflow_main:
  machine_type: The machine type for the Airflow Main virtual machine
  disk_size: The disk size for the Airflow Main virtual machine in GB
  disk_type: The disk type for the Airflow Main virtual machine
airflow_worker:
  machine_type: The machine type for the Airflow Worker virtual machine(s).
  disk_size: The disk size for the Airflow Worker virtual machine(s) in GB
  disk_type: The disk type for the Airflow Worker virtual machine(s)
airflow_worker_create: Determines whether the airflow worker VM is created or destroyed
airflow_secrets:
  fernet_key: The fernet key which is used to encrypt the secrets in the airflow database
  postgres_password: The password for the airflow postgres database user
  redis_password: The password for redis, which is used by Celery to send messages, e.g. task messages
  airflow_ui_user_password: The password for the Apache Airflow UI's airflow user
  airflow_ui_user_email: The email for the Apache Airflow UI's airflow user
airflow_connections:
  mag_releases_table: The mag_releases_table connection
  mag_snapshots_container: The mag_snapshots_container connection
  crossref: Contains the crossref API token
  terraform: Contains the terraform API token
  slack: Contains the webhook URL and webhook token
airflow_variables:
  example_name: Additional Airflow variable that isn't interpolated from Terraform resources
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