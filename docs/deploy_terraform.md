# Observatory Terraform Environment
This is a tutorial for deploying the Observatory Platform to Google Cloud with Terraform.

You should have [installed the Observatory Platform](installation.html) before following this tutorial.

## Install dependencies
The dependencies that are required include:
* [Packer](https://www.packer.io/): for automating the creation of the Google Cloud VM images.
* [Terraform](https://www.terraform.io/): to automate the deployment of the various Google Cloud services.
* [Google Cloud SDK](https://cloud.google.com/sdk/docs#install_the_latest_cloud_tools_version_cloudsdk_current_version): the Google
Cloud SDK including the gcloud command line tool.

### Linux
Install Packer:
```bash
sudo curl -L "https://releases.hashicorp.com/packer/1.6.0/packer_1.6.0_linux_amd64.zip" -o /usr/local/bin/packer
# When asked to replace, answer 'y'
unzip /usr/local/bin/packer -d /usr/local/bin/
sudo chmod +x /usr/local/bin/packer
```

Install Google Cloud SDK:
```bash
sudo curl -L "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-330.0.0-linux-x86_64.tar.gz" -o /usr/local/bin/google-cloud-sdk.tar.gz
sudo tar -xzvf /usr/local/bin/google-cloud-sdk.tar.gz -C /usr/local/bin
rm /usr/local/bin/google-cloud-sdk.tar.gz
sudo chmod +x /usr/local/bin/google-cloud-sdk
/usr/local/bin/google-cloud-sdk/install.sh
```

Install Terraform:
```bash
sudo curl -L "https://releases.hashicorp.com/terraform/0.13.5/terraform_0.13.5_linux_amd64.zip" -o /usr/local/bin/terraform
# When asked to replace, answer 'y'
sudo unzip /usr/local/bin/terraform -d /usr/local/bin/
sudo chmod +x /usr/local/bin/terraform
```

### Mac
Install Packer:
```bash
sudo curl -L "https://releases.hashicorp.com/packer/1.6.0/packer_1.6.0_darwin_amd64.zip" -o /usr/local/bin/packer
# When asked to replace, answer 'y'
unzip /usr/local/bin/packer -d /usr/local/bin/
sudo chmod +x /usr/local/bin/packer
```

Install Google Cloud SDK:
```bash
sudo curl -L "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-330.0.0-darwin-x86_64.tar.gz" -o /usr/local/bin/google-cloud-sdk.tar.gz
mkdir /usr/local/bin/google-cloud-sdk
sudo tar -xzvf /usr/local/bin/google-cloud-sdk.tar.gz -C /usr/local/bin
rm /usr/local/bin/google-cloud-sdk.tar.gz
sudo chmod +x /usr/local/bin/google-cloud-sdk
/usr/local/bin/google-cloud-sdk/install.sh
```

Install Terraform:
```bash
sudo curl -L "https://releases.hashicorp.com/terraform/0.13.5/terraform_0.13.5_darwin_amd64.zip" -o /usr/local/bin/terraform
# When asked to replace, answer 'y'
unzip /usr/local/bin/terraform -d /usr/local/bin/
sudo chmod +x /usr/local/bin/terraform
```

## Prepare Google Cloud project
Each environment (develop, staging, production) requires its own project.
See [Creating and managing projects](https://cloud.google.com/resource-manager/docs/creating-managing-projects) for more 
details on creating a project. The following instructions are for one project only, repeat these steps for each 
environment you would like to use.

## Prepare permissions for Google Cloud service account
A Google Cloud service account will need to be created and it's service account key will need to be downloaded to 
your workstation. See the article [Getting Started with Authentication](https://cloud.google.com/docs/authentication/getting-started) for
more details.

### Development/test project
For the development and staging environments, the following permissions will need to be assigned to the service account 
so that Terraform and Packer are able to provision the appropriate services:
```bash
BigQuery Admin
Cloud Build Service Account (API)
Cloud Run Admin (API)
Cloud SQL Admin
Compute Admin
Compute Image User
Compute Network Admin
Create Service Accounts
Delete Service Accounts
Project IAM Admin
Service Account Key Admin
Service Account User
Service Management Administrator (API)
Secret Manager Admin
Service Usage Admin
Storage Admin
Storage Transfer Admin
```

### Production project
For the production environment, two custom roles with limited permissions need to be created to prevent storage buckets 
as well as the Cloud SQL database instance from accidentally being destroyed.  

When running `terraform destroy` with these roles, Terraform will produce an error, because the service account 
doesn't have the required permissions to destroy these resources (buckets and sql database instance). New roles can be 
created in the Google Cloud Console, under 'IAM & Roles' and then 'Roles'.

The two custom roles are:  
* Custom Cloud SQL editor
Filter the Roles table on 'Cloud SQL Editor', select the role and click on 'create role from selection'.  
Click on 'ADD PERMISSIONS' and add `cloudsql.users.create` and `cloudsql.instances.create`.   
This new role replaces the 'Cloud SQL Admin' role compared to the development environment above.  

* Custom Storage Admin  
Filter the Roles table on 'Storage Admin', select the role and click on 'create role from selection'.  
At the 'assigned permissions' section filter for and remove `storage.buckets.delete` and `storage.objects.delete`.   
This new role replaces the 'Storage Admin' role compared to the development environment above.  

```bash
Custom Cloud SQL Editor
Custom Storage Admin
BigQuery Admin
Cloud Build Service Account (API)
Cloud Run Admin (API)
Compute Admin
Compute Image User
Compute Network Admin
Create Service Accounts
Delete Service Accounts
Project IAM Admin
Service Account Key Admin
Service Account User
Service Management Administrator (API)
Secret Manager Admin
Service Usage Admin
Storage Transfer Admin
```

## Prepare Google Cloud services
Enable the [Compute Engine API](https://console.developers.google.com/apis/api/compute.googleapis.com/overview) for the
google project. This is required for Packer to create the image. Other Google Cloud services are enabled by Terraform 
itself.

## Add user as verified domain owner
The terraform service account needs to be added as a verified domain owner in order to map the Cloud Run domain that is created
to a custom domain. The custom domain is used for the API service. See the [Google documentation](https://cloud.google.com/run/docs/mapping-custom-domains#adding_verified_domain_owners_to_other_users_or_service_accounts) 
for more info on how to add a verified owner. 

## Switch to the branch that you would like to deploy
Enter the observatory-platform project folder:
```bash
cd observatory-platform
```

Switch to the branch that you would like to deploy, for example:
```
git checkout develop
```

## Prepare configuration files
The Observatory Terraform configuration file needs to be created, to generate a default file run the following command:
```bash
observatory generate config terraform
```

The file is saved to `~/.observatory/config-terraform.yaml`. Customise the generated file, parameters with '<--' need 
to be customised and parameters commented out are optional.

See below for an example generated file:
```yaml
# The backend type: terraform
# The environment type: develop, staging or production
backend:
  type: terraform
  environment: develop

# Apache Airflow settings
airflow:
  fernet_key: 4yfYXnxjUZSsh1CefVigTuUGcH-AUnuKC9jJ2sUq-xA= # the fernet key which is used to encrypt the secrets in the airflow database
  ui_user_email: my-email@example.com <-- # the email for the Apache Airflow UI's airflow user
  ui_user_password: my-password <-- # the password for the Apache Airflow UI's airflow user

# Terraform settings
terraform:
  organization: my-terraform-org-name <-- # the terraform cloud organization

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

# Elasticsearch
elasticsearch:
  host: https://address.region.gcp.cloud.es.io:port <-- # the address of the elasticsearch host
  api_key: API_KEY <-- # the api key of the elasticsearch account

# API settings
api:
  domain_name: api.observatory.academy <-- # the custom domain name for the API, used for the google cloud endpoints service
  subdomain: project_id # can be either 'project_id' or 'environment', used to determine a prefix for the domain_name

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

The config file will be read when running `observatory terraform create-workspace` and
`observatory terraform update-workspace` and the variables are stored inside the Terraform Cloud workspace.

### Fernet key
One of the required variables is a Fernet key, the generated default file includes a newly generated Fernet key that 
can be used right away. Alternatively, generate a Fernet key yourself, with the following command:
```bash
observatory generate fernet-key
```

### Encoding airflow connections 
Note that the login and passwords in the 'airflow_connections' variables need to be URL encoded, otherwise they will 
not be parsed correctly. 

### Elasticsearch
Note that the host is the hostname for elasticsearch is different than the hostname for kibana.  
To generate an API key, execute in the Kibana Dev console:
```yaml
POST /_security/api_key
{
  "name": "my-dev-api-key",
  "role_descriptors": { 
    "role-read-access-all": {
      "cluster": ["all"],
      "index": [
        {
          "names": ["*"],
          "privileges": ["read", "view_index_metadata", "monitor"]
        }
      ]
    }
  }
}
```  

This returns:
```yaml
{
  "id" : "random_id",
  "name" : "my-dev-api-key",
  "api_key" : "random_api_key"
}
```

Concat id:api_key and base64 encode (this final value is what you use in the configuration file):
```bash
printf 'random_id:random_api_key' | base64
```

## Building the Google Compute VM image with Packer
First, build and deploy the Observatory Platform Google Compute VM image with Packer:
```bash
observatory terraform build-image ~/.observatory/config-terraform.yaml
```

Use this command if you have:
* Created, removed or updated user defined Observatory DAGs projects via the field `dags_projects`, in the Observatory
Terraform config file.
* Updated any code in the Observatory Platform.
* Update the `backend.environment` variable in the Observatory Terraform config file: you need to make sure that an
image is built for the other environment.

You will need to taint the VMs and update them so that they use the new image.

You do not need to run this command if:
* You have created, removed or updated user defined Apache Airflow connections or variables in the Observatory
Terraform config file: in this case you will need to update the Terraform workspace.
* You have changed any other settings in the Observatory Terraform config file (apart from `backend.environment`): 
in this case you will need to update the Terraform workspace variables and run `terraform apply`.

## Building the Cloud Run image
The Docker image for the API needs to be uploaded to the Google Cloud container registry. This is used to create the 
Cloud Run backend service, to build the Docker image run the following command:
```bash
observatory terraform build-api-image ~/.observatory/config-terraform.yaml
```

Use this command if:
 * This is the first time you are deploying the Terraform resources
 * You have updated any files in the API directory (`/home/user/workspace/observatory-platform/observatory-platform/observatory/platform/api`)

## Building the Terraform files
To refresh the files that are built into the `~/.observatory/build/terraform` directory, without rebuilding the entire
Google Compute VM image again, run the following command:
```bash
observatory terraform build-terraform ~/.observatory/config-terraform.yaml
```

Use this command if you have:
 * Updated the Terraform deployment scripts, but nothing else.

## Setting up Terraform
Enter the terraform directory:
```bash
cd ~/.observatory/build/terraform/terraform
```

Create token and login on Terraform Cloud:
```bash
terraform login
```

This should automatically store the token in `/home/user/.terraform.d/credentials.tfrc.json`, this file is used during 
the next commands to retrieve the token.

It's also possible to explicitly set the path to the credentials file using the option '--terraform-credentials-file'.

## Creating and updating Terraform workspaces
See below for instructions on how to run observatory terraform create-workspace and update-workspace.

### Create a workspace
Create a new workspace (this will use the created token file): 
See [Observatory Terraform Environment](./observatory_dev.html#observatory-terraform-environment) for more info on the 
usage of `observatory terraform`.
```bash
observatory terraform create-workspace ~/.observatory/config-terraform.yaml
```

You should see the following output:
```bash
Observatory Terraform: all dependencies found                                   
  Config:
   - path: /home/user/.observatory/config-terraform.yaml
   - file valid
  Terraform credentials file:
   - path: /home/user/.terraform.d/credentials.tfrc.json

Terraform Cloud Workspace: 
  Organization: jamie-test
  - Name: observatory-develop (prefix: 'observatory-' + suffix: 'develop')
  - Settings: 
   - Auto apply: True
  - Terraform Variables:
   * environment: develop
   * airflow: sensitive
   * google_cloud: sensitive
   * cloud_sql_database: sensitive
   * elasticsearch: sensitive
   * api: {"domain_name"="api.observatory.academy","subdomain"="project_id"}
   * airflow_main_vm: {"machine_type"="n2-standard-2","disk_size"=20,"disk_type"="pd-standard","create"=true}
   * airflow_worker_vm: {"machine_type"="n2-standard-2","disk_size"=20,"disk_type"="pd-standard","create"=false}
   * airflow_variables: {}
   * airflow_connections: sensitive
Would you like to create a new workspace with these settings? [y/N]: 
Creating workspace...
Successfully created workspace
```

### Update a workspace
To update variables in an existing workspace in Terraform Cloud:
```bash
observatory terraform update-workspace ~/.observatory/config-terraform.yaml
```

Depending on which variables are updated, you should see output similar to this:
```bash
  Config:
   - path: /home/user/.observatory/config-terraform.yaml
   - file valid
  Terraform credentials file:
   - path: /home/user/.terraform.d/credentials.tfrc.json

Terraform Cloud Workspace: 
  Organization: jamie-test
  - Name: observatory-develop (prefix: 'observatory-' + suffix: 'develop')
  - Settings: 
   - Auto apply: True
  - Terraform Variables:
  UPDATE
   * airflow: sensitive -> sensitive
   * google_cloud: sensitive -> sensitive
   * cloud_sql_database: sensitive -> sensitive
   * airflow_connections: sensitive -> sensitive
   * elasticsearch: sensitive -> sensitive
  UNCHANGED
   * api: {"domain_name"="api.observatory.academy","subdomain"="project_id"}
   * environment: develop
   * airflow_main_vm: {"machine_type"="n2-standard-2","disk_size"=20,"disk_type"="pd-standard","create"=true}
   * airflow_worker_vm: {"machine_type"="n2-standard-2","disk_size"=20,"disk_type"="pd-standard","create"=false}
   * airflow_variables: {}
Would you like to update the workspace with these settings? [y/N]: y
Updating workspace...
Successfully updated workspace
```

## Deploy
Once you have created your Terraform workspace, you can deploy the system with Terraform Cloud.

Initialize Terraform using key/value pairs:
```bash
terraform init -backend-config="hostname="app.terraform.io"" -backend-config="organization="coki""
```

Or using a backend file:
```bash
terraform init -backend-config=backend.hcl
```

With backend.hcl:
```hcl
hostname = "app.terraform.io"
organization = "coki"
```

If Terraform prompts to migrate all workspaces to "remote", answer "yes". 

Select the correct workspace in case multiple workspaces exist:
```bash
terraform workspace list
terraform workspace select <environment>
```

To preview the plan that will be executed with apply (optional):
```bash
terraform plan
```

To deploy the system with Terraform:
```bash
terraform apply
```

To destroy the system with Terraform:
```bash
terraform destroy
```

## Rebuild the VMs with a new Google Cloud VM image
If you have re-built the Google Cloud VM image, then you will need to manually taint the VMs and rebuild them:
```bash
terraform taint module.airflow_main_vm.google_compute_instance.vm_instance
terraform taint module.airflow_worker_vm.google_compute_instance.vm_instance
terraform apply
```

## Manually destroy the VMs
Run the following commands to manually destroy the VMs:
```
terraform destroy -target module.airflow_main_vm.google_compute_instance.vm_instance
terraform destroy -target module.airflow_worker_vm.google_compute_instance.vm_instance
```

## Logging into the VMs
To ssh into airflow-main-vm:
```bash
gcloud compute ssh airflow-main-vm --project your-project-id --zone your-compute-zone
```

To ssh into airflow-worker-vm (this is off by default, turn on using airflow DAG):
```bash
gcloud compute ssh airflow-worker-vm --project your-project-id --zone your-compute-zone
```

## Viewing the Apache Airflow and Flower UIs
To view the Apache Airflow and Flower web user interfaces you must forward ports 8080 and 5555 from the airflow-main-vm
into your local workstation.

To port forward with the gcloud command line tool:
```bash
gcloud compute ssh airflow-main-vm --project your-project-id --zone us-west1-c -- -L 5555:localhost:5555 -L 8080:localhost:8080
```

## Syncing files with a VM
To sync your local Observatory Platform project with a VM run the following commands, making sure to customise
the username and vm-hostname for the machine:
```bash
rsync --rsync-path 'sudo -u airflow rsync' -av -e ssh --chown=airflow:airflow --exclude='docs' --exclude='*.pyc' \
  --exclude='*.tfvars' --exclude='*.tfstate*' --exclude='venv' --exclude='.terraform' --exclude='.git' \
  --exclude='*.egg-info' /path/to/observatory-platform username@vm-hostname:/opt/observatory
```
