# Deploy with Terraform
This is a tutorial for deploying the Observatory Platform to Google Cloud with Terraform.

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
sudo chmod +x /usr/local/bin/packer
```

Install Terraform:
```bash
sudo curl -L "https://releases.hashicorp.com/terraform/0.13.0-beta3/terraform_0.13.0-beta3_linux_amd64.zip" -o /usr/local/bin/terraform
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

Install Terraform:
```bash
sudo curl -L "https://releases.hashicorp.com/terraform/0.12.28/terraform_0.12.28_darwin_amd64.zip" -o /usr/local/bin/terraform
# When asked to replace, answer 'y'
unzip /usr/local/bin/terraform -d /usr/local/bin/
sudo chmod +x /usr/local/bin/terraform
```

## Prepare Google Cloud project
Each environment (dev, test, prod) requires its own project.
See [Creating and managing projects](https://cloud.google.com/resource-manager/docs/creating-managing-projects) for more 
details on creating a project. The following instructions are for 1 project only, repeat these steps for each environment 
you would like to use.

## Prepare permissions for Google Cloud service account
A Google Cloud service account will need to be created and it's service account key will need to be downloaded to 
your workstation. See the article [Getting Started with Authentication](https://cloud.google.com/docs/authentication/getting-started) for
more details.


### Development/test project
For the development or test environment the following permissions will need to be assigned to the service account so that 
Terraform and Packer are able to provision the appropriate services:
```bash
BigQuery Admin
Cloud SQL Admin
Compute Admin
Compute Image User
Compute Network Admin
Create Service Accounts
Delete Service Accounts
Project IAM Admin
Service Account Key Admin
Service Account User
Secret Manager Admin
Service Usage Admin
Storage Admin
Storage Transfer Admin
```

### Production project
For the production environment we created two custom roles with limited permissions to prevent storage buckets as well as 
the sql database instance from accidentally being destroyed.  
When running `terraform destroy` with these roles, terraform will produce an error, because the service account 
doesn't have the required permissions to destroy those resources (buckets and sql database instance).  
New roles can be created in the Google Cloud Console, under 'IAM & Roles' and then 'Roles'.

The two custom roles are:  
*  Custom Cloud SQL editor
Filter the Roles table on 'Cloud SQL Editor', select the role and click on 'create role from selection'.  
Click on 'ADD PERMISSIONS' and add `cloudsql.users.create` and `cloudsql.instances.create`.   
This new role replaces the 'Cloud SQL Admin' role compared to the development environment above.  

*  Custom Storage Admin  
Filter the Roles table on 'Storage Admin', select the role and click on 'create role from selection'.  
At the 'assigned permissions' section filter for and remove `storage.buckets.delete` and `storage.objects.delete`.   
This new role replaces the 'Storage Admin' role compared to the development environment above.  

```bash
Custom Cloud SQL Editor
Custom Storage Admin
BigQuery Admin
Compute Admin
Compute Image User
Compute Network Admin
Create Service Accounts
Delete Service Accounts
Project IAM Admin
Service Account Key Admin
Service Account User
Secret Manager Admin
Service Usage Admin
Storage Transfer Admin
```

## Prepare Google Cloud services
Enable the compute engine API (https://console.developers.google.com/apis/api/compute.googleapis.com/overview) for the google project.  
This is required for Packer to create the image. Other Google Cloud services are enabled by Terraform itself.

## Clone and checkout files
Clone the version of the Observatory Platform that you would like to deploy
```bash
git clone git@github.com:The-Academic-Observatory/observatory-platform.git
```

Enter the observatory-platform project folder:
```bash
cd observatory-platform
```

## Prepare configuration files
Two configuration files need to be created, one for Packer and one for Terraform. They are both defined in 
HashiCorp Configuration Language (HCL).

For Packer, create a file called `dev.pkrvars.hcl` with the following fields, customising them for your project:
```hcl
project_id = "your-project-id"
credentials_file = "/path/to/service/account/key.json"
zone = "us-west1-c"
```

For Terraform, create a file called `config_terraform.yaml` following the example below, customising it for your project.
Parameters with '<--' need to be customised and parameters with '#' are optional.
To automatically generate a default file use `observatory generate config_terraform.yaml`. 

One of the required variables is a Fernet key, the generated default file includes a newly generated Fernet key that can be used right away.  
Alternatively, generate a Fernet key yourself, with the following command:
```bash
observatory generate fernet-key
```

The config file will be read when running `observatory terraform create-workspace` and the variables are stored inside the Terraform Cloud workspace.

It is possible to add the custom tag '!sensitive', adding this tag will make sure that the values of the underlying variables are not displayed in Terraform Cloud.

Note that the login and passwords in the 'airflow_connections' variables (e.g. mag_releases_table_connection) need to be URL encoded
in the config file, otherwise they will not be parsed correctly.   
All values under 'airflow_secrets' will be URL encoded by Terraform before they are stored in the Google Cloud Secret Manager.   
Example file:   
```yaml
backend:
  terraform:
    organization: your-organization <--
    workspaces_prefix: observatory- <--
project_id: your-project-id <--
!sensitive google_application_credentials: /path/to/service/account/key.json <--
environment: dev
region: us-west1
zone: us-west1-c
data_location: us
database:
  tier: db-custom-4-15360
  backup_start_time: '23:00'
airflow_main:
  machine_type: n2-standard-2
  disk_size: 20
  disk_type: pd-ssd
airflow_worker:
  machine_type: n1-standard-2
  disk_size: 20
  disk_type: pd-standard
airflow_worker_create: false
!sensitive airflow_secrets:
  fernet_key: g1tVaGkjIjh88QTbkBoMuM_GH0KQVRn3wcNgZhG26eY=
  postgres_password: random_password <--
  redis_password: random_password <--
  airflow_ui_user_password: random_password <--
  airflow_ui_user_email: your.email@somehost.com <--
!sensitive airflow_connections:
  #mag_releases_table: mysql://myname:mypassword@myhost.com <--
  #mag_snapshots_container: mysql://myname:mypassword@myhost.com <--
  #crossref: mysql://myname:mypassword@myhost.com <--
  terraform: mysql://:terraform-token@ <--
  #slack: https://:T00000000%2FB00000000%2FXXXXXXXXXXXXXXXXXXXXXXXX@https%3A%2F%2Fhooks.slack.com%2Fservices <--
#airflow_variables:
  #example_name: example_value <--
```

## Deploy
Build and deploy the VM image with Packer:
```bash
packer build -var-file=dev.pkrvars.hcl -force terraform/ao-image.pkr.hcl
```

Enter the terraform directory:
```bash
cd terraform
```

Create token and login on Terraform Cloud:
```bash
terraform login
```

Create a new workspace (this will use the created token file):  
See [Observatory Terraform Environment](./observatory_dev.html#observatory-terraform-environment) for more info on the usage of `observatory terraform`.
```bash
observatory terraform create-workspace /home/user/.observatory/config_terraform.yaml
```

Initialize Terraform using key/value pairs:

```bash
terraform init -backend-config="hostname="app.terraform.io"" -backend-config="organization="coki""
```

or using a backend file:
```bash
terraform init -backend-config=backend.hcl
```

with backend.hcl:
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

The workspace is likely set to 
To deploy the system with Terraform:
```bash
terraform apply
```

To destroy the system with Terraform:
```bash
terraform destroy
```

## Create Worker VM
```bash
terraform apply -target module.airflow_worker_vm.google_compute_instance.vm_instance -var-file=dev.tfvars terraform
```

## Destroy
```bash
terraform destroy -target module.airflow_worker_vm.google_compute_instance.vm_instance -var-file=dev.tfvars terraform
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
