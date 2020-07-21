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

## Prepare Google Cloud service account
A Google Cloud service account will need to be created and it's service account key will need to be downloaded to 
your workstation. See the article [Getting Started with Authentication](https://cloud.google.com/docs/authentication/getting-started) for
more details.

The following permissions will need to be assigned to the service account so that Terraform and Packer are able to 
provision the appropriate services:
```bash
Cloud SQL Admin
Compute Admin
Compute Image User
Compute Network Admin
Project IAM Admin
Secret Manager Admin
Storage Admin
```

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

Generate a Fernet key, to use in the next step, with the following command:
```bash
observatory generate fernet-key
```

For Terraform, create a file called `dev.tfvars` with the following fields, customising them for your project. Note
that the login and passwords in the connection variables (e.g. mag_releases_table_connection) need to be URL encoded
otherwise they will not be parsed correctly.
```hcl
# Project settings
project_id = "your-project-id"
credentials_file = "/path/to/service/account/key.json"
region = "us-west1"
zone = "us-west1-c"
data_location = "us"
environment = "prod"

# Database
database_tier = "db-custom-4-15360"
backup_start_time = "23:00"

# Machines
airflow_main_machine_type = "n2-standard-2"
airflow_main_disk_size = 20  # Disk size is in Gigabytes
airflow_main_disk_type = "pd-ssd"

airflow_worker_machine_type = "n1-standard-16"
airflow_worker_disk_size = 3000  # Disk size is in Gigabytes
airflow_worker_disk_type = "pd-standard"

# Airflow user
airflow_ui_user_password = ""
airflow_ui_user_email = "your.email@somehost.com"

# Secrets that are required to run Airflow
fernet_key = "v9IF2yPZtUH5aFnc6iGZsbBb3p5FpN6MemX4QWAHWPw="
postgres_password = "random password"
redis_password = "random password"

# Secrets that are required to run telescopes
mag_releases_table_connection = "mysql://login:password@"
mag_snapshots_container_connection = "mysql://login:password@"
crossref_connection = "mysql://:password@"
```

## Deploy
Build and deploy the VM image with Packer:
```bash
packer build -var-file=dev.pkrvars.hcl -force terraform/ao-image.pkr.hcl
```

Initialize Terraform:
```bash
terraform init terraform
```

To deploy the system with Terraform:
```bash
terraform apply -var-file=dev.tfvars terraform
```

To destroy the system with Terraform:
```bash
terraform destroy -var-file=dev.tfvars terraform
```

## Logging into the VMs
To ssh into airflow-main-vm:
```bash
gcloud compute ssh airflow-main-vm --project your-project-id --zone your-compute-zone
```

To ssh into airflow-worker-vm:
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
