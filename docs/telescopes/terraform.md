# Terraform
The terraform telescope is used to create or destroy the worker Virtual Machine (VM).  
There are two DAGs created from this telescope, vm_create and vm_destroy. 

## How the worker VM is created or destroyed
In the terraform configuration the worker VM is defined by a separate module. 
The number of instances created of this module is defined by the terraform variable 'vm_create'. 
If this boolean variable is set to True, running terraform will ensure that there is one instance available and, when set to false, that there are none.

## Slack webhook 
The slack webhook is used in the two tasks check_runtime_vm and terraform_check_run_status. To create a webhook for your 
channel follow the instructions [here](https://api.slack.com/messaging/webhooks) and update the slack airflow connection
in the config file using your newly created webhook URL. 

## Tasks
There is a lot of shared functionality between the two DAGs. Below is a list of all tasks with a short description on what 
they do and whether they're used in both DAGs or only in vm_create/vm_destroy.

### check_dependencies (both)
Like all DAGs, first it will check if all required dependencies are met. These dependencies include the airflow variables 
'project_id', 'terraform_organization', 'terraform_prefix' and airflow connection 'terraform'.

To connect to the terraform cloud workspace the organization and prefix are used to from the airflow variables and the 
password from the terraform connection is used as a token.

### check_vm_status (both)
Before any changes are made to the terraform variable 'vm_create' the current value for this variable is requested. 
This is done by using the terraform API and requesting the variables of the relevant workspace.
If it's already set to True and vm_create is running, it will skip the remaining tasks. Similarly, if it's set to False 
and vm_destroy is running it will skip the remaining tasks. This ensures that there aren't any unnecessary terraform runs made.

### check_all_dags_success (vm_destroy)
This task branches out to either check_runtime_vm or update_var_destroy, depending on whether the outcome is True or False.

The vm_destroy DAG is given a list of DAG ids for which to check whether they have all finished successfully. More specifically 
it collects the expected run dates of these DAGs between the previous time the VM was turned on and the last time the VM 
was turned on. For each run date it will check whether it is in a 'success' state.  
The datetimes of when the VM has been turned on are pulled from previous XCOM messages that are pushed in the terraform_run_configuration
task described below. 

Additionally, it checks if any DAGs of the given list are currently running (even though the run date falls outside of the 
expected run date).

If all DAG runs are in a 'success' state and there aren't any DAGs running, the update_var_destroy task will be executed, 
otherwise it will go to check_runtime_vm.

### check_runtime_vm (vm_destroy)
When this task is executed it means that the worker VM will not be destroyed. At this point it checks how long the VM has 
been running for and if it exceeds the threshold defined in the telescope, it will send a warning message to a slack channel
that is defined by the created token of the slack webhook.

### update_var_create / update_var_destroy (both)
The DAG vm_create will update the terraform variable 'vm_create' to True and vm_destroy will update the variable to False.
This is done by using the terraform API and updating this variable specifically.

### terraform_run_configuration (both)
After updating vm_create to either True of False a terraform run is executed using the terraform API. There shouldn't be any
changes to the terraform configuration except for the worker vm module, however the run is set to target only this module, 
as a safety measure to prevent unwanted changes to the rest of the infrastructure. 

### terraform_check_run_status (both)
While the terraform run is being executed this task will use the terraform API to request the run status until it transitioned 
from planning/applying into a finished state. This can either be successful or errored. When an error has occurred, a warning
will be sent to a slack channel that is defined by the created token of the slack webhook.