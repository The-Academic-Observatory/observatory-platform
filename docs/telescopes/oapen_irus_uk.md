# Oapen Irus Uk

## Cloud Function
The oapen irus uk telescope makes use of a google cloud function that lives inside the oapen project id.  
Inside the airflow task, the cloud function is created if it does not exist, or updated if the source code has changed.  
The source code for the cloud function can be found inside the observatory-platform project.


### Download access stats data
The cloud function downloads oapen irus uk access stats data for 1 month and for a single publisher. Usage data after April 2020 is hosted on a new platform.  
The newer data can be obtained by using their API, this requires a `requestor_id` and an `api_key`.  
Data before April 2020 can be obtained from an URL, this requires an `email` and a `password`.  

The required values for either the newer or older way of downloading data are passed on as a `username` and `password` to the cloud function.
The `username` and `password` are obtained from an airflow connection, which should be set in the config file (see below).

### Replace IP addresses
Once the data is downloaded, the IP addresses are replaced with geographical information (corresponding city and country).  
This is done using the GeoIp database, which is downloaded from inside the cloud function. The license key for this database is passed on as a parameter as well, `geoip_license_key`.
The `geoip_license_key` is also obtained from an airflow connection, which should be set in the config file (see below).

### Upload data to storage bucket
Next, the data without the IP addresses is upload to a bucket inside the Oapen project. All files in this bucket are deleted after 1 day.
In the next airflow task, the data can then be copied from this bucket to the appropriate bucket in the project where airflow is hosted.

## Set-up Oapen Google Cloud project
To make use of the cloud function described above it is necessary to enable two APIs and set up permissions for the google service account (in your own project) that airflow is using.

See the [google support answer](https://support.google.com/googleapi/answer/6158841?hl=en) for info on how to enable an API. The API's that need to be enabled are:
- cloud functions API
- cloud build API

Inside the Oapen google project, add the airflow google service account (<airflow_project_id>@<airflow_project_id>.iam.gserviceaccount.com, where airflow_project_id is the project where airflow is hosted). 
This can be done from the 'IAM & Admin' menu and 'IAM' tab. Then, assign the following permissions to this account:  
  - Cloud Functions Developer (to create or update the cloud function)
  - Cloud Functions Invoker (to call/invoke the cloud function)
  - Storage Admin (to create a bucket)
  - Storage Object Admin (to list and get a blob from the storage bucket)

Additionally, it is necessary to assign the role of service account user to the service account of the cloud function, with the airflow service account as a member.
The cloud SDK command for this is:  
`gcloud iam service-accounts add-iam-policy-binding <oapen_project_id>@appspot.gserviceaccount.com --member=<airflow_project_id@airflow_project_id.iam.gserviceaccount.com> --role=roles/iam.serviceAccountUser`

Alternatively, it can be done with the google cloud console, from the 'IAM & Admin' menu and 'Service Accounts' tab.  
Click on the service account of the cloud function: `<oapen_project_id>@appspot.gserviceaccount.com`.  
In the 'permissions' tab, click 'Grant Access', add the airflow service account as a member `<airflow_project_id@airflow_project_id.iam.gserviceaccount.com>` and assign the role 'Service Account User'.

## Airflow connections
Note that all values need to be urlencoded.
In the config.yaml file, the following airflow connections are required:  

`oapen_irus_uk_login: mysql://email_address:password`  
To get the email address/password combination, contact oapen irus uk.

`oapen_irus_uk_api: mysql://requestor_id:api_key`  
To get the requestor_id/api_key, contact oapen irus uk.

`geoip_license_key: mysql://user_id:license_key`  
To get the user_id/license_key, first sign up for geolite2 at https://www.maxmind.com/en/geolite2/signup.  
From your account, in the 'Services', click on 'Manage License Keys'. The user_id is displayed on this page.
Then, click on 'Generate new license key', this can be used for the 'license_key'. Answer *No* for the question: "Old versions of our GeoIP Update program use a different license key format. Will this key be used for GeoIP Update?"
