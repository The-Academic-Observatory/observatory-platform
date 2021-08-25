# OAPEN Irus Uk
IRUS-UK provides OAPEN COUNTER standard access reports. Almost all books on OAPEN are provided as a whole book PDF file. 
The reports show access figures for each month as well as the location of the access. 
Since the location info includes an IP-address, the original data is handled only from within the OAPEN Google Cloud project.

Using a Cloud Function, the original data is downloaded and IP-addresses are replaced with geographical information, such as city and country.  
After this transformation, the data without IP-addresses is uploaded to a Google Cloud Storage Bucket.  

This is all done from within the OAPEN Google Cloud project. 
The Cloud Function is created and called from the telescope, when the Cloud Function has finished the data is copied from the Storage Bucket inside the OAPEN project, to a Bucket inside the main airflow project.

The corresponding table created in BigQuery is `oapen.oapen_irus_ukYYYYMMDD`.

```eval_rst
+------------------------------+---------+
| Summary                      |         |
+==============================+=========+
| Average runtime              | ? min   |
+------------------------------+---------+
| Average download size        |  ? MB   |
+------------------------------+---------+
| Harvest Type                 |  API    |
+------------------------------+---------+
| Harvest Frequency            | Monthly |
+------------------------------+---------+
| Runs on remote worker        | True    |
+------------------------------+---------+
| Catchup missed runs          | True    |
+------------------------------+---------+
| Table Write Disposition      | Truncate|
+------------------------------+---------+
| Update Frequency             | Daily   |
+------------------------------+---------+
| Credentials Required         | Yes     |
+------------------------------+---------+
| Uses Telescope Template      | Snapshot|
+------------------------------+---------+
| Each shard includes all data | No      |
+------------------------------+---------+
```

## Telescope object 'extra'
This telescope is created using the Observatory API. There is one 'extra' field that is required for the
 corresponding Telescope object, namely the 'publisher_id'.   

### publisher_id
A mapping is required between the OAPEN publisher ID and the organisation name obtained from the observatory API.
The OAPEN publisher_id is used directly for the older platform and is used for the new platform to look up the
 publisher uuid, which is then used to collect the data. 
 
The publisher_id can be found by going to the OAPEN [page to manually create reports](https://irus.jisc.ac.uk/IRUSConsult/irus-oapen/v2/br1b/).
On this page there is a drop down list with publisher names, to get the publisher_id simply url encode the publisher
 name from this list.

Note that occasionally there are multiple publisher names for one publisher.  
For example to get all data from Edinburgh University Press, you need data from both publisher_ids 
`Edinburgh University Press` and `Edinburgh University Press,`.

## Cloud Function
The OAPEN IRUS-UK telescope makes use of a Google Cloud Function that resides in the OAPEN Google project. 
There is a specific airflow task that will create the Cloud Function if it does not exist yet, or update it if the source code has changed.  
The source code for the Cloud Function can be found inside a separate repository that is part of the same organization (https://github.com/The-Academic-Observatory/oapen-irus-uk-cloud-function).

### Download access stats data
The Cloud Function downloads OAPEN IRUS-UK access stats data for 1 month and for a single publisher. Usage data after April 2020 is hosted on a new platform.  
The newer data is obtained by using their API, this requires a `requestor_id` and an `api_key`.  
Data before April 2020 is obtained from an URL, this requires an `email` and a `password`.  

The required values for either the newer or older way of downloading data are passed on as a `username` and `password` to the Cloud Function.
The `username` and `password` are obtained from an airflow connection, which should be set in the config file (see below).

### Replace IP addresses
Once the data is downloaded, the IP addresses are replaced with geographical information (corresponding city and country).  
This is done using the GeoIp database, which is downloaded from inside the Cloud Function. The license key for this database is passed on as a parameter as well, `geoip_license_key`.  
The `geoip_license_key` is also obtained from an airflow connection, which should be set in the config file (see below).

### Upload data to storage bucket
Next, the data without the IP addresses is upload to a bucket inside the OAPEN project. All files in this bucket are deleted after 1 day.
In the next airflow task, the data can then be copied from this bucket to the appropriate bucket in the project where airflow is hosted.

## Set-up OAPEN Google Cloud project
To make use of the Cloud Function described above it is required to enable two APIs and set up permissions for the Google service account that airflow is using.

See the [Google support answer](https://support.google.com/googleapi/answer/6158841?hl=en) for info on how to enable an API. The API's that need to be enabled are:
- Cloud Functions API
- Cloud build API

Inside the OAPEN Google project, add the airflow Google service account (<airflow_project_id>@<airflow_project_id>.iam.gserviceaccount.com, where airflow_project_id is the project where airflow is hosted). 
This can be done from the 'IAM & Admin' menu and 'IAM' tab. Then, assign the following permissions to this account:  
  - Cloud Functions Developer (to create or update the Cloud Function)
  - Cloud Functions Invoker (to call/invoke the Cloud Function)
  - Storage Admin (to create a bucket)
  - Storage Object Admin (to list and get a blob from the storage bucket)

Additionally, it is required to assign the role of service account user to the service account of the Cloud Function, with the airflow service account as a member.
The Cloud SDK command for this is:  
`gcloud iam service-accounts add-iam-policy-binding <OAPEN_project_id>@appspot.gserviceaccount.com --member=<airflow_project_id@airflow_project_id.iam.gserviceaccount.com> --role=roles/iam.serviceAccountUser`

Alternatively, it can be done with the Google Cloud console, from the 'IAM & Admin' menu and 'Service Accounts' tab.  
Click on the service account of the Cloud Function: `<OAPEN_project_id>@appspot.gserviceaccount.com`.  
In the 'permissions' tab, click 'Grant Access', add the airflow service account as a member `<airflow_project_id@airflow_project_id.iam.gserviceaccount.com>` and assign the role 'Service Account User'.

## Airflow connections
Note that all values need to be urlencoded.
In the config.yaml file, the following airflow connections are required:  

### oapen_irus_uk_login
To get the email address/password combination, contact OAPEN IRUS-UK.

### oapen_irus_uk_api
To get the requestor_id/api_key, contact OAPEN IRUS-UK.

### geoip_license_key
To get the user_id/license_key, first sign up for geolite2 at https://www.maxmind.com/en/geolite2/signup.  
From your account, in the 'Services' section, click on 'Manage License Keys'. The user_id is displayed on this page.  
Then, click on 'Generate new license key', this can be used for the 'license_key'.  
Answer *No* for the question: "Old versions of our GeoIP Update program use a different license key format. Will this key be used for GeoIP Update?"  

```yaml
oapen_irus_uk_login: mysql://email_address:password@
oapen_irus_uk_api: mysql://requestor_id:api_key@
geoip_license_key: mysql://user_id:license_key@
```

## Latest schema
``` eval_rst
.. csv-table::
   :file: ../schemas/oapen_irus_uk_latest.csv
   :width: 100%
   :header-rows: 1
```