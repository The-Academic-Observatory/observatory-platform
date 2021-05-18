# Google Analytics

Google Analytics is a web analytics service offered by Google that tracks and reports website traffic.  
This telescope gets data from Google Analytics for 1 view id per publisher and for several combinations of metrics and dimensions.  
It is possible to add a regex expression to filter on pagepaths, so only data on relevant pagepaths is collected.  

Both the 'view_id' and 'pagepath_regex' need to be set in the 'extra' field of this Telescope. The pagepath_regex can be an empty string.  
To get access to the analytics data a publisher needs to add the relevant google service account as a user.

The corresponding table created in BigQuery is `google.google_analyticsYYYYMMDD`.

```eval_rst
+------------------------------+---------+
| Summary                      |         |
+==============================+=========+
| Average runtime              | ? min   |
+------------------------------+---------+
| Average download size        |  ? MB   |
+------------------------------+---------+
| Harvest Type                 | API     |
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

## Setting up service account
* Create a service account from IAM & Admin - Service Accounts
* Create a JSON key and download the file with key
* For each organisation/publisher of interest, ask them to add this service account as a user for the correct view id

## Getting the view ID (after given access)
```python
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

scopes = ['https://www.googleapis.com/auth/analytics.readonly']
credentials_path = '/path/to/service_account_credentials.json'
creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scopes=scopes)

# Build the service object.
service = build('analytics', 'v3', credentials=creds)

account_summaries = service.management().accountSummaries().list().execute()
profiles = account_summaries['items'][1]['webProperties'][0]['profiles']
for profile in profiles:
    view_id = profile['id']
```

## Airflow connections
Note that all values need to be urlencoded.
In the config.yaml file, the following airflow connections are required:  

### oaebu_service_account
After creating the JSON key file as described above, open the JSON file and use the information to create the connection.  
URL encode each of the fields 'private_key_id', 'private_key', 'client_email' and 'client_id'.
```yaml
oaebu_service_account: google-cloud-platform://?type=service_account&private_key_id=<private_key_id>&private_key=<private_key>&client_email=<client_email>&client_id=<client_id>
```