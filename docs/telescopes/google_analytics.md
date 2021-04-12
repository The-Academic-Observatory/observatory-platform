# Google Analytics

## Setting up service account
* Create a service account from IAM & Admin - Service Accounts
* Create a JSON key and download the file with key
* For each organisation/publisher of interest, ask them to add this service account as a user for the correct view id

## The oaebu_service_account airflow connection
After creating the JSON key file as described above, open the JSON file and use the information to create the connection.  
URL encode each of the fields 'private_key_id', 'private_key', 'client_email' and 'client_id'.
```yaml
oaebu_service_account: google-cloud-platform://?type=service_account&private_key_id=<private_key_id>&private_key=<private_key>&client_email=<client_email>&client_id=<client_id>
```

## Getting the view ID (after given access)
```python
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

scopes = ['https://www.googleapis.com/auth/analytics.readonly']
creds = ServiceAccountCredentials.from_json_keyfile_name('/path/to/credentials.json',
                                                         scopes=scopes)
# Build the service object.
service = build('analytics', 'v3', credentials=creds)

account_summaries = service.management().accountSummaries().list().execute()
profiles = account_summaries['items'][1]['webProperties'][0]['profiles']
for profile in profiles:
    view_id = profile['id']
```