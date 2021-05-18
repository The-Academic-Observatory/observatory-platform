# JSTOR

JSTOR provides publisher usage reports, the reports offer details about the use of journal or book content by title, institution, and country. 
Journal reports also include usage by issue and article. 
Usage is aligned with the COUNTER 5 standard of Item Requests (views + downloads).  
Reports can be run or scheduled weekly, monthly, or quarterly with custom date ranges. 

A mapping is needed between the JSTOR publisher ID and the organisation name obtained from the observatory API.
To get the right publisher id, the 'publisher_id' needs to be set in the 'extra' field of this Telescope.

To get access to the analytics data a publisher needs to grant access to e.g. a Gmail account. 
This account can then be used to login and set-up the scheduled reports (see below). 

The corresponding tables created in BigQuery are `jstor_countryYYYYMMDD` and `jstor_institutionYYYYMMDD`.


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

## Setting up a report schedule
Log in to the JSTOR website and set up a report schedule at https://www.jstor.org/publisher-reports/#request-schedules.  
It will be best to set the report frequency the same as the schedule interval of the telescope. Currently this is set to monthly.

The format needs to be set to 'TSV' and the recipient to the Gmail account you will use with the Gmail API.  
The title of the report is not used in the telescope, so set this to anything you'd like (it does not show up in the email).  

## Using the Gmail API
See the [google support answer](https://support.google.com/googleapi/answer/6158841?hl=en) for info on how to enable an API.
Search for the Gmail API and enable this.

### Creating the Gmail API connection and credentials
* Set-up google cloud project to use Gmail account.  
    If the Gmail account is part of the same organization (e.g. using a g-suite account):  
    *  In the IAM section add the Gmail account you want to use as a user.
  
    If the Gmail account is not part of the same organization (e.g. regular account@gmail.com):  
    *  Make the project external from the 'APIs & Services' section and 'OAuth consent screen' subsection.
    *  In the same subsection, add the Gmail account you want to use as a test user.
  
*  Create OAuth credentials from the 'APIs & Services' section and Credentials subsection.
    *  Application type: Web application
    *  Name: Can be anything, e.g. 'gmail API'
    *  Authorized redirect URIs: add the URI: http://localhost:8080/
    *  Click 'Create'
    *  Back in the Credentials overview, download a JSON with the client secret info of the Client ID you just created. 
    The file will be named something like 'client_secret_token.apps.googleusercontent.com.json'
    
* Get the credentials info using the JSON file with client secret info by executing the following python code. 
Note that when you create these credentials, any previously stored credentials become invalid.
```python
import urllib.parse
from google_auth_oauthlib.flow import InstalledAppFlow

# When modifying these scopes, recreate the file token.json
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 'https://www.googleapis.com/auth/gmail.modify']
flow = InstalledAppFlow.from_client_secrets_file('/path/to/client_secret_token.apps.googleusercontent.com.json', SCOPES)

# This will open a pop-up, authorize the Gmail account you want to use
creds = flow.run_local_server(access_type='offline', approval_prompt='force', port=8080)

# Get the necessary credentials info
token = urllib.parse.quote(creds.token, safe='')
refresh_token = urllib.parse.quote(creds.refresh_token, safe='')
client_id = urllib.parse.quote(creds.client_id, safe='')
client_secret = urllib.parse.quote(creds.client_secret, safe='')

# This connection can be used in the config file
gmail_api_conn = f'google-cloud-platform://?token={token}&refresh_token={refresh_token}&client_id={client_id}&client_secret={client_secret}'
```

## Airflow connections
Note that all values need to be urlencoded.
In the config.yaml file, the following airflow connections are required:  

## gmail_api
Use the values from the Gmail API credentials as described above.

```yaml
gmail_api: google-cloud-platform://?token=<token>&refresh_token=<refresh_token>&client_id=<client_id>&client_secret=<client_secret>
```
