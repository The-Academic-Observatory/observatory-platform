#Jstor

## Setting up a report schedule
Log in to the jstor website and set up a report schedule at https://www.jstor.org/publisher-reports/#request-schedules.  
It will be best to set the report frequency similar to the schedule interval of the telescope. Currently this is set to monthly.

The format needs to be set to 'TSV' and the recipient to the gmail account you will use with the Gmail API.  
The title of the report is not used in the telescope, so set this to anything you'd like (it does not show up in the email).  

## Using the Gmail API
See the [google support answer](https://support.google.com/googleapi/answer/6158841?hl=en) for info on how to enable an API.
Search for the gmail API and enable this.

### Creating the gmail api connection and credentials
* Set-up google cloud project to use gmail account.  
    If the gmail account is part of the same organization (e.g. using a g-suite account):  
    *  In the IAM section add the gmail account you want to use as a user.
  
    If the gmail account is not part of the same organization (e.g. regular account@gmail.com):  
    *  Make the project external from the 'APIs & Services' section and 'OAuth consent screen' subsection.
    *  In the same subsection, add the gmail account you want to use as a test user.
  
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

# This will open a pop-up, authorize the gmail account you want to use
creds = flow.run_local_server(access_type='offline', approval_prompt='force', port=8080)

# Get the necessary credentials info
token = urllib.parse.quote(creds.token, safe='')
refresh_token = urllib.parse.quote(creds.refresh_token, safe='')
client_id = urllib.parse.quote(creds.client_id, safe='')
client_secret = urllib.parse.quote(creds.client_secret, safe='')

# This connection can be used in the config file
gmail_api_conn = f'google-cloud-platform://?token{token}=&refresh_token={refresh_token}&client_id={client_id}&client_secret={client_secret}'
```
