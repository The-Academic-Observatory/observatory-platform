#Jstor

## Using the Gmail API
See the [google support answer](https://support.google.com/googleapi/answer/6158841?hl=en) for info on how to enable an API.
Search for the gmail API and enable this.

### Creating the gmail api connection and credentials
*  Add the gmail account you want to use as a user to the google project where airflow is hosted
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

# When modifying these scopes, recreate the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 'https://www.googleapis.com/auth/gmail.modify']
flow = InstalledAppFlow.from_client_secrets_file('/path/to/client_secret_token.apps.googleusercontent.com.json', SCOPES)

# This will open a pop-up, authorize the gmail account you want to use.
creds = flow.run_local_server(access_type='offline', approval_prompt='force', port=8080)

# Get the necessary credentials info
token = urllib.parse.quote(creds.token, safe='')
refresh_token = urllib.parse.quote(creds.refresh_token, safe='')
client_id = urllib.parse.quote(creds.client_id, safe='')
client_secret = urllib.parse.quote(creds.client_secret, safe='')

# This connection can be used in the config file
gmail_api_conn = f'google-cloud-platform://?token{token}=&refresh_token={refresh_token}&client_id={client_id}&client_secret={client_secret}'
```
