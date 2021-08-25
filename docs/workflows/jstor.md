# JSTOR

JSTOR provides publisher usage reports, the reports offer details about the use of journal or book content by title, institution, and country. 
Journal reports also include usage by issue and article. 
Usage is aligned with the COUNTER 5 standard of Item Requests (views + downloads).  
Reports can be run or scheduled weekly, monthly, or quarterly with custom date ranges. 

To directly get access to the analytics data a publisher needs to grant access to e.g. a Gmail account. 
This account can then be used to login to the JSTOR portal and set-up the scheduled reports (see below) that are
 mailed to a G-suite account.  
Alternatively, the publisher can set-up a schedule to create reports that are send to the G-suite account.  
In the telescope the Gmail of the G-suite account is parsed for messages with a download link to the JSTOR report.

The production server of the observatory-platform has been white listed by JSTOR to avoid bot detection.

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

## Telescope object 'extra'
This telescope is created using the Observatory API. There is one 'extra' field that is required for the
 corresponding Telescope object, namely the 'publisher_id'.   

### publisher_id
A mapping is required between the JSTOR publisher ID and the organisation name obtained from the observatory API.
The JSTOR publisher_id can be found in the original filename of a JSTOR report, for example:  
`PUB_<publisher_id>_PUBBIU_20210501.tsv`

It is possible to get the original filename by directly downloading a (previous) report from the JSTOR portal.

## Setting up a report schedule
Log in to the JSTOR website and set up a report schedule at their [portal](https://www.jstor.org/publisher-reports/#request-schedules).  
It will be easiest to set the report frequency the same as the schedule interval of the telescope. Currently this is set
 to monthly.  
For this telescope only the 'Book Usage by Country' (PUB_BCU) and 'Book Usage by Institution' (PUB_BIU) are used.  

The format needs to be set to 'TSV' and the recipient to the Gmail account that will be used with the Gmail API.  
The title of the report is not used in the telescope, so set this to anything you'd like (it does not show up in the email).  

## Downloading previous reports
Above is described how to set up a report schedule. Unfortunately this schedule can only be set up starting from the
 current date.  
To get previous reports (from before the start date of the schedule) it is possible to create a 'one-time' report and
 mail this to the relevant gmail account. It will then still be processed by this Telescope.  
The settings are the same as for the scheduled report.

### Using Selenium to get previous reports
When downloading many reports it might be faster to use the script below that helps to create the reports.  
It is required to run the script in debug mode, so a breakpoint can be set at the right spot (marked in the code) and
 you can manually login with your Google account.  
From there on, the reports are automatically sent to the gmail account on a monthly basis between the given start and
 end date, for the given publishers.  
To use Selenium you need the chrome webdriver, this can be downloaded from [here](https://chromedriver.chromium.org/downloads)

<details>
    <summary> Click to expand and see the full script </summary>

```python
import platform
import time
from datetime import datetime

import pendulum
from selenium import webdriver
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select


def main():
    """ Create the JSTOR 'Book Usage by Country' and 'Book Usage by Institution' reports that are in the past and 
    can't be scheduled.
    Needs to be run in debug mode, because it requires manual sign in at breakpoint (to avoid bot detection).

    Reports are created at a monthly granularity between the start_date and end_date.
    The reports are created for each publisher in the 'publisher_names' list and a link to the report is send to the 
    email_address.
    
    There are some 'sleep' statements so the driver waits for the page to load.
    A common issue is that the driver fails at the 'select report' step. The dropdown menu becomes unclickable. It 
    might become clickable after waiting a few seconds or it is necessary to refresh the page and possibly do the 
    reCAPTCHA that shows up.
    
    :return: None.
    """

    """ Customise values """
    driver_path = '/path/to/chromedriver'
    # The publisher name is the exact text displayed when you click 'Select a publisher'
    publisher_names = ["UCL Press (uclpress)", "ANU Press (anuepress)"]
    email_address = 'address@gmail.com'
    start_date = pendulum.datetime(2018, 2, 1)
    end_date = pendulum.now()
    """ Customise values """

    # Initialise webdriver and go to jstor url to login
    driver = webdriver.Chrome(executable_path=driver_path)
    driver.implicitly_wait(10)
    driver.get('https://www.jstor.org/publisher-reports/')

    # Close cookies bar
    try:  # <-------- set breakpoint here and manually sign in
        driver.find_element_by_xpath('//*[@id="onetrust-close-btn-container"]/button').click()
    except:
        pass

    # Click 'create report'
    driver.find_element_by_id('create-report-button').click()

    # Loop through months
    period = pendulum.period(start_date, end_date)
    for dt in period.range('months'):
        # Loop through publishers
        for publisher_name in publisher_names:
            for report_type in ["PUB_BCU", "PUB_BIU"]:
                # Select the publisher
                select_publisher = Select(driver.find_element_by_id('institution-list'))
                select_publisher.select_by_visible_text(publisher_name)

                # Set report type to 'one-time'
                driver.find_element_by_id('is-scheduled-no').click()

                # Select the report type
                time.sleep(5)
                if driver.find_element_by_id('template-list').get_attribute('disabled'):
                    print('pause')  # <----- optionally add breakpoint to wait longer than 5s
                select_report = Select(driver.find_element_by_id('template-list'))
                select_report.select_by_value(report_type)

                # Skip month if month is not finished yet
                if dt.end_of('month') >= pendulum.now():
                    continue

                # Get start and end date of one month
                start_month = dt
                end_month = dt.end_of('month')

                # Set the start and end date
                set_calendar_dates(driver, start_month, end_month)

                # Set the report format
                driver.find_element_by_xpath('//*[@id="available-reports"]/div/fieldset[4]/div[2]/label').click()

                # Fill in email address
                if platform.system() == 'Darwin':
                    key = Keys.COMMAND
                else:
                    key = Keys.CONTROL
                driver.find_element_by_name('email_address').send_keys(key, "a")
                driver.find_element_by_name('email_address').send_keys(email_address)

                # Click continue
                driver.find_element_by_xpath('//*[@id="create-report"]/div[2]/pharos-button').click()

                # Click submit, test if duplicate report
                time.sleep(5)
                try:
                    # Submit if not duplicate
                    driver.find_element_by_xpath('//*[@id="create-report"]/div[2]/pharos-button[2]').click()
                    print(f'Created report, name: {publisher_name}, type: {report_type}, start: {start_month}, '
                          f'end: {end_month}')
                except ElementClickInterceptedException:
                    # Close if duplicate
                    if driver.find_element_by_xpath('//*[@id="available-reports"]/div/div/div/div['
                                                    '1]/span/strong').text == 'Duplicate report found!':
                        driver.find_element_by_xpath('//*[@id="create-report"]/button').click()
                        print(f'Report already exists, name: {publisher_name}, type: {report_type}, '
                              f'start: {start_month}, end: {end_month}')
                    else:
                        raise ElementClickInterceptedException

                # Click 'create report'
                time.sleep(5)
                create_report_button = driver.find_element_by_id('create-report-button')
                action = ActionChains(driver)
                action.move_to_element(create_report_button).click().perform()


def set_calendar_dates(driver: webdriver, start_date: pendulum, end_date: pendulum):
    """ Set the calendar date for the start and end date of the report.
    
    :param driver: The webdriver
    :param start_date: The start date of this report
    :param end_date: The end dat eof this report
    :return: None.
    """
    date_info = {start_date: {'calendar_id': 'start-calendar',
                              'date_id': 'begin-date',
                              'button': start_date.weekday() + start_date.day},
                 end_date: {'calendar_id': 'end-calendar',
                            'date_id': 'end-date',
                            'button': start_date.weekday() + start_date.day + end_date.day - 1}}
    for target_date, info in date_info.items():
        calendar_id = info['calendar_id']

        # Click to open calendar
        date_id = driver.find_element_by_id(info['date_id'])
        action = ActionChains(driver)
        action.move_to_element(date_id).click().perform()

        # Find currently set year and month
        set_date_str = driver.find_element_by_xpath(
            f'//*[@id="{calendar_id}"]/div/div[1]/button[3]/span').get_attribute("innerHTML")
        set_month = datetime.strptime(set_date_str, "%B %Y").month
        set_year = datetime.strptime(set_date_str, "%B %Y").year

        button_map = {'next_year': '5',
                      'previous_year': '1',
                      'next_month': '4',
                      'previous_month': '2'}

        # Go to previous year
        while target_date.year < set_year:
            set_date = go_to(driver, button_map['previous_year'], calendar_id)
            set_year = set_date.year

        # Go to next year
        while target_date.year > set_year:
            set_date = go_to(driver, button_map['next_year'], calendar_id)
            set_year = set_date.year

        # Go to previous month
        while target_date.month < set_month:
            set_date = go_to(driver, button_map['previous_month'], calendar_id)
            set_month = set_date.month

        # Go to next month
        while target_date.month > set_month:
            set_date = go_to(driver, button_map['next_month'], calendar_id)
            set_month = set_date.month

        # Set day, button number starts at
        button = info['button']
        driver.find_element_by_xpath(f'//*[@id="{calendar_id}"]/div/div[2]/div/div/div/div[2]/button[{button}]').click()

    # Check that set date matches target date
    assert driver.find_element_by_id('begin-date').get_attribute("value") == start_date.strftime('%Y-%m-%d')
    assert driver.find_element_by_id('end-date').get_attribute("value") == end_date.strftime('%Y-%m-%d')


def go_to(driver: webdriver, button: str, calendar_id: str) -> datetime:
    """ Click to go to the next/previous month/year
    
    :param driver: The webdriver
    :param button: The button number corresponding to next or previous and month or year
    :param calendar_id: The calendar id, either 'start calendar' or 'end calendar'
    :return: The month and year that the calendar was set to.
    """
    driver.find_element_by_xpath(f'//*[@id="{calendar_id}"]/div/div[1]/button[{button}]').click()
    set_date_str = driver.find_element_by_xpath(f'//*[@id="{calendar_id}"]/div/div[1]/button[3]/span').text
    set_date = datetime.strptime(set_date_str, "%B %Y")

    return set_date


if __name__ == '__main__':
    main()
```

</details>

## Using the Gmail API
See the [google support answer](https://support.google.com/googleapi/answer/6158841?hl=en) for info on how to enable an API.
Search for the Gmail API and enable this.

### Creating the Gmail API connection and credentials  
Currently, the telescope works only with a Gmail account that is an internal user (a G-suite account).  
It is possible to create credentials for an external user with a project status of 'Testing' in the OAuth screen, 
however refresh tokens created in such a project expire after 7 days and the telescope does not handle expired
 refresh tokens.  
See the [documentation](https://developers.google.com/identity/protocols/oauth2#expiration) for more info on OAuth
 refresh token expiration.  

#### Create OAuth credentials   
- In the IAM section add the G-suite account you would like to use as a user. 
- From the 'APIs & Services' section, click the 'Credentials' menu item.
- Click 'Create Credentials' and choose OAuth client ID.
- In the form, enter the following information:
  -  Application type: Web application
  -  Name: Can be anything, e.g. 'Gmail API'
  -  Authorized redirect URIs: add the URI: http://localhost:8080/
  -  Click 'Create'
- Download the client secrets file for the newly created OAuth 2.0 Client ID, by clicking the download icon for the 
  client ID that you created. The file will be named something like `client_secret_token.apps.googleusercontent.com.json`    
- Get the credentials info using the JSON file with client secret info by executing the following python code. 

Note that there is currently a limit of 50 refresh tokens per client ID. 
If the limit is reached, creating a new refresh token automatically invalidates the oldest refresh token without
 warning.  
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


## Latest schema

### JSTOR Institution

``` eval_rst
.. csv-table::
   :file: ../schemas/jstor_institution_latest.csv
   :width: 100%
   :header-rows: 1
```

### JSTOR Country

``` eval_rst
.. csv-table::
   :file: ../schemas/jstor_country_latest.csv
   :width: 100%
   :header-rows: 1
```