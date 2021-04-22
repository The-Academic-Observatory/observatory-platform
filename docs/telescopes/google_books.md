# Google Books
The Google Books Partner program enables selling books through the Google Play store and offering a preview on Google books.  
The program makes books discoverable to Google users around the world on Google books. When readers find a book on Google Books, they can preview a limited number of pages to decide if they're interested in it. 
Readers can also follow links to buy the book or borrow or download it when applicable.

As a publisher you can download reports on Google Books data from https://play.google.com/books/publish/.  

Currently there are 3 report types available:
- Google Play sales summary report
- Google Play sales transaction report
- Google Books Traffic Report

In this telescope we collect data from the last 2 reports.  
The corresponding tables created in BigQuery are `google.google_books_salesYYYYMMDD` and `google.google_books_trafficYYYYMMDD`.

| Summary                 |        |
|-------------------------|--------|
| Average runtime         |   ? min |
| Average download size   |   ? mb |
| Harvest Type            |  SFTP  |
| Harvest Frequency       | Monthly|
| Runs on remote worker   |  True  |
| Catchup missed runs     |  True  |
| Table Write Disposition |Truncate|
| Update Frequency        |  Daily |
| Credentials Required    |   Yes  |
| Uses Telescope Template |Snapshot|
| Each shard includes all data |   No    |


## Authentication
The reports are downloaded from https://play.google.com/books/publish/. To get access to the reports the publisher needs to give access to a google service account.  
This service account can then be used to login on this webpage and download each report manually.

## Setting up a service account
* Create a service account from IAM & Admin - Service Accounts
* Create a JSON key and download the file with key
* For each organisation/publisher of interest, ask them to add this service account for Google Books

## Downloading Reports Manually
There is no API available to download the Google Books report and it is quite challenging to automate the Google login process through tools such as Selenium, because of Google's bot detection triggering a reCAPTCHA.  
Until this step can be automated, the reports need to be downloaded manually, for each publisher and for both the sales transaction report and the traffic report: 
*  A report should be created for exactly 1 month (e.g. starting 2021-01-01 and ending 2021-01-31). 
*  All titles should be selected.
*  All countries should be selected.
*  It is important to save the file with the right name, this should be in the format:
    *  `GoogleSalesTransactionReport_YYYY_MM.csv` or
    *  `GoogleBooksTrafficReport_YYYY_MM.csv`
*  Upload each report to the SFTP server at https://oaebu.exavault.com/
    *   Add it to the folder `/telescopes/google_books/<publisher>/upload`
    *   Files are automatically moved between folders, please do not move files between folders manually

## Airflow connections
Note that all values need to be urlencoded.  
In the config.yaml file, the following airflow connection is required:  

### sftp_service
The sftp_service airflow connection is used to connect to the sftp_service and download the reports.  
The username and password are created by the sftp service and the host is e.g. `oaebu.exavault.com`.  
The host key is optional, you can get it by running ssh-keyscan, e.g.:
```
ssh-keyscan oaebu.exavault.com
```

```yaml
sftp_service: ssh://<username>:<password>@<host>?host_key=<host_key>
```

