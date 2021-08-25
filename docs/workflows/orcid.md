# ORCID

The ORCID (Open Researcher and Contributor ID) is a nonproprietary alphanumeric code to uniquely identify authors and contributors of scholarly communication as well as ORCID's website and services to look up authors and their bibliographic output (and other user-supplied pieces of information).
For more information, see: https://orcid.org/
This telescope syncs records from the ORCID AWS bucket and stores the up-to-date records in the BigQuery table.

The corresponding tables created in BigQuery are `orcid.orcid` and `orcid.orcid_partitions`.

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
| Catchup missed runs          | False   |
+------------------------------+---------+
| Table Write Disposition      | Append  |
+------------------------------+---------+
| Update Frequency             | Daily   |
+------------------------------+---------+
| Credentials Required         | Yes     |
+------------------------------+---------+
| Uses Telescope Template      | Stream  |
+------------------------------+---------+
```

## Using the transfer service
The files in the AWS bucket are transferred to a separate Google Cloud storage bucket using the storage transfer
 service.
Unfortunately it is not possible to use the transfer service with a specified directory in a Google Cloud bucket, so
 a separate bucket needs to be created to sync the data.
To use the transfer service it is required to enable the Storage Transfer API and to set the correct permissions on
 the Google Cloud Storage bucket as well as the AWS bucket.
 
### Enabling the Storage Transfer API
The API should already be enabled from the Terraform set-up. If this is not the case, see the [google support answer
](https://support.google.com/googleapi/answer/6158841?hl=en) for info on how to enable an API.
Search for the Storage Transfer API and enable this.

### Setting permissions on Google Cloud bucket
A separate bucket needs to be created for the ORCID records.
The following permissions are required on this Google Cloud bucket:
* storage.buckets.get
* storage.objects.list
* storage.objects.get
* storage.objects.create

The roles/storage.objectViewer and roles/storage.legacyBucketWriter roles together contain the permissions that are
 always required.
These roles or permissions need to be assigned at the specific bucket to the service account performing the transfer. 

The Storage Transfer Service uses the `project-[$PROJECT_NUMBER]@storage-transfer-service.iam.gserviceaccount.com` service account.

Additionally, the Airflow Service account requires the `storage.buckets.get` permission on the ORCID bucket, in order to
 check whether the bucket exists before starting the telescope, the role Storage Admin contains this permission. 
The Airflow Service account is in the format of `<project_id>@<project_id>.iam.gserviceaccount.com`

### Setting permissions on AWS bucket
The AWS buckets are managed by ORCID. There are three different buckets:
* orcid-lambda-file
* v2.0-summaries
* v2.0-activities

In this telescope only the first two are used.
The required policy for these buckets is:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::orcid-lambda-file",
        "arn:aws:s3:::v2.0-activities",
        "arn:aws:s3:::v2.0-summaries"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:s3:::orcid-lambda-file/*",
        "arn:aws:s3:::v2.0-activities/*",
        "arn:aws:s3:::v2.0-summaries/*"
      ]
    }
  ]
}
```

## Airflow connections
Note that all values need to be urlencoded.
In the config.yaml file, the following airflow connections are required:  

## orcid
This connection contains the AWS access key id and secret access key that are used to access data in the AWS buckets.
Make sure to URL encode each of the fields 'access_key_id' and 'secret_access_key'.
```yaml
orcid: aws://<access_key_id>:<secret_access_key>@
```

## Airflow variables
In the config.yaml file, the following airflow variables are required (without gs:// prefix):  

## orcid_bucket
```yaml
orcid_bucket: <orcid_bucket_name>
```