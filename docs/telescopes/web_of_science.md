# Web of Science

"The Web of Science is the information and technology provider for the global scientific research community. We provide data, analytics and insights, as well as workflow tools and bespoke professional services to researchers and the entire research community that underpins research – universities and research institutions, national and local governments, private and public research funding organizations, publishers and research-intensive corporations, across the world."
-- [Web of Science website](https://www.clarivate.com/webofsciencegroup>).

Web of science, previously Web of knowledge, provides bibliometric information, including funding acknowledgements,
international publication identifiers, and abstracts. - source: [WOS](https://clarivate.com/webofsciencegroup) and
[data details](https://clarivate.com/webofsciencegroup/solutions/xml-and-apis).

This telescope will follow general ETL design pattern of the other DAGs.  SubDAGs will be used for handling ETL for each
institution. SubDAGs might be taskgroups in the future once Airflow 2.0 lands.

The telescope will check for any connection id entries for institutions where we want to pull.  Each institution must have
access credentials, and entries will be pulled from the specified start date in the connection id to the start date of
the dag.


## Connection id

Relying on Python wos library for establishing connections. Just need to store enough credentials and parameters to
fetch for each of the frameworks.


### Naming convention of each connection id

```
wos_<institution>[_optionally a list of _ separated other attributes]
```
e.g.,
```
wos_curtin
```


### Connection id attributes
```
username: for the institution
password: for the institution
```

In the extras field, a json compatible string with ```start_date, id```, where ```start_date``` is a Pendulum parsable
date string, and ```id``` is a WoS searchable institution id, or list of ids (if there is more than one institution).
For example,

```
{
  "start_date" : "2020-09-01",
  "id" : ["Curtin University", "Some other valid institution id"],
  "project_id": "optional override for project_id",
  "transform_bucket_name": "optional override for transform_bucket_name",
  "download_bucket_name": "optional override for download_bucket_name",
  "data_location": "optional override for data_location"
}
```
or
```
{
  "start_date" : "2020-09-01",
  "id" : "Curtin University",
  "project_id": "new_project_id",
  "transform_bucket_name": "new_transform_bucket_name",
  "download_bucket_name": "new_download_bucket_name"
}
```

## DAG flow
For each subdag: ```check_api_server >> subdag```

Within each subdag:
```
check_dependencies >> download >> upload_downloaded >> transform_xml_to_json >> transform_db_format >> upload_transformed >> bq_load >> cleanup
```


### check_api_server

See if http://scientific.thomsonreuters.com is contactable.


### check_dependencies

Airflow configuration check.

### download_data

Downloads the data. Currently it will make API calls for each month of data. Extendible to have finer control.
Throttling and retry limits will be more conservative than the WoS limits.  See WosTelescope and helper classes for
more details.

***Obey the bandwidth limits:***

_Web of science bandwidth limits_ (see [link](http://help.incites.clarivate.com/wosWebServicesExpanded/bandwidthThrottlingGroup/bandwidthThrottling.html))

New session creation: 5 per 5-min period.
  * API calls: 2 calls/s
  * Returned results: 100 max per call.
  * Cited references: 100 max per article.
  * Max records retrievable in period: licence dependent. Unclear what Curtin’s limit is if any.


### upload_downloaded

Upload gzipped xml data.

### transform_xml_to_json

Transform xml to json.

### transform_db_format

Transform json into schema compatible format, and convert to jsonlines.

### upload_transformed

Upload jsonlines data.

### bq_load

Load the entries into BigQuery.

### cleanup

Do any necessary cleanup/deletion.

## Latest schema
``` eval_rst
.. csv-table::
   :file: ../schemas/wos_wok5.4_latest.csv
   :width: 100%
   :header-rows: 1
```
## External references
 * [Web of Science API documentation](http://help.incites.clarivate.com/wosWebServicesExpanded/WebServicesExpandedOverviewGroup/Introduction.html)
 * [Field name descriptions](http://help.incites.clarivate.com/wosWebServicesExpanded/appendix1Group/wosfieldNameTable.html)