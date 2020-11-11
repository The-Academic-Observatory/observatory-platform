# SCOPUS

The DAG will look for connections conforming to the Airflow connection ID naming convention (below) and generate a
subdag to handle the entire ETL pipeline for each institution.

## Airflow Connection ID naming

Each connection id will have a name of the form ```scopus_<institution>```, for example ```scopus_curin```. 

### Attributes / extra fields
The extra field of the Airflow connection should be a json parsable dictionary with the following keys. 
```
"api_keys": [list of dictionaries of the form {"key": "keystring", (optionally) "view": "standard or complete"]
"start_date":  Python pendulum or datetime parseable date string
"id": [list of scopus institution id strings],
"project_id": "optional override for project_id",
"transform_bucket_name": "optional override for transform_bucket_name",
"download_bucket_name": "optional override for download_bucket_name",
"data_location": "optional override for data_location"
```
For example:
```
{
  "api_keys" : [ {"key": "test_key", "view": "standard"}, {"key": "another_key"}],
  "start_date" : "2020-09-01",
  "id" : "60031226",
  "project_id": "new_project_id",
  "transform_bucket_name": "new_transform_bucket_name",
  "download_bucket_name": "new_download_bucket_name"
}
```

## DAG definition
The main dag will check that the API server is up.  If this is the case, each subdag will proceed to execute its ETL
pipeline.

## Subdag flow
```
check_dependencies >> download >> upload_downloaded >> transform_db_format >> upload_transformed >> bq_load >> cleanup
```

### check_dependencies

This checks the airflow configuration variables (including overrides), and configures some release parameters.

### download

Two download modes are available. ```sequential``` and ```parallel```. The current way to configure this is in the
```ScopusTelescope``` code.

Sequential mode distributes jobs out to each API key in a round robin.  The process blocks until the worker succeeds or
 fails, before serving new jobs. If a key has exceeded its quota, it is put on cool down and will be added back when the
 cool down period has elapsed.
 
Parallel mode allows each key client to fetch jobs when they have free cycles. If a client exceeds their quota, they 
will relinquish their task to the others, and sleep until cool down period has elapsed, before making itself available
for further processing.

Sequential mode will more likely evenly distribute the tasks, while parallel mode offers no load balance guarantees.

***Throttling limits*** (see [link](https://dev.elsevier.com/api_key_settings.html))
 * API calls are rate limited to 1 call/s (Elsevier sets 2 call/s as their documented rate).
 * Number of results returned per call is capped at 25 (Elsevier limit).
 * Maximum number of results per query is 5000 (Elsevier limit).

### upload_downloaded

Gzips up downloaded files and uploads to the cloud.

### transform_db_format

Transforms json data into BigQuery schema compatible fields, and converts this into jsonlines format.

### bq_load

Loads the jsonlines entries into BigQuery.

### cleanup

Deletes any temporary files.

## Database schema

Refer to docs/datasets/provider_wos for schema information.

## External references
* [Developer API portal](https://dev.elsevier.com/scopus.html)
* [SCOPUS API specification](https://dev.elsevier.com/documentation/ScopusSearchAPI.wadl)
* [Search tips](https://dev.elsevier.com/sc_search_tips.html)
* [Search views (response description)](https://dev.elsevier.com/sc_search_views.html)
* [API key settings](https://dev.elsevier.com/api_key_settings.html)