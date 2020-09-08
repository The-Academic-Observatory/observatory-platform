# WOS (Web of science)

It will follow general ETL design pattern of the other DAGs.  SubDAGs will be used for each step requiring per institution handling. SubDAGs might be taskgroups in the future once Airflow 2.0 lands.

The DAG will check for any connection id entries for institutions where we want to pull.  Each institution must have access credentials, and entries will be pulled from the specified start date in the connection id to the start date of the dag.


## Connection id

Relying on Python wos library for establishing connections. Just need to store enough credentials and parameters to fetch for each of the frameworks.


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
start_date:  Python pendulum or datetime parseable date string
```


## DAG flow
```
check_airflow >> check_source_server >> download_data >> upload_raw_data >> transform_data >> upload_transformed_data >> bq_load >> cleanup
```


## check_airflow [maybe this can be skipped]

See if http://scientific.thomsonreuters.com is contactable.


## download_data

This will be a subDAG to handle institution downloads separately. It will pull the full record from the specified start date in the connection id, to the start date of the DAG. This will be chunked into monthly blocks.  Some throttling and retry mechanisms will be baked into the code to obey the following limits.

***Obey the bandwidth limits:***

_Web of science bandwidth limits_
New session creation: 5 per 5-min period.
  * API calls: 2 calls/s
  * Returned results: 100 max per call.
  * Cited references: 100 max per article.
  * Max records retrievable in period: licence dependent. Unclear what Curtin’s limit is if any.


## transform_data

Transform list of json into jsonlines and gzip the data.


## upload_transformed_data

Upload jsonlines data.


## bq_load

Load the entries into BigQuery.


## cleanup

Do any necessary cleanup/deletion.

## Database schema

Refer to docs/datasets/provider_wos for schema information.