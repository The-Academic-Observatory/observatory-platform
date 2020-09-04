# WOS (Web of science)

The DAG will be a slight modification of the snapshot data source DAGs.  DAGs will be dynamically generated for each institution. Each dynamically generated DAG will be similar to the snapshot DAGs and offload the work to a Telescope class.


## Dag naming

A dynamic dag will be generated with name:
```
wos_<institution>_<optional attributes>
```
e.g.,
```
wos_curtin
```


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


### Attributes / extra fields
```
username: (if relevant)
password: (if relevant)
start_date:  Python pendulum or datetime parseable date string
```

At some point it might be worthwhile to programmatically determine the optimal date ranges to minimise API calls.


## DAG definition

There will be a slight difference compared to the snapshot DAGs. It will query Airflow for a list of connection ids relevant to this particular data source, e.g., wos_curtin, wos_auckland, etc. A dynamically generated DAG will be used for each institution.

The template for the dynamically generated DAGs will follow the pattern in the snapshot DAG, where the heavy lifting is done by Telescope classes.

Listing airflow connections can be done with
```python
session.query(Connection)
```


## DAG flow
```
check_airflow >> check_source_server >> download_data >> upload_raw_data >> transform_data >> upload_transformed_data >> bq_load >> cleanup
```


## check_airflow [maybe this can be skipped]

See if http://scientific.thomsonreuters.com is contactable.


## download_data

From the start date til the most recently completed month, pull data from the data source using an adapted version of Richard’s code, and append to a file on GC containing a list of json responses. Not sure if you want timing information like you did before.
***Obey the bandwidth limits:***

_Web of science bandwidth limits_
New session creation: 5 per 5-min period.
  * API calls: 2 calls/s
  * Returned results: 100 max per call.
  * Cited references: 100 max per article.
  * Max records retrievable in period: licence dependent. Unclear what Curtin’s limit is if any.

Use Airflow params:
```python
retries=2
retry_delay=timedelta(minutes=15) # Does pendulum.duration work too?
```

Since all download is lumped into 1 task, the task would need to have retry and delay code baked into it.

***ALTERNATIVELY***, dynamically generate tasks for each query. One benefit is being able to leverage Airflow’s parameters to do retries, but would create a massive amount of download tasks and an unwieldly looking DAG.


## transform_data

Transform list of json into jsonlines


## upload_transformed_data

Upload jsonlines data.


## bq_load

Load the entries into BigQuery.


## cleanup

Do any necessary cleanup/deletion.

## Database schema

Refer to docs/datasets/provider_wos for schema information.