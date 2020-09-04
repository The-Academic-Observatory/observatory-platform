# SCOPUS

The DAG will be a slight modification of the snapshot data source DAGs.  DAGs will be dynamically generated for each institution. Each dynamically generated DAG will be similar to the snapshot DAGs and offload the work to a Telescope class.


## Dag naming

A dynamic dag will be generated with name:
```
scopus_<institution>_<optional attributes>
```
e.g.,
```
scopus_curtin
```


## Connection id

Relying on Python elsapy libraries for establishing connections. Just need to store enough credentials and parameters to fetch for each of the frameworks.


### Naming convention of each connection id

```
scopus_<institution>[_optionally a list of _ separated other attributes]
```
e.g.,
```
scopus_curtin
```


### Attributes / extra fields
```
api_key: (if relevant)
start_date:  Python pendulum or datetime parseable date string
max_record_quota: (if relevant)
```

At some point it might be worthwhile to programmatically determine the optimal date ranges to minimise API calls.


## DAG definition

There will be a slight difference compared to the snapshot DAGs. It will query Airflow for a list of connection ids relevant to this particular data source, e.g., scopus_curtin, scopus_auckland, etc. A dynamically generated DAG will be used for each institution.

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

See if https://dev.elsevier.com is contactable.


## download_data

From the start date til the most recently completed month, pull data from the data source using an adapted version of Richard’s code, and append to a file on GC containing a list of json responses. Not sure if you want timing information like you did before. Maybe also round robin multiple API keys when they exist for each query.

***Obey the bandwidth limits:***

_Web of science bandwidth limits_
New session creation: 5 per 5-min period.
  * API calls: 2 calls/s (unclear what SCOPUS limit is, using WOS ones)
  * Remaining quota: 20000 per API key per weekRemaining quota: 20000 per API key per week
  * Cool down period: 1 week if exceed quota
  * Returned results: 100 max per call.  Unclear what SCOPUS limit is, using WOS limit.

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