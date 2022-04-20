# Python API Use Cases
This is a set of uses cases for using the Python API.

The observatory platform API trades off the Airflow paradigm of "workflows as code" for a little more dynamic control over certain settings.


## Parameterising workflows with dataset metadata
Consider an example where you need to aggregate sales data from multiple retail partners.  The sales column differs from 
partner to partner. Partners change on a regular basis. Frequent code changes to reflect changing partners might be 
cumbersome. Partner dataset information can be saved in the API instead.

```python
@dataclass
class SalesPartner:
    dataset_name: str  # e.g., "book_sales"
    sales_field : float  # e.g., "sales"
    db_service: str  # e.g., bigquery
    table_address: str  # e.g., project.dataset.table
    table_type: str  # e.g., sharded

# Get all datasets and filter by datasets in the "sales_partner" group
api = make_observatory_api()
workflows = api.get_workflows(organisation_id=organisation_id, limit=1000)
partners = []
for workflow in workflows:
    tags = json.loads(workflow.tags)
    if "sales_partner" not in tags:
        continue

    datasets = api.get_datasets(workflow_id=workflow.id, limit=1000)
    for dataset in datasets:
        partners.append(
            SalesPartner(
                dataset_name=dataset.name,
                sales_field=dataset.dataset_type.extras.get("sales_field"),
                db_service=dataset.service,
                table_address=dataset.address,
                table_type=dataset.dataset_type.table_type.type_id,
            )
        )


workflow = AggregateSalesWorkflow(partners=partners)
globals()[workflow.dag_id] = workflow.make_dag()
```

## Monitoring data sources through dag sensors

Airflow workflows can monitor the state of other running workflows.  This can be used to delay execution of specific workflows until other workflows have finished execution.  Consider a workflow which aggregates data ingested by other workflows. It can leverage this mechanic to wait until all data ingestion tasks are complete before it starts the aggregation process.

One possible implementation is to set `DatasetType`'s `type_id` to be the Airflow DAG id and add a tag to specify it as an input source for aggregation.

```python
workflows = api.get_workflows(organisation_id=organisation_id, limit=1000)

monitored_dag_ids = []
for workflow in workflows:
    tags = json.loads(workflow.tags)
    if "aggregation_input" not in tags:
        continue

    datasets = api.get_datasets(workflow_id=workflow.id, limit=1000)
    for dataset in datasets:
        monitored_dag_ids.append(dataset.dataset_type.type_id)

workflow = AggregationWorkflow(monitored_dag_ids=monitored_dag_ids)
globals()[workflow.dag_id] = workflow.make_dag()
```

## Parameterising workflow creation

Sometimes it's useful to dynamically create workflows depending on API parameters. We might want to create a specific workflow for every Organisation in the API.

```python
organisations = api.get_organisations(limit=1000)
for org in organisations:
    dag_id_prefix = "dag_id_prefix"
    dag_id = make_dag_id(dag_id_prefix, org.name)
    workflow = MyWorkflow(dag_id=dag_id)
    globals()[workflow.dag_id] = workflow.make_dag()
```

## Configuring dataset storage locations

It's beneficial in some situations not to specify dataset storage locations in code.  This could be due to security considerations, or to provide allow more flexibility in migrating to different storage providers later.

One way to accomplish this is to store these credentials as Airflow variables or connections. The API provides an alternative mechanism by storing the information in the Dataset APIs.  It allows you to conduct access control without needing to grant Airflow server access, and gives some resilience during Airflow version migration.

### Fetching dataset storage address information

```python
dataset = api.get_dataset(id=dataset_id)
if dataset.service == "bigquery":
    gcp_project_id, gcp_dataset_id, gcp_table_id = address_to_gcp_fields(dataset.address)
    # Code to handle bigquery table creation
```

### Modifying dataset storage address information
```python
dataset = Dataset(
    id=target_dataset_id,
    service="new service",
    address="new address",
)
api.put_dataset(dataset)
```

## Tracking dataset release information
Deducing dataset release information from Airflow metadata can lead to complicated logic.  One way to facilitate easier
access is to store dataset release information in the API.  Consider a workflow that updates a dataset with daily 
updates. Relying on the Airflow scheduling mechanic for data date intervals means you are often at least a day behind 
when updating the dataset. You also need to rely on `catchup=True` in order to maintain continuity.  An alternative is
to use DatasetRelease information from the API.

Instead of relying on Airflow date mechanics, the workflow calculates the necessary releases that need to be processed through an API query.
```python
limit = 100
dataset_id = 123
releases = api.get_dataset_releases(limit, dataset_id)
# Code to look through the list of releases and compare with what daily updates are available to see what you need to fill in
```

Once the dataset has been successfully loaded into the storage location, you could make a separate task to add the new release(s) added.
```python
def add_dataset_release(self, release, **kwargs):
    obj = DatasetRelease(
        dataset=self.dataset,  # API Dataset object
        start_date=pendulum.datetime(2021,1,1),
        end_date=pendulum.datetime(2021,2,1),
    )
    result = self.api.post_dataset_release(obj)
```

This common use case has been added as an Airflow task `add_new_dataset_releases` in the `Workflow` class.  When initialising a workflow object, you can add the task with
```python
self.add_task(self.add_new_dataset_releases)
```

The task uses the `workflow_id` of the workflow to get a list of associated `Dataset` objects.  For each associated `Dataset`, it creates a new `DatasetRelease` record using the release date information supplied by the current release objects relating to that workflow, e.g., `release_date` in a `SnapshotRelease` object.

These `DatasetRelease` records allow you to quickly lookup release information without requiring Airflow access, or making sql queries on the storage tables.

```python
dataset_id = 1
limit = 100
releases = api.get_dataset_releases(limit, dataset_id)
```

### Tracking across different services

The API allows easy tracking of Datasets stored across different services. It provides visibility on Datasets across different services without needing to query the different services separately.  For example, ElasticSearch snapshots could also be tracked as well as the BigQuery exports.

### Debugging DagRuns

The task `add_new_dataset_releases` creates a new `DatasetRelease` API record for each `Dataset` associated with a particular `workflow_id`.    

Since this task is executed for each dag run with successful data ingestion, `DatasetRelease` records are created for each successful dag run. This can be used to help with debugging, and increase traceability.

As an example, snapshot telescopes often create sharded BigQuery tables. Each successful dag run creates a new shard in the table, and this can be used to see how many successful dag runs occurred. A stream telescope often creates a single BigQuery table which is updated with each successful dag run.  Since it is a single table, it's not obvious how many times the table has been updated without permissions to view the dag runs or run special sql queries.

## Substitute for the Airflow catchup mechanic

Workflows are run on a schedule controlled by Airflow.  When Airflow is interrupted, dags with the `catchup` parameter set to `True` will run scheduled dag runs which were not executed due to the interruptions.  With this design paradigm, workflows that want to take advantage of catchup need to closely align release dates with Airflow execution dates so that the correct period of data is fetched during catchup dag runs.  This sometimes allows you to simply the logic in a workflow, but it could also complicate things.

The DatasetRelease API allows an alternative way to handle catchup.  Since all dataset releases are recorded by the API, workflows can alternatively:

1. Pull a list of all releases available from the data source.
2. Pull a list of all releases recorded by the API.
3. Find the releases available from the data source that are not recorded by the API.
4. Ingest the dataset releases calculated from step 3 and update the API.

This approach allows filling in the missing datasets without using the catchup mechanic.

```python
limit = 1000
dataset_id = 1
processed_data = api.get_dataset_releases(limit, dataset_id)
available_data = get_available_dataset_releases(...)
missing_releases = get_missing_data(processed_data, available_data)
download_releases(missing_releases)
# Other ETL tasks
# Update records with the DatasetRelease API
```

## Multiple releases from a single dag run

Tightly coupling release information with dag run execution dates limits the precision of dataset release information that can be recorded. The API provides another mechanism to record precise release information when you ingest more than one release during a single dag run.  One example of this is the previous use case of replacing the catchup mechanism.

## Calculating table names from TableType
BigQuery table names are different depending on the type of table used.  For example, sharded tables have a date suffix on the end, e.g., `mytable20210102`, whereas partitioned tables reference partitions using a \$ prefix, e.g., `mytable$202101`.  The TableType record gives you information on the type of table used, and can be used by workflows to construct the correct table_id for a specific dataset.

```python
dataset = api.get_dataset(id=dataset_id)
print(dataset.dataset_type.table_type.type_id)
```

## Possible extensions
Other future extensions are possible with the platform API.  Some ideas include:
- Storing dataset licensing information, and letting workflows restrict exports using licensing criteria.
- Conducting data quality analysis on datasets by creating a snapshot using live API calls. Since we are not able to take a snapshot at a frozen point in time, the underlying data could potentially change during the download process.  Recording ingestion times could help quantify the amount of possible drift.
- Dataset retention policies could be set by API instead of being hard coded in workflows to allow faster response to changing requirements.