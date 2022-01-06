# Python API Use Cases
This is a set of uses cases for using the Python API.

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
datasets = api.get_datasets()
partner_datasets = list(filter(lambda ds: "sales_partner" in ds.extras.get("groups", []), datasets))

partners = []
for dataset in partner_datasets:
    storage = api.get_storages(1, dataset.id)
    partners.append(
        SalesPartner(
            dataset_name=dataset.name,
            sales_field=dataset.extra.get("sales_field"),
            db_service=storage.service,
            table_address=storage.address,
            table_type=storage.extra.get("table_type"),
        )
    )

workflow = AggregateSalesWorkflow(partners=partners)
globals()[workflow.dag_id] = workflow.make_dag()
```

## Tracking dataset release information
Deducing dataset release information from Airflow metadata can lead to complicated logic.  One way to facilitate easier
access is to store dataset release information in the API.  Consider a workflow that updates a dataset with daily 
updates. Relying on the Airflow scheduling mechanic for data date intervals means you are often at least a day behind 
when updating the dataset. You also need to rely on `catchup=True` in order to maintain continuity.  An alternative is
to use DatasetRelease information from the API.

Instead of relying on Airflow date mechanics, the workflow calculate the necessary releases that need to be processed 
through an API query.
```python
limit = 100
dataset_id = 123
releases = api.get_dataset_releases(limit, dataset_id)
# Code to look through the list of releases and compare with what daily updates are available to see what you need to fill in
```

Once the dataset has been successfully loaded into the storage location, you could make a separate task to add the new 
release(s) added.
```python
def add_dataset_release(self, release, **kwargs):
    obj = DatasetRelease(
        dataset=self.dataset,
        schema_version="20211112",
        schema_version_alt="wok5.4",
        start_date=pendulum.datetime(2021,1,1),
        end_date=pendulum.datetime(2021,2,1),
        ingestion_start=actual_execution_date(),  # actual_execution_date could for example be the dagrun start_date.
        ingestion_end=pendulum.now("UTC")
    )
    result = self.api.post_dataset_release(obj)
```
