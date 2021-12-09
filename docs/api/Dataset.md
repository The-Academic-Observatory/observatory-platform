# Dataset

The Dataset object allows you to store some basic information about a dataset like the name, and ingesting telescope.  More importantly, it acts a connecting link to more detailed data objects that provide richer information about a dataset, like the DatasetRelease, and DatasetStorage classes.

## Properties
<div class="wy-table-responsive"><table border="1" class="docutils">
<thead>
<tr>
<th>Name</th>
<th>Type</th>
<th>Description</th>
<th>Notes</th>
</tr>
</thead>
<tbody>






<tr>
    <td><strong>id</strong></td>
    <td><strong>int</strong></td>
    <td>Dataset ID.</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>name</strong></td>
    <td><strong>str</strong></td>
    <td>Name of the dataset.</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>connection</strong></td>
    <td><a href="Telescope.html"><strong>Telescope</strong></a></td>
    <td>Telescope that created this dataset.</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>extra</strong></td>
    <td><strong>bool, date, datetime, dict, float, int, list, str, none_type</strong></td>
    <td>Additional attributes as a JSON string.</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>created</strong></td>
    <td><strong>datetime</strong></td>
    <td>Creation time.</td>
    <td>[optional] [readonly] </td>
</tr>
<tr>
    <td><strong>modified</strong></td>
    <td><strong>datetime</strong></td>
    <td>Last modified.</td>
    <td>[optional] [readonly] </td>
</tr>


</tbody>
</table></div>


## Get a Dataset object

A specific Dataset object can be retrieved using `get_dataset`. A dataset ID is required.

```python
from observatory.api.client.api.observatory_api import ObservatoryApi
from observatory.api.client import ApiClient, Configuration

configuration = Configuration(host=f"http://host:port")
api = ObservatoryApi(api_client=ApiClient(configuration))

dataset_id = 1
dataset = api.get_dataset(dataset_id)
```

A list of Dataset objects can be retrieved using `get_datasets`. A limit on the number of results returned must be specified. The telescope ID can optionally be specified to limit results to a given Telescope ID.

```python
telescope_id = 1
limit = 100
all_datasets = api.get_datasets(limit)
filtered_datasets = api.get_datasets(limit, telescope_id)
```

## Create a Dataset object

New Dataset objects can be created using `put` and `post` methods. The dataset_id is omitted from the API call. The Dataset must be linked to a Telescope object with an ID set.

### Put call

```python
from observatory.api.client.model.dataset import Dataset
from observatory.api.client.model.telescope import Telescope

related_telescope = Telescope(id=123)
obj = Dataset(
    name="My dataset",
    extra="{}",
    connection=related_telescope,
)
api.put_dataset(obj)
```

### Post call

```python
related_telescope = Telescope(id=123)
obj = Dataset(
    name="My dataset",
    extra="{}",
    connection=related_telescope,
)
api.post_dataset_release(obj)
```

## Modify an existing Dataset object

An existing Dataset object can be modified through a `put` call by also specifying its Dataset ID.

```python
related_telescope = Telescope(id=123)
dataset_id = 1
obj = DatasetRelease(
    id=dataset_id,
    extra='{"type": "snapshot"}',
    connection=related_telescope,
)
api.put_dataset(obj)
```

## Deleting Dataset objects

A Dataset object can be deleted by specifying its Dataset ID.

```python
object_id_to_delete=123
api.delete_dataset_release(object_id_to_delete)
```

# Example applications

## Parameterising workflows with dataset metadata

Consider an example where you need to aggregate sales data from multiple retail partners.  The sales column differs from partner to partner. Partners change on a regular basis. Frequent code changes to reflect changing partners might be cumbersome.   Partner dataset information can be saved in the API instead.

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


