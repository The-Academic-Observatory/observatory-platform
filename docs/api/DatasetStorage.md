# DatasetStorage

The DatasetStorage object describes a storage location for a Dataset object.

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
    <td>DatasetStorage ID</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>service</strong></td>
    <td><strong>str</strong></td>
    <td>Type of lake/warehouse service, e.g., bigquery.</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>address</strong></td>
    <td><strong>str</strong></td>
    <td>Access address, e.g., project.dataset.table</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>extra</strong></td>
    <td><strong>bool, date, datetime, dict, float, int, list, str, none_type</strong></td>
    <td>Additional attributes as a JSON string.</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>dataset</strong></td>
    <td><a href="Dataset.html"><strong>Dataset</strong></a></td>
    <td>Associated Dataset.</td>
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
    <td>Last modified time.</td>
    <td>[optional] [readonly] </td>
</tr>


</tbody>
</table></div>



## Get a DatasetStorage object

A DatasetStorage object can be retrieved using `get_dataset_storage`. A DatasetStorage ID is required.

```python
from observatory.api.client.api.observatory_api import ObservatoryApi
from observatory.api.client import ApiClient, Configuration

configuration = Configuration(host=f"http://{self.host}:{self.port}")
api = ObservatoryApi(api_client=ApiClient(configuration))

dataset_storage_id = 1
dataset_storage = api.get_dataset_storage(dataset_storage_id)
```

A list of DatasetStorage objects can be retrieved using `get_dataset_storages`. A limit on the number of results returned must be specified. The dataset ID can optionally be specified to limit results to a given Dataset ID.

```python
dataset_id = 1
limit = 100
all_dataset_storages = api.get_dataset_storages(limit)
filtered_dataset_storage = api.get_dataset_storages(limit, dataset_id)
```

## Create a DatasetStorage object

New DatasetStorage objects can be created using `put` and `post` methods. The dataset_storage_id is omitted from the API call. DatasetStorage must be linked to a Dataset object with an ID set.

### Put call
```python
from observatory.api.client.model.dataset import Dataset
from observatory.api.client.model.dataset_storage import DatasetStorage

related_dataset = Dataset(id=123)
obj = DatasetStorage(
    dataset=related_dataset,
    service="bigquery",
    address="myproject.datasetname.tablename",
    extra="{}",
)
api.put_dataset_storage(obj)
```

### Post call

```python
related_dataset = Dataset(id=123)
obj = DatasetStorage(
    dataset=related_dataset,
    service="bigquery",
    address="myproject.datasetname.tablename",
    extra="{}",
)
api.post_dataset_storage(obj)
```

## Modify an existing DatasetStorage object

An existing DatasetStorage object can be modified through a `put` call by also specifying its DatasetStorage ID.

```python
related_dataset = Dataset(id=123)
dataset_storage_id = 1
obj = DatasetStorage(
    id=dataset_storage_id,
    dataset=related_dataset,
    service="bigquery",
    address="myproject.datasetname.newtable",
    extra="{}",
)
api.put_dataset_storage(obj)
```

## Deleting DatasetStorage objects

A DatasetStorage object can be deleted by specifying its DatasetStorage ID.

```python
object_id_to_delete=123
api.delete_dataset_storage(object_id_to_delete)
```

# Example applications

See the `Dataset` API documentation.