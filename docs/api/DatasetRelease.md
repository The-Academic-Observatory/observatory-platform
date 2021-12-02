# DatasetRelease

The DatasetRelease object describes a specific release of a given Dataset object.

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
    <td>DatasetRelease ID.</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>schema_version</strong></td>
    <td><strong>str</strong></td>
    <td>Schema version used by the academic observatory for this release.</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>schema_version_alt</strong></td>
    <td><strong>str, none_type</strong></td>
    <td>Alternative schema version.</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>start_date</strong></td>
    <td><strong>datetime</strong></td>
    <td>Coverage start date, or the date of dataset if there is no range.</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>end_date</strong></td>
    <td><strong>datetime, none_type</strong></td>
    <td>Coverage end date (inclusive).</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>ingestion_start</strong></td>
    <td><strong>datetime, none_type</strong></td>
    <td>Timestamp of when the telescope started ingesting the dataset.</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>ingestion_end</strong></td>
    <td><strong>datetime, none_type</strong></td>
    <td>Timestamp of when the  telescope finished ingesting the dataset.</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>dataset</strong></td>
    <td><a href="Dataset.html"><strong>Dataset</strong></a></td>
    <td>Dataset that this is a release for.</td>
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


## Get a DatasetRelease object

A DatasetRelease object can be retrieved using `get_dataset_release`. A DatasetRelease ID is required.

```python
from observatory.api.client.api.observatory_api import ObservatoryApi
from observatory.api.client import ApiClient, Configuration

configuration = Configuration(host=f"http://host:port")
api = ObservatoryApi(api_client=ApiClient(configuration))

dataset_release_id = 1
dataset_release = api.get_dataset_release(dataset_release_id)
```

A list of DatasetRelease objects can be retrieved using `get_dataset_releases`. A limit on the number of results returned must be specified. The dataset ID can optionally be specified to limit results to a given Dataset ID.

```python
dataset_id = 1
limit = 100
all_dataset_releases = api.get_dataset_releases(limit)
filtered_dataset_releases = api.get_dataset_releases(limit, dataset_id)
```

## Create a DatasetRelease object

New DatasetRelease objects can be created using `put` and `post` methods. The dataset_release_id is omitted from the API call. The DatasetRelease must be linked to a Dataset object with an ID set.

### Put call

```python
from observatory.api.client.model.dataset import Dataset
from observatory.api.client.model.dataset_release import DatasetRelease

related_dataset = Dataset(id=123)
obj = DatasetRelease(
    dataset=related_dataset,
    schema_version="20211112",
    schema_version_alt="wok5.4",
    start_date=pendulum.datetime(2021,1,1),
    end_date=pendulum.datetime(2021,2,1),
    ingestion_start=pendulum.datetime(2021,3,1,12,0,0),
    ingestion_end=pendulum.datetime(2021,3,1,18,0,0),
)
api.put_dataset_release(obj)
```

### Post call

```python
related_dataset = Dataset(id=123)
obj = DatasetRelease(
    dataset=related_dataset,
    schema_version="20211112",
    schema_version_alt="wok5.4",
    start_date=pendulum.datetime(2021,1,1),
    end_date=pendulum.datetime(2021,2,1),
    ingestion_start=pendulum.datetime(2021,3,1,12,0,0),
    ingestion_end=pendulum.datetime(2021,3,1,18,0,0),
)
api.post_dataset_release(obj)
```

## Modify an existing DatasetRelease object

An existing DatasetRelease object can be modified through a `put` call by also specifying its DatasetRelease ID.

```python
related_dataset = Dataset(id=123)
dataset_release_id = 1
obj = DatasetRelease(
    id=dataset_release_id,
    dataset=related_dataset,
    schema_version="20211212",
    schema_version_alt="wok5.4",
    start_date=pendulum.datetime(2021,1,1),
    end_date=pendulum.datetime(2021,2,1),
    ingestion_start=pendulum.datetime(2021,3,1,12,0,0),
    ingestion_end=pendulum.datetime(2021,3,1,18,0,0),
)
api.put_dataset_storage(obj)
```

## Deleting DatasetRelease objects

A DatasetStorage object can be deleted by specifying its DatasetRelease ID.

```python
object_id_to_delete=123
api.delete_dataset_release(object_id_to_delete)
```