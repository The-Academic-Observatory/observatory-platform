# Python API Tutorial

# TableType

### Get a TableType object

A specific TableType object can be retrieved using `get_table_type`. A TableType `id` or `type_id` is required.

```python
from observatory.api.client.api.observatory_api import ObservatoryApi
from observatory.api.client import ApiClient, Configuration

configuration = Configuration(host=f"http://host:port")
api = ObservatoryApi(api_client=ApiClient(configuration))

id_ = 1
table_type = api.get_table_type(id=id_)

type_id = "some unique type id"
table_type = api.get_table_type(type_id=type_id)
```

A list of TableType objects can be retrieved using `get_table_types`. A liimit on the number of results returned must be specified.
```python
telescope_id = 1
limit = 100
all_table_types = api.get_table_types(limit)
```

### Create a TableType object

New TableType objects can be created using `put` and `post` methods. The TableType id is omitted.

#### Put call

```python
from observatory.api.client.model.table_type import TableType

obj = TableType(
    name="My table type",
    type_id="partitioned",
)
api.put_table_type(obj)
```

#### Post call

```python
obj = TableType(
    name="My table type",
    type_id="partitioned",
)
api.post_table_type(obj)
```

### Modify an existing TableType object

An existing TableType object can be modified through a `put` call by also specifying its TableType id.

```python
id_ = 1
obj = TableType(
    id=id_,
    name="My new type",
    type_id="new_type",
)
api.put_table_type(obj)
```

### Deleting TableType objects

A TableType object can be deleted by specifying its TableType id.

```python
object_id_to_delete=123
api.delete_table_type(object_id_to_delete)
```

# DatasetType

### Get a DatasetType object

A specific Dataset object can be retrieved using `get_dataset_type`. A DatasetType id is required.

```python
from observatory.api.client.api.observatory_api import ObservatoryApi
from observatory.api.client import ApiClient, Configuration

configuration = Configuration(host=f"http://host:port")
api = ObservatoryApi(api_client=ApiClient(configuration))

dataset_type_id = 1
dataset_type = api.get_dataset_type(dataset_type_id)
```

A list of DatasetType objects can be retrieved using `get_dataset_types`. A limit on the number of results returned must be specified.

```python
telescope_id = 1
limit = 100
all_dataset_types = api.get_dataset_types(limit)
```

### Create a DatasetType object

New DatasetType objects can be created using `put` and `post` methods. The DatasetType id is omitted from the API call. A TableType id must be specified.

#### Put call

```python
from observatory.api.client.model.dataset_type import DatasetType

obj = DatasetType(
    name="My dataset type",
    type_id="type_id",
    table_type=TableType(id=1),  # Assuming TableType with id=1 exists
)
api.put_dataset_type(obj)
```

#### Post call

```python
obj = DatasetType(
    name="My dataset type",
    type_id="type_id",
    table_type=TableType(id=1),  # Assuming TableType with id=1 exists
)
api.post_dataset_type(obj)
```

### Modify an existing DatasetType object

An existing DatasetType object can be modified through a `put` call by also specifying its DatasetType id.

```python
id_ = 1
obj = DatasetType(
    id=id_,
    name="New dataset type name",
)
api.put_dataset_type(obj)
```

### Deleting DatasetType objects

A DatasetType object can be deleted by specifying its DatasetType id.

```python
object_id_to_delete=123
api.delete_dataset_type(object_id_to_delete)
```

## Dataset

### Get a Dataset object

A specific Dataset object can be retrieved using `get_dataset`. A dataset ID is required.

```python
from observatory.api.client.api.observatory_api import ObservatoryApi
from observatory.api.client import ApiClient, Configuration

configuration = Configuration(host=f"http://host:port")
api = ObservatoryApi(api_client=ApiClient(configuration))

dataset_id = 1
dataset = api.get_dataset(dataset_id)
```

A list of Dataset objects can be retrieved using `get_datasets`. A limit on the number of results returned must be specified. The telescope ID can optionally be specified to limit results to a given Workflow ID.

```python
telescope_id = 1
limit = 100
all_datasets = api.get_datasets(limit)
filtered_datasets = api.get_datasets(limit, telescope_id)
```

### Create a Dataset object

New Dataset objects can be created using `put` and `post` methods. The dataset id is omitted from the API call. The Dataset must be linked to a Workflow object with a Workflow id set.

The `service` field is the name of the service used to store the dataset, e.g., "bigquery".
The `address` field is a standardised URI for the service. For example, the BigQuery service uses the format `project_id.dataset_id.table_name`.

#### Put call

```python
from observatory.api.client.model.dataset import Dataset
from observatory.api.client.model.workflow import Workflow

related_workflow = Workflow(id=123)
obj = Dataset(
    name="My dataset",
    workflow=related_workflow,
    service="bigquery",
    address="project.dataset.table",
)
api.put_dataset(obj)
```

#### Post call

```python
related_workflow = Workflow(id=123)
obj = Dataset(
    name="My dataset",
    extra="{}",
    workflow=related_workflow,
)
api.post_dataset_release(obj)
```

### Modify an existing Dataset object

An existing Dataset object can be modified through a `put` call by also specifying its Dataset ID.

```python
related_workflow = Workflow(id=123)
dataset_id = 1
obj = DatasetRelease(
    id=dataset_id,
    extra='{"type": "snapshot"}',
    workflow=related_workflow,
)
api.put_dataset(obj)
```

### Deleting Dataset objects

A Dataset object can be deleted by specifying its Dataset ID.

```python
object_id_to_delete=123
api.delete_dataset_release(object_id_to_delete)
```

## DatasetRelease

### Get a DatasetRelease object

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

### Create a DatasetRelease object

New DatasetRelease objects can be created using `put` and `post` methods. The dataset_release_id is omitted from the API call. The DatasetRelease must be linked to a Dataset object with an ID set.

#### Put call

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

#### Post call

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

### Modify an existing DatasetRelease object

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

### Deleting DatasetRelease objects

A DatasetStorage object can be deleted by specifying its DatasetRelease ID.

```python
object_id_to_delete=123
api.delete_dataset_release(object_id_to_delete)
```
