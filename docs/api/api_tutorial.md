# Python API Tutorial

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

A list of Dataset objects can be retrieved using `get_datasets`. A limit on the number of results returned must be specified. The telescope ID can optionally be specified to limit results to a given Telescope ID.

```python
telescope_id = 1
limit = 100
all_datasets = api.get_datasets(limit)
filtered_datasets = api.get_datasets(limit, telescope_id)
```

### Create a Dataset object

New Dataset objects can be created using `put` and `post` methods. The dataset_id is omitted from the API call. The Dataset must be linked to a Telescope object with an ID set.

#### Put call

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

#### Post call

```python
related_telescope = Telescope(id=123)
obj = Dataset(
    name="My dataset",
    extra="{}",
    connection=related_telescope,
)
api.post_dataset_release(obj)
```

### Modify an existing Dataset object

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

## DatasetStorage

### Get a DatasetStorage object

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

### Create a DatasetStorage object

New DatasetStorage objects can be created using `put` and `post` methods. The dataset_storage_id is omitted from the API call. DatasetStorage must be linked to a Dataset object with an ID set.

#### Put call
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

#### Post call

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

### Modify an existing DatasetStorage object

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

### Deleting DatasetStorage objects

A DatasetStorage object can be deleted by specifying its DatasetStorage ID.

```python
object_id_to_delete=123
api.delete_dataset_storage(object_id_to_delete)
```