# ObservatoryApi

All URIs are relative to *https://localhost:5002*

<div class="wy-table-responsive"><table border="1" class="docutils">
<thead>
<tr>
<th>Method</th>
<th>HTTP request</th>
<th>Description</th>
</tr>
</thead>
<tbody>


<tr>
  <td><a href="ObservatoryApi.html#delete_dataset_release"><strong>delete_dataset_release</strong></a></td>
  <td><strong>DELETE</strong> /v1/dataset_release</td>
  <td>delete a DatasetRelease</td>
</tr>

<tr>
  <td><a href="ObservatoryApi.html#get_dataset_release"><strong>get_dataset_release</strong></a></td>
  <td><strong>GET</strong> /v1/dataset_release</td>
  <td>get a DatasetRelease</td>
</tr>

<tr>
  <td><a href="ObservatoryApi.html#get_dataset_releases"><strong>get_dataset_releases</strong></a></td>
  <td><strong>GET</strong> /v1/dataset_releases</td>
  <td>Get a list of DatasetRelease objects</td>
</tr>

<tr>
  <td><a href="ObservatoryApi.html#post_dataset_release"><strong>post_dataset_release</strong></a></td>
  <td><strong>POST</strong> /v1/dataset_release</td>
  <td>create a DatasetRelease</td>
</tr>

<tr>
  <td><a href="ObservatoryApi.html#put_dataset_release"><strong>put_dataset_release</strong></a></td>
  <td><strong>PUT</strong> /v1/dataset_release</td>
  <td>create or update a DatasetRelease</td>
</tr>


</tbody>
</table></div>

## **delete_dataset_release**
> delete_dataset_release(id)

delete a DatasetRelease

Delete a DatasetRelease by passing it's id. 

### Example

* Api Key Authentication (api_key):
```python
import time
import observatory.api.client
from observatory.api.client.api import observatory_api
from pprint import pprint
# Defining the host is optional and defaults to https://localhost:5002
# See configuration.py for a list of all supported configuration parameters.
configuration = observatory.api.client.Configuration(
    host = "https://localhost:5002"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure API key authorization: api_key
configuration.api_key['api_key'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['api_key'] = 'Bearer'

# Enter a context with an instance of the API client
with observatory.api.client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = observatory_api.ObservatoryApi(api_client)
    id = 1 # int | DatasetRelease id

    # example passing only required values which don't have defaults set
    try:
        # delete a DatasetRelease
        api_instance.delete_dataset_release(id)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->delete_dataset_release: %s\n" % e)
```


### Parameters


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
<td>DatasetRelease id</td>
<td></td>
</tr>




</tbody>
</table></div>


### Return type

void (empty response body)

### Authorization

[api_key](ObservatoryApi.html#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined


### HTTP response details
<div class="wy-table-responsive"><table border="1" class="docutils">
<thead>
<tr>
<th>Status code</th>
<th>Description</th>
<th>Response headers</th>
</tr>
</thead>
<tbody>

<tr>
    <td><strong>200</strong></td>
    <td>DatasetRelease deleted</td>
    <td> - </td>
</tr>

</tbody>
</table></div>

## **get_dataset_release**
> DatasetRelease get_dataset_release(id)

get a DatasetRelease

Get the details of a DatasetRelease by passing it's id. 

### Example

* Api Key Authentication (api_key):
```python
import time
import observatory.api.client
from observatory.api.client.api import observatory_api
from observatory.api.client.model.dataset_release import DatasetRelease
from pprint import pprint
# Defining the host is optional and defaults to https://localhost:5002
# See configuration.py for a list of all supported configuration parameters.
configuration = observatory.api.client.Configuration(
    host = "https://localhost:5002"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure API key authorization: api_key
configuration.api_key['api_key'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['api_key'] = 'Bearer'

# Enter a context with an instance of the API client
with observatory.api.client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = observatory_api.ObservatoryApi(api_client)
    id = 1 # int | DatasetRelease id

    # example passing only required values which don't have defaults set
    try:
        # get a DatasetRelease
        api_response = api_instance.get_dataset_release(id)
        pprint(api_response)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->get_dataset_release: %s\n" % e)
```


### Parameters


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
<td>DatasetRelease id</td>
<td></td>
</tr>




</tbody>
</table></div>


### Return type

[**DatasetRelease**](DatasetRelease.html)

### Authorization

[api_key](ObservatoryApi.html#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
<div class="wy-table-responsive"><table border="1" class="docutils">
<thead>
<tr>
<th>Status code</th>
<th>Description</th>
<th>Response headers</th>
</tr>
</thead>
<tbody>

<tr>
    <td><strong>200</strong></td>
    <td>the fetched DatasetRelease</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>400</strong></td>
    <td>bad input parameter</td>
    <td> - </td>
</tr>

</tbody>
</table></div>

## **get_dataset_releases**
> [DatasetRelease] get_dataset_releases()

Get a list of DatasetRelease objects

Get a list of DatasetRelease objects 

### Example

* Api Key Authentication (api_key):
```python
import time
import observatory.api.client
from observatory.api.client.api import observatory_api
from observatory.api.client.model.dataset_release import DatasetRelease
from pprint import pprint
# Defining the host is optional and defaults to https://localhost:5002
# See configuration.py for a list of all supported configuration parameters.
configuration = observatory.api.client.Configuration(
    host = "https://localhost:5002"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure API key authorization: api_key
configuration.api_key['api_key'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['api_key'] = 'Bearer'

# Enter a context with an instance of the API client
with observatory.api.client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = observatory_api.ObservatoryApi(api_client)
    dag_id = "dag_id_example" # str | the dag_id to fetch release info for (optional)
    dataset_id = "dataset_id_example" # str | the dataset_id to fetch release info for (optional)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # Get a list of DatasetRelease objects
        api_response = api_instance.get_dataset_releases(dag_id=dag_id, dataset_id=dataset_id)
        pprint(api_response)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->get_dataset_releases: %s\n" % e)
```


### Parameters


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
<td><strong>dag_id</strong></td>
<td><strong>str</strong></td>
<td>the dag_id to fetch release info for</td>
<td>
[optional]
<tr>
<td><strong>dataset_id</strong></td>
<td><strong>str</strong></td>
<td>the dataset_id to fetch release info for</td>
<td>
[optional]
</tbody>
</table></div>


### Return type

[**[DatasetRelease]**](DatasetRelease.html)

### Authorization

[api_key](ObservatoryApi.html#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
<div class="wy-table-responsive"><table border="1" class="docutils">
<thead>
<tr>
<th>Status code</th>
<th>Description</th>
<th>Response headers</th>
</tr>
</thead>
<tbody>

<tr>
    <td><strong>200</strong></td>
    <td>a list of DatasetRelease objects</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>400</strong></td>
    <td>bad input parameter</td>
    <td> - </td>
</tr>

</tbody>
</table></div>

## **post_dataset_release**
> DatasetRelease post_dataset_release(body)

create a DatasetRelease

Create a DatasetRelease by passing a DatasetRelease object, without an id. 

### Example

* Api Key Authentication (api_key):
```python
import time
import observatory.api.client
from observatory.api.client.api import observatory_api
from observatory.api.client.model.dataset_release import DatasetRelease
from pprint import pprint
# Defining the host is optional and defaults to https://localhost:5002
# See configuration.py for a list of all supported configuration parameters.
configuration = observatory.api.client.Configuration(
    host = "https://localhost:5002"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure API key authorization: api_key
configuration.api_key['api_key'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['api_key'] = 'Bearer'

# Enter a context with an instance of the API client
with observatory.api.client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = observatory_api.ObservatoryApi(api_client)
    body = DatasetRelease(
        id=1,
        dag_id="doi_workflow",
        dataset_id="doi",
        dag_run_id="YYYY-MM-DDTHH:mm:ss.ssssss",
        data_interval_start=dateutil_parser('2020-01-02T20:01:05Z'),
        data_interval_end=dateutil_parser('2020-01-02T20:01:05Z'),
        snapshot_date=dateutil_parser('2020-01-02T20:01:05Z'),
        partition_date=dateutil_parser('2020-01-02T20:01:05Z'),
        changefile_start_date=dateutil_parser('2020-01-02T20:01:05Z'),
        changefile_end_date=dateutil_parser('2020-01-02T20:01:05Z'),
        sequence_start=1,
        sequence_end=3,
        extra={},
    ) # DatasetRelease | DatasetRelease to create

    # example passing only required values which don't have defaults set
    try:
        # create a DatasetRelease
        api_response = api_instance.post_dataset_release(body)
        pprint(api_response)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->post_dataset_release: %s\n" % e)
```


### Parameters


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
<td><strong>body</strong></td>
<td><a href="DatasetRelease.html"><strong>DatasetRelease</strong></a></td>
<td>DatasetRelease to create</td>
<td></td>
</tr>




</tbody>
</table></div>


### Return type

[**DatasetRelease**](DatasetRelease.html)

### Authorization

[api_key](ObservatoryApi.html#api_key)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
<div class="wy-table-responsive"><table border="1" class="docutils">
<thead>
<tr>
<th>Status code</th>
<th>Description</th>
<th>Response headers</th>
</tr>
</thead>
<tbody>

<tr>
    <td><strong>201</strong></td>
    <td>DatasetRelease created, returning the created object with an id</td>
    <td> - </td>
</tr>

</tbody>
</table></div>

## **put_dataset_release**
> DatasetRelease put_dataset_release(body)

create or update a DatasetRelease

Create a DatasetRelease by passing a DatasetRelease object, without an id. Update an existing DatasetRelease by passing a DatasetRelease object with an id. 

### Example

* Api Key Authentication (api_key):
```python
import time
import observatory.api.client
from observatory.api.client.api import observatory_api
from observatory.api.client.model.dataset_release import DatasetRelease
from pprint import pprint
# Defining the host is optional and defaults to https://localhost:5002
# See configuration.py for a list of all supported configuration parameters.
configuration = observatory.api.client.Configuration(
    host = "https://localhost:5002"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure API key authorization: api_key
configuration.api_key['api_key'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['api_key'] = 'Bearer'

# Enter a context with an instance of the API client
with observatory.api.client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = observatory_api.ObservatoryApi(api_client)
    body = DatasetRelease(
        id=1,
        dag_id="doi_workflow",
        dataset_id="doi",
        dag_run_id="YYYY-MM-DDTHH:mm:ss.ssssss",
        data_interval_start=dateutil_parser('2020-01-02T20:01:05Z'),
        data_interval_end=dateutil_parser('2020-01-02T20:01:05Z'),
        snapshot_date=dateutil_parser('2020-01-02T20:01:05Z'),
        partition_date=dateutil_parser('2020-01-02T20:01:05Z'),
        changefile_start_date=dateutil_parser('2020-01-02T20:01:05Z'),
        changefile_end_date=dateutil_parser('2020-01-02T20:01:05Z'),
        sequence_start=1,
        sequence_end=3,
        extra={},
    ) # DatasetRelease | DatasetRelease to create or update

    # example passing only required values which don't have defaults set
    try:
        # create or update a DatasetRelease
        api_response = api_instance.put_dataset_release(body)
        pprint(api_response)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->put_dataset_release: %s\n" % e)
```


### Parameters


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
<td><strong>body</strong></td>
<td><a href="DatasetRelease.html"><strong>DatasetRelease</strong></a></td>
<td>DatasetRelease to create or update</td>
<td></td>
</tr>




</tbody>
</table></div>


### Return type

[**DatasetRelease**](DatasetRelease.html)

### Authorization

[api_key](ObservatoryApi.html#api_key)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
<div class="wy-table-responsive"><table border="1" class="docutils">
<thead>
<tr>
<th>Status code</th>
<th>Description</th>
<th>Response headers</th>
</tr>
</thead>
<tbody>

<tr>
    <td><strong>200</strong></td>
    <td>DatasetRelease updated</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>201</strong></td>
    <td>DatasetRelease created, returning the created object with an id</td>
    <td> - </td>
</tr>

</tbody>
</table></div>

