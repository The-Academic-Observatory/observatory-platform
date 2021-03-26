# ObservatoryApi

All URIs are relative to *https://api.observatory.academy*

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
  <td><a href="ObservatoryApi.html#delete_organisation"><strong>delete_organisation</strong></a></td>
  <td><strong>DELETE</strong> /v1/organisation</td>
  <td>delete an Organisation</td>
</tr>

<tr>
  <td><a href="ObservatoryApi.html#delete_telescope"><strong>delete_telescope</strong></a></td>
  <td><strong>DELETE</strong> /v1/telescope</td>
  <td>delete a Telescope</td>
</tr>

<tr>
  <td><a href="ObservatoryApi.html#delete_telescope_type"><strong>delete_telescope_type</strong></a></td>
  <td><strong>DELETE</strong> /v1/telescope_type</td>
  <td>delete a TelescopeType</td>
</tr>

<tr>
  <td><a href="ObservatoryApi.html#get_organisation"><strong>get_organisation</strong></a></td>
  <td><strong>GET</strong> /v1/organisation</td>
  <td>get an Organisation</td>
</tr>

<tr>
  <td><a href="ObservatoryApi.html#get_organisations"><strong>get_organisations</strong></a></td>
  <td><strong>GET</strong> /v1/organisations</td>
  <td>Get a list of Organisations</td>
</tr>

<tr>
  <td><a href="ObservatoryApi.html#get_telescope"><strong>get_telescope</strong></a></td>
  <td><strong>GET</strong> /v1/telescope</td>
  <td>get a Telescope</td>
</tr>

<tr>
  <td><a href="ObservatoryApi.html#get_telescope_type"><strong>get_telescope_type</strong></a></td>
  <td><strong>GET</strong> /v1/telescope_type</td>
  <td>get a TelescopeType</td>
</tr>

<tr>
  <td><a href="ObservatoryApi.html#get_telescope_types"><strong>get_telescope_types</strong></a></td>
  <td><strong>GET</strong> /v1/telescope_types</td>
  <td>Get a list of TelescopeType objects</td>
</tr>

<tr>
  <td><a href="ObservatoryApi.html#get_telescopes"><strong>get_telescopes</strong></a></td>
  <td><strong>GET</strong> /v1/telescopes</td>
  <td>Get a list of Telescope objects</td>
</tr>

<tr>
  <td><a href="ObservatoryApi.html#post_organisation"><strong>post_organisation</strong></a></td>
  <td><strong>POST</strong> /v1/organisation</td>
  <td>create an Organisation</td>
</tr>

<tr>
  <td><a href="ObservatoryApi.html#post_telescope"><strong>post_telescope</strong></a></td>
  <td><strong>POST</strong> /v1/telescope</td>
  <td>create a Telescope</td>
</tr>

<tr>
  <td><a href="ObservatoryApi.html#post_telescope_type"><strong>post_telescope_type</strong></a></td>
  <td><strong>POST</strong> /v1/telescope_type</td>
  <td>create a TelescopeType</td>
</tr>

<tr>
  <td><a href="ObservatoryApi.html#put_organisation"><strong>put_organisation</strong></a></td>
  <td><strong>PUT</strong> /v1/organisation</td>
  <td>create or update an Organisation</td>
</tr>

<tr>
  <td><a href="ObservatoryApi.html#put_telescope"><strong>put_telescope</strong></a></td>
  <td><strong>PUT</strong> /v1/telescope</td>
  <td>create or update a Telescope</td>
</tr>

<tr>
  <td><a href="ObservatoryApi.html#put_telescope_type"><strong>put_telescope_type</strong></a></td>
  <td><strong>PUT</strong> /v1/telescope_type</td>
  <td>create or update a TelescopeType</td>
</tr>

<tr>
  <td><a href="ObservatoryApi.html#queryv1"><strong>queryv1</strong></a></td>
  <td><strong>GET</strong> /v1/query</td>
  <td>Search the Observatory API</td>
</tr>


</tbody>
</table></div>

## **delete_organisation**
> delete_organisation(id)

delete an Organisation

Delete an Organisation by passing it's id. 

### Example

* Api Key Authentication (api_key):
```python
import time
import observatory.api.client
from observatory.api.client.api import observatory_api
from pprint import pprint
# Defining the host is optional and defaults to https://api.observatory.academy
# See configuration.py for a list of all supported configuration parameters.
configuration = observatory.api.client.Configuration(
    host = "https://api.observatory.academy"
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
    id = 1 # int | Organisation id

    # example passing only required values which don't have defaults set
    try:
        # delete an Organisation
        api_instance.delete_organisation(id)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->delete_organisation: %s\n" % e)
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
<td>Organisation id</td>
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
    <td>Organisation deleted</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>401</strong></td>
    <td>API key is missing or invalid</td>
    <td> * WWW_Authenticate -  <br> </td>
</tr>

</tbody>
</table></div>

## **delete_telescope**
> delete_telescope(id)

delete a Telescope

Delete a Telescope by passing it's id. 

### Example

* Api Key Authentication (api_key):
```python
import time
import observatory.api.client
from observatory.api.client.api import observatory_api
from pprint import pprint
# Defining the host is optional and defaults to https://api.observatory.academy
# See configuration.py for a list of all supported configuration parameters.
configuration = observatory.api.client.Configuration(
    host = "https://api.observatory.academy"
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
    id = 1 # int | Telescope id

    # example passing only required values which don't have defaults set
    try:
        # delete a Telescope
        api_instance.delete_telescope(id)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->delete_telescope: %s\n" % e)
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
<td>Telescope id</td>
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
    <td>Telescope deleted</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>401</strong></td>
    <td>API key is missing or invalid</td>
    <td> * WWW_Authenticate -  <br> </td>
</tr>

</tbody>
</table></div>

## **delete_telescope_type**
> delete_telescope_type(id)

delete a TelescopeType

Delete a TelescopeType by passing it's id. 

### Example

* Api Key Authentication (api_key):
```python
import time
import observatory.api.client
from observatory.api.client.api import observatory_api
from pprint import pprint
# Defining the host is optional and defaults to https://api.observatory.academy
# See configuration.py for a list of all supported configuration parameters.
configuration = observatory.api.client.Configuration(
    host = "https://api.observatory.academy"
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
    id = 1 # int | TelescopeType id

    # example passing only required values which don't have defaults set
    try:
        # delete a TelescopeType
        api_instance.delete_telescope_type(id)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->delete_telescope_type: %s\n" % e)
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
<td>TelescopeType id</td>
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
    <td>TelescopeType deleted</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>401</strong></td>
    <td>API key is missing or invalid</td>
    <td> * WWW_Authenticate -  <br> </td>
</tr>

</tbody>
</table></div>

## **get_organisation**
> Organisation get_organisation(id)

get an Organisation

Get the details of an Organisation by passing it's id. 

### Example

* Api Key Authentication (api_key):
```python
import time
import observatory.api.client
from observatory.api.client.api import observatory_api
from observatory.api.client.model.organisation import Organisation
from pprint import pprint
# Defining the host is optional and defaults to https://api.observatory.academy
# See configuration.py for a list of all supported configuration parameters.
configuration = observatory.api.client.Configuration(
    host = "https://api.observatory.academy"
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
    id = 1 # int | Organisation id

    # example passing only required values which don't have defaults set
    try:
        # get an Organisation
        api_response = api_instance.get_organisation(id)
        pprint(api_response)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->get_organisation: %s\n" % e)
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
<td>Organisation id</td>
<td></td>
</tr>




</tbody>
</table></div>


### Return type

[**Organisation**](Organisation.html)

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
    <td>the fetched Organisation</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>400</strong></td>
    <td>bad input parameter</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>401</strong></td>
    <td>API key is missing or invalid</td>
    <td> * WWW_Authenticate -  <br> </td>
</tr>

</tbody>
</table></div>

## **get_organisations**
> [Organisation] get_organisations(limit)

Get a list of Organisations

Gets a list of organisations 

### Example

* Api Key Authentication (api_key):
```python
import time
import observatory.api.client
from observatory.api.client.api import observatory_api
from observatory.api.client.model.organisation import Organisation
from pprint import pprint
# Defining the host is optional and defaults to https://api.observatory.academy
# See configuration.py for a list of all supported configuration parameters.
configuration = observatory.api.client.Configuration(
    host = "https://api.observatory.academy"
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
    limit = 1 # int | the maximum number of results to return

    # example passing only required values which don't have defaults set
    try:
        # Get a list of Organisations
        api_response = api_instance.get_organisations(limit)
        pprint(api_response)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->get_organisations: %s\n" % e)
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
<td><strong>limit</strong></td>
<td><strong>int</strong></td>
<td>the maximum number of results to return</td>
<td></td>
</tr>




</tbody>
</table></div>


### Return type

[**[Organisation]**](Organisation.html)

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
    <td>a list of Organisation objects</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>400</strong></td>
    <td>bad input parameter</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>401</strong></td>
    <td>API key is missing or invalid</td>
    <td> * WWW_Authenticate -  <br> </td>
</tr>

</tbody>
</table></div>

## **get_telescope**
> Telescope get_telescope(id)

get a Telescope

Get the details of a Telescope by passing it's id. 

### Example

* Api Key Authentication (api_key):
```python
import time
import observatory.api.client
from observatory.api.client.api import observatory_api
from observatory.api.client.model.telescope import Telescope
from pprint import pprint
# Defining the host is optional and defaults to https://api.observatory.academy
# See configuration.py for a list of all supported configuration parameters.
configuration = observatory.api.client.Configuration(
    host = "https://api.observatory.academy"
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
    id = 1 # int | Telescope id

    # example passing only required values which don't have defaults set
    try:
        # get a Telescope
        api_response = api_instance.get_telescope(id)
        pprint(api_response)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->get_telescope: %s\n" % e)
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
<td>Telescope id</td>
<td></td>
</tr>




</tbody>
</table></div>


### Return type

[**Telescope**](Telescope.html)

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
    <td>the fetched Telescope</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>400</strong></td>
    <td>bad input parameter</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>401</strong></td>
    <td>API key is missing or invalid</td>
    <td> * WWW_Authenticate -  <br> </td>
</tr>

</tbody>
</table></div>

## **get_telescope_type**
> TelescopeType get_telescope_type()

get a TelescopeType

Get the details of a TelescopeType by passing it's id or type_id. 

### Example

* Api Key Authentication (api_key):
```python
import time
import observatory.api.client
from observatory.api.client.api import observatory_api
from observatory.api.client.model.telescope_type import TelescopeType
from pprint import pprint
# Defining the host is optional and defaults to https://api.observatory.academy
# See configuration.py for a list of all supported configuration parameters.
configuration = observatory.api.client.Configuration(
    host = "https://api.observatory.academy"
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
    id = 1 # int | TelescopeType id (optional)
    type_id = "type_id_example" # str | TelescopeType type_id (optional)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # get a TelescopeType
        api_response = api_instance.get_telescope_type(id=id, type_id=type_id)
        pprint(api_response)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->get_telescope_type: %s\n" % e)
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
<td>TelescopeType id</td>
<td>
[optional]
<tr>
<td><strong>type_id</strong></td>
<td><strong>str</strong></td>
<td>TelescopeType type_id</td>
<td>
[optional]
</tbody>
</table></div>


### Return type

[**TelescopeType**](TelescopeType.html)

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
    <td>the fetched TelescopeType</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>400</strong></td>
    <td>bad input parameter</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>401</strong></td>
    <td>API key is missing or invalid</td>
    <td> * WWW_Authenticate -  <br> </td>
</tr>

</tbody>
</table></div>

## **get_telescope_types**
> [TelescopeType] get_telescope_types(limit)

Get a list of TelescopeType objects

Get a list of TelescopeType objects 

### Example

* Api Key Authentication (api_key):
```python
import time
import observatory.api.client
from observatory.api.client.api import observatory_api
from observatory.api.client.model.telescope_type import TelescopeType
from pprint import pprint
# Defining the host is optional and defaults to https://api.observatory.academy
# See configuration.py for a list of all supported configuration parameters.
configuration = observatory.api.client.Configuration(
    host = "https://api.observatory.academy"
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
    limit = 1 # int | the maximum number of results to return

    # example passing only required values which don't have defaults set
    try:
        # Get a list of TelescopeType objects
        api_response = api_instance.get_telescope_types(limit)
        pprint(api_response)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->get_telescope_types: %s\n" % e)
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
<td><strong>limit</strong></td>
<td><strong>int</strong></td>
<td>the maximum number of results to return</td>
<td></td>
</tr>




</tbody>
</table></div>


### Return type

[**[TelescopeType]**](TelescopeType.html)

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
    <td>a list of TelescopeType objects</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>400</strong></td>
    <td>bad input parameter</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>401</strong></td>
    <td>API key is missing or invalid</td>
    <td> * WWW_Authenticate -  <br> </td>
</tr>

</tbody>
</table></div>

## **get_telescopes**
> [Telescope] get_telescopes(limit)

Get a list of Telescope objects

Get a list of Telescope objects and optionally filter via a Telescope id and or an Organisation id. 

### Example

* Api Key Authentication (api_key):
```python
import time
import observatory.api.client
from observatory.api.client.api import observatory_api
from observatory.api.client.model.telescope import Telescope
from pprint import pprint
# Defining the host is optional and defaults to https://api.observatory.academy
# See configuration.py for a list of all supported configuration parameters.
configuration = observatory.api.client.Configuration(
    host = "https://api.observatory.academy"
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
    limit = 1 # int | the maximum number of results to return
    telescope_type_id = 1 # int | filter telescopes by a TelescopeType id (optional)
    organisation_id = 1 # int | filter telescopes by an Organisation id (optional)

    # example passing only required values which don't have defaults set
    try:
        # Get a list of Telescope objects
        api_response = api_instance.get_telescopes(limit)
        pprint(api_response)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->get_telescopes: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # Get a list of Telescope objects
        api_response = api_instance.get_telescopes(limit, telescope_type_id=telescope_type_id, organisation_id=organisation_id)
        pprint(api_response)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->get_telescopes: %s\n" % e)
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
<td><strong>limit</strong></td>
<td><strong>int</strong></td>
<td>the maximum number of results to return</td>
<td></td>
</tr>




<tr>
<td><strong>telescope_type_id</strong></td>
<td><strong>int</strong></td>
<td>filter telescopes by a TelescopeType id</td>
<td>
[optional]
<tr>
<td><strong>organisation_id</strong></td>
<td><strong>int</strong></td>
<td>filter telescopes by an Organisation id</td>
<td>
[optional]
</tbody>
</table></div>


### Return type

[**[Telescope]**](Telescope.html)

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
    <td>a list of Telescope objects</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>400</strong></td>
    <td>bad input parameter</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>401</strong></td>
    <td>API key is missing or invalid</td>
    <td> * WWW_Authenticate -  <br> </td>
</tr>

</tbody>
</table></div>

## **post_organisation**
> Organisation post_organisation(body)

create an Organisation

Create an Organisation by passing an Organisation object, without an id. 

### Example

* Api Key Authentication (api_key):
```python
import time
import observatory.api.client
from observatory.api.client.api import observatory_api
from observatory.api.client.model.organisation import Organisation
from pprint import pprint
# Defining the host is optional and defaults to https://api.observatory.academy
# See configuration.py for a list of all supported configuration parameters.
configuration = observatory.api.client.Configuration(
    host = "https://api.observatory.academy"
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
    body = Organisation(
        id=1,
        name="Curtin University",
        gcp_project_id="curtin-dev",
        gcp_download_bucket="curtin-dev-download",
        gcp_transform_bucket="curtin-dev-transform",
        telescopes=[
            Telescope(
                id=1,
                name="Curtin University ONIX Telescope",
                extra={},
                organisation=Organisation(Organisation),
                telescope_type=TelescopeType(
                    id=1,
                    type_id="onix",
                    name="Scopus",
                    created=dateutil_parser('1970-01-01T00:00:00.00Z'),
                    modified=dateutil_parser('1970-01-01T00:00:00.00Z'),
                ),
                created=dateutil_parser('1970-01-01T00:00:00.00Z'),
                modified=dateutil_parser('1970-01-01T00:00:00.00Z'),
            ),
        ],
        created=dateutil_parser('1970-01-01T00:00:00.00Z'),
        modified=dateutil_parser('1970-01-01T00:00:00.00Z'),
    ) # Organisation | Organisation to create

    # example passing only required values which don't have defaults set
    try:
        # create an Organisation
        api_response = api_instance.post_organisation(body)
        pprint(api_response)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->post_organisation: %s\n" % e)
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
<td><a href="Organisation.html"><strong>Organisation</strong></a></td>
<td>Organisation to create</td>
<td></td>
</tr>




</tbody>
</table></div>


### Return type

[**Organisation**](Organisation.html)

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
    <td>Organisation created, returning the created object with an id</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>401</strong></td>
    <td>API key is missing or invalid</td>
    <td> * WWW_Authenticate -  <br> </td>
</tr>

</tbody>
</table></div>

## **post_telescope**
> Telescope post_telescope(body)

create a Telescope

Create a Telescope by passing a Telescope object, without an id. 

### Example

* Api Key Authentication (api_key):
```python
import time
import observatory.api.client
from observatory.api.client.api import observatory_api
from observatory.api.client.model.telescope import Telescope
from pprint import pprint
# Defining the host is optional and defaults to https://api.observatory.academy
# See configuration.py for a list of all supported configuration parameters.
configuration = observatory.api.client.Configuration(
    host = "https://api.observatory.academy"
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
    body = Telescope(
        id=1,
        name="Curtin University ONIX Telescope",
        extra={},
        organisation=Organisation(
            id=1,
            name="Curtin University",
            gcp_project_id="curtin-dev",
            gcp_download_bucket="curtin-dev-download",
            gcp_transform_bucket="curtin-dev-transform",
            telescopes=[
                Telescope(Telescope),
            ],
            created=dateutil_parser('1970-01-01T00:00:00.00Z'),
            modified=dateutil_parser('1970-01-01T00:00:00.00Z'),
        ),
        telescope_type=TelescopeType(
            id=1,
            type_id="onix",
            name="Scopus",
            created=dateutil_parser('1970-01-01T00:00:00.00Z'),
            modified=dateutil_parser('1970-01-01T00:00:00.00Z'),
        ),
        created=dateutil_parser('1970-01-01T00:00:00.00Z'),
        modified=dateutil_parser('1970-01-01T00:00:00.00Z'),
    ) # Telescope | Telescope to create

    # example passing only required values which don't have defaults set
    try:
        # create a Telescope
        api_response = api_instance.post_telescope(body)
        pprint(api_response)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->post_telescope: %s\n" % e)
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
<td><a href="Telescope.html"><strong>Telescope</strong></a></td>
<td>Telescope to create</td>
<td></td>
</tr>




</tbody>
</table></div>


### Return type

[**Telescope**](Telescope.html)

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
    <td>Telescope created, returning the created object with an id</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>401</strong></td>
    <td>API key is missing or invalid</td>
    <td> * WWW_Authenticate -  <br> </td>
</tr>

</tbody>
</table></div>

## **post_telescope_type**
> TelescopeType post_telescope_type(body)

create a TelescopeType

Create a TelescopeType by passing a TelescopeType object, without an id. 

### Example

* Api Key Authentication (api_key):
```python
import time
import observatory.api.client
from observatory.api.client.api import observatory_api
from observatory.api.client.model.telescope_type import TelescopeType
from pprint import pprint
# Defining the host is optional and defaults to https://api.observatory.academy
# See configuration.py for a list of all supported configuration parameters.
configuration = observatory.api.client.Configuration(
    host = "https://api.observatory.academy"
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
    body = TelescopeType(
        id=1,
        type_id="onix",
        name="Scopus",
        created=dateutil_parser('1970-01-01T00:00:00.00Z'),
        modified=dateutil_parser('1970-01-01T00:00:00.00Z'),
    ) # TelescopeType | TelescopeType to create

    # example passing only required values which don't have defaults set
    try:
        # create a TelescopeType
        api_response = api_instance.post_telescope_type(body)
        pprint(api_response)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->post_telescope_type: %s\n" % e)
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
<td><a href="TelescopeType.html"><strong>TelescopeType</strong></a></td>
<td>TelescopeType to create</td>
<td></td>
</tr>




</tbody>
</table></div>


### Return type

[**TelescopeType**](TelescopeType.html)

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
    <td>TelescopeType created, returning the created object with an id</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>401</strong></td>
    <td>API key is missing or invalid</td>
    <td> * WWW_Authenticate -  <br> </td>
</tr>

</tbody>
</table></div>

## **put_organisation**
> Organisation put_organisation(body)

create or update an Organisation

Create an Organisation by passing an Organisation object, without an id. Update an existing Organisation by passing an Organisation object with an id. 

### Example

* Api Key Authentication (api_key):
```python
import time
import observatory.api.client
from observatory.api.client.api import observatory_api
from observatory.api.client.model.organisation import Organisation
from pprint import pprint
# Defining the host is optional and defaults to https://api.observatory.academy
# See configuration.py for a list of all supported configuration parameters.
configuration = observatory.api.client.Configuration(
    host = "https://api.observatory.academy"
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
    body = Organisation(
        id=1,
        name="Curtin University",
        gcp_project_id="curtin-dev",
        gcp_download_bucket="curtin-dev-download",
        gcp_transform_bucket="curtin-dev-transform",
        telescopes=[
            Telescope(
                id=1,
                name="Curtin University ONIX Telescope",
                extra={},
                organisation=Organisation(Organisation),
                telescope_type=TelescopeType(
                    id=1,
                    type_id="onix",
                    name="Scopus",
                    created=dateutil_parser('1970-01-01T00:00:00.00Z'),
                    modified=dateutil_parser('1970-01-01T00:00:00.00Z'),
                ),
                created=dateutil_parser('1970-01-01T00:00:00.00Z'),
                modified=dateutil_parser('1970-01-01T00:00:00.00Z'),
            ),
        ],
        created=dateutil_parser('1970-01-01T00:00:00.00Z'),
        modified=dateutil_parser('1970-01-01T00:00:00.00Z'),
    ) # Organisation | Organisation to create or update

    # example passing only required values which don't have defaults set
    try:
        # create or update an Organisation
        api_response = api_instance.put_organisation(body)
        pprint(api_response)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->put_organisation: %s\n" % e)
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
<td><a href="Organisation.html"><strong>Organisation</strong></a></td>
<td>Organisation to create or update</td>
<td></td>
</tr>




</tbody>
</table></div>


### Return type

[**Organisation**](Organisation.html)

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
    <td>Organisation updated</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>201</strong></td>
    <td>Organisation created, returning the created object with an id</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>401</strong></td>
    <td>API key is missing or invalid</td>
    <td> * WWW_Authenticate -  <br> </td>
</tr>

</tbody>
</table></div>

## **put_telescope**
> Telescope put_telescope(body)

create or update a Telescope

Create a Telescope by passing a Telescope object, without an id. Update an existing Telescope by passing a Telescope object with an id. 

### Example

* Api Key Authentication (api_key):
```python
import time
import observatory.api.client
from observatory.api.client.api import observatory_api
from observatory.api.client.model.telescope import Telescope
from pprint import pprint
# Defining the host is optional and defaults to https://api.observatory.academy
# See configuration.py for a list of all supported configuration parameters.
configuration = observatory.api.client.Configuration(
    host = "https://api.observatory.academy"
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
    body = Telescope(
        id=1,
        name="Curtin University ONIX Telescope",
        extra={},
        organisation=Organisation(
            id=1,
            name="Curtin University",
            gcp_project_id="curtin-dev",
            gcp_download_bucket="curtin-dev-download",
            gcp_transform_bucket="curtin-dev-transform",
            telescopes=[
                Telescope(Telescope),
            ],
            created=dateutil_parser('1970-01-01T00:00:00.00Z'),
            modified=dateutil_parser('1970-01-01T00:00:00.00Z'),
        ),
        telescope_type=TelescopeType(
            id=1,
            type_id="onix",
            name="Scopus",
            created=dateutil_parser('1970-01-01T00:00:00.00Z'),
            modified=dateutil_parser('1970-01-01T00:00:00.00Z'),
        ),
        created=dateutil_parser('1970-01-01T00:00:00.00Z'),
        modified=dateutil_parser('1970-01-01T00:00:00.00Z'),
    ) # Telescope | Telescope to create or update

    # example passing only required values which don't have defaults set
    try:
        # create or update a Telescope
        api_response = api_instance.put_telescope(body)
        pprint(api_response)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->put_telescope: %s\n" % e)
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
<td><a href="Telescope.html"><strong>Telescope</strong></a></td>
<td>Telescope to create or update</td>
<td></td>
</tr>




</tbody>
</table></div>


### Return type

[**Telescope**](Telescope.html)

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
    <td>Telescope updated</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>201</strong></td>
    <td>Telescope created, returning the created object with an id</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>401</strong></td>
    <td>API key is missing or invalid</td>
    <td> * WWW_Authenticate -  <br> </td>
</tr>

</tbody>
</table></div>

## **put_telescope_type**
> TelescopeType put_telescope_type(body)

create or update a TelescopeType

Create a TelescopeType by passing a TelescopeType object, without an id. Update an existing TelescopeType by passing a TelescopeType object with an id. 

### Example

* Api Key Authentication (api_key):
```python
import time
import observatory.api.client
from observatory.api.client.api import observatory_api
from observatory.api.client.model.telescope_type import TelescopeType
from pprint import pprint
# Defining the host is optional and defaults to https://api.observatory.academy
# See configuration.py for a list of all supported configuration parameters.
configuration = observatory.api.client.Configuration(
    host = "https://api.observatory.academy"
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
    body = TelescopeType(
        id=1,
        type_id="onix",
        name="Scopus",
        created=dateutil_parser('1970-01-01T00:00:00.00Z'),
        modified=dateutil_parser('1970-01-01T00:00:00.00Z'),
    ) # TelescopeType | TelescopeType to create or update

    # example passing only required values which don't have defaults set
    try:
        # create or update a TelescopeType
        api_response = api_instance.put_telescope_type(body)
        pprint(api_response)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->put_telescope_type: %s\n" % e)
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
<td><a href="TelescopeType.html"><strong>TelescopeType</strong></a></td>
<td>TelescopeType to create or update</td>
<td></td>
</tr>




</tbody>
</table></div>


### Return type

[**TelescopeType**](TelescopeType.html)

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
    <td>TelescopeType updated</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>201</strong></td>
    <td>TelescopeType created, returning the created object with an id</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>401</strong></td>
    <td>API key is missing or invalid</td>
    <td> * WWW_Authenticate -  <br> </td>
</tr>

</tbody>
</table></div>

## **queryv1**
> QueryResponse queryv1(agg, subset)

Search the Observatory API

Search the Observatory API

### Example

* Api Key Authentication (api_key):
```python
import time
import observatory.api.client
from observatory.api.client.api import observatory_api
from observatory.api.client.model.query_response import QueryResponse
from pprint import pprint
# Defining the host is optional and defaults to https://api.observatory.academy
# See configuration.py for a list of all supported configuration parameters.
configuration = observatory.api.client.Configuration(
    host = "https://api.observatory.academy"
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
    agg = "author" # str | The aggregation level
    subset = "citations" # str | The required subset
    index_date = dateutil_parser('1970-01-01').date() # date | Index date, defaults to latest (optional)
    _from = 4 # int | Start year (included) (optional)
    to = 4 # int | End year (not included) (optional)
    limit = 1 # int | Limit number of results (max 10000) (optional)
    scroll_id = "scroll_id_example" # str | The scroll id (optional)
    pit_id = "pit_id_example" # str | The PIT id (optional)
    search_after_no = 1 # int | The search after key (optional)
    search_after_text = "search_after_text_example" # str | The search after key (optional)
    id = [
        "id_example",
    ] # [str] |  (optional)
    name = [
        "name_example",
    ] # [str] |  (optional)
    published_year = [
        "published_year_example",
    ] # [str] |  (optional)
    coordinates = [
        "coordinates_example",
    ] # [str] |  (optional)
    country = [
        "country_example",
    ] # [str] |  (optional)
    country_code = [
        "country_code_example",
    ] # [str] |  (optional)
    region = [
        "region_example",
    ] # [str] |  (optional)
    subregion = [
        "subregion_example",
    ] # [str] |  (optional)
    access_type = [
        "access_type_example",
    ] # [str] |  (optional)
    label = [
        "label_example",
    ] # [str] |  (optional)
    status = [
        "status_example",
    ] # [str] |  (optional)
    collaborator_coordinates = [
        "collaborator_coordinates_example",
    ] # [str] |  (optional)
    collaborator_country = [
        "collaborator_country_example",
    ] # [str] |  (optional)
    collaborator_country_code = [
        "collaborator_country_code_example",
    ] # [str] |  (optional)
    collaborator_id = [
        "collaborator_id_example",
    ] # [str] |  (optional)
    collaborator_name = [
        "collaborator_name_example",
    ] # [str] |  (optional)
    collaborator_region = [
        "collaborator_region_example",
    ] # [str] |  (optional)
    collaborator_subregion = [
        "collaborator_subregion_example",
    ] # [str] |  (optional)
    field = [
        "field_example",
    ] # [str] |  (optional)
    source = [
        "source_example",
    ] # [str] |  (optional)
    funder_country_code = [
        "funder_country_code_example",
    ] # [str] |  (optional)
    funder_name = [
        "funder_name_example",
    ] # [str] |  (optional)
    funder_sub_type = [
        "funder_sub_type_example",
    ] # [str] |  (optional)
    funder_type = [
        "funder_type_example",
    ] # [str] |  (optional)
    journal = [
        "journal_example",
    ] # [str] |  (optional)
    output_type = [
        "output_type_example",
    ] # [str] |  (optional)
    publisher = [
        "publisher_example",
    ] # [str] |  (optional)

    # example passing only required values which don't have defaults set
    try:
        # Search the Observatory API
        api_response = api_instance.queryv1(agg, subset)
        pprint(api_response)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->queryv1: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # Search the Observatory API
        api_response = api_instance.queryv1(agg, subset, index_date=index_date, _from=_from, to=to, limit=limit, scroll_id=scroll_id, pit_id=pit_id, search_after_no=search_after_no, search_after_text=search_after_text, id=id, name=name, published_year=published_year, coordinates=coordinates, country=country, country_code=country_code, region=region, subregion=subregion, access_type=access_type, label=label, status=status, collaborator_coordinates=collaborator_coordinates, collaborator_country=collaborator_country, collaborator_country_code=collaborator_country_code, collaborator_id=collaborator_id, collaborator_name=collaborator_name, collaborator_region=collaborator_region, collaborator_subregion=collaborator_subregion, field=field, source=source, funder_country_code=funder_country_code, funder_name=funder_name, funder_sub_type=funder_sub_type, funder_type=funder_type, journal=journal, output_type=output_type, publisher=publisher)
        pprint(api_response)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->queryv1: %s\n" % e)
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
<td><strong>agg</strong></td>
<td><strong>str</strong></td>
<td>The aggregation level</td>
<td></td>
</tr>

<tr>
<td><strong>subset</strong></td>
<td><strong>str</strong></td>
<td>The required subset</td>
<td></td>
</tr>





<tr>
<td><strong>index_date</strong></td>
<td><strong>date</strong></td>
<td>Index date, defaults to latest</td>
<td>
[optional]
<tr>
<td><strong>_from</strong></td>
<td><strong>int</strong></td>
<td>Start year (included)</td>
<td>
[optional]
<tr>
<td><strong>to</strong></td>
<td><strong>int</strong></td>
<td>End year (not included)</td>
<td>
[optional]
<tr>
<td><strong>limit</strong></td>
<td><strong>int</strong></td>
<td>Limit number of results (max 10000)</td>
<td>
[optional]
<tr>
<td><strong>scroll_id</strong></td>
<td><strong>str</strong></td>
<td>The scroll id</td>
<td>
[optional]
<tr>
<td><strong>pit_id</strong></td>
<td><strong>str</strong></td>
<td>The PIT id</td>
<td>
[optional]
<tr>
<td><strong>search_after_no</strong></td>
<td><strong>int</strong></td>
<td>The search after key</td>
<td>
[optional]
<tr>
<td><strong>search_after_text</strong></td>
<td><strong>str</strong></td>
<td>The search after key</td>
<td>
[optional]
<tr>
<td><strong>id</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>name</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>published_year</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>coordinates</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>country</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>country_code</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>region</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>subregion</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>access_type</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>label</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>status</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>collaborator_coordinates</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>collaborator_country</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>collaborator_country_code</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>collaborator_id</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>collaborator_name</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>collaborator_region</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>collaborator_subregion</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>field</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>source</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>funder_country_code</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>funder_name</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>funder_sub_type</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>funder_type</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>journal</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>output_type</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
<tr>
<td><strong>publisher</strong></td>
<td><strong>[str]</strong></td>
<td></td>
<td>
[optional]
</tbody>
</table></div>


### Return type

[**QueryResponse**](QueryResponse.html)

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
    <td>Successfully return query results</td>
    <td> - </td>
</tr>
<tr>
    <td><strong>401</strong></td>
    <td>API key is missing or invalid</td>
    <td> * WWW_Authenticate -  <br> </td>
</tr>

</tbody>
</table></div>

