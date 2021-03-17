# observatory.api.client.ObservatoryApi

All URIs are relative to *https://api.observatory.academy*

Method | HTTP request | Description
------------- | ------------- | -------------
[**delete_organisation**](ObservatoryApi.md#delete_organisation) | **DELETE** /v1/organisation | delete an Organisation
[**delete_telescope**](ObservatoryApi.md#delete_telescope) | **DELETE** /v1/telescope | delete a Telescope
[**delete_telescope_type**](ObservatoryApi.md#delete_telescope_type) | **DELETE** /v1/telescope_type | delete a TelescopeType
[**get_organisation**](ObservatoryApi.md#get_organisation) | **GET** /v1/organisation | get an Organisation
[**get_organisations**](ObservatoryApi.md#get_organisations) | **GET** /v1/organisations | Get a list of Organisations
[**get_telescope**](ObservatoryApi.md#get_telescope) | **GET** /v1/telescope | get a Telescope
[**get_telescope_type**](ObservatoryApi.md#get_telescope_type) | **GET** /v1/telescope_type | get a TelescopeType
[**get_telescope_types**](ObservatoryApi.md#get_telescope_types) | **GET** /v1/telescope_types | Get a list of TelescopeType objects
[**get_telescopes**](ObservatoryApi.md#get_telescopes) | **GET** /v1/telescopes | Get a list of Telescope objects
[**post_organisation**](ObservatoryApi.md#post_organisation) | **POST** /v1/organisation | create an Organisation
[**post_telescope**](ObservatoryApi.md#post_telescope) | **POST** /v1/telescope | create a Telescope
[**post_telescope_type**](ObservatoryApi.md#post_telescope_type) | **POST** /v1/telescope_type | create a TelescopeType
[**put_organisation**](ObservatoryApi.md#put_organisation) | **PUT** /v1/organisation | create or update an Organisation
[**put_telescope**](ObservatoryApi.md#put_telescope) | **PUT** /v1/telescope | create or update a Telescope
[**put_telescope_type**](ObservatoryApi.md#put_telescope_type) | **PUT** /v1/telescope_type | create or update a TelescopeType


# **delete_organisation**
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

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **int**| Organisation id |

### Return type

void (empty response body)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | Organisation deleted |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_telescope**
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

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **int**| Telescope id |

### Return type

void (empty response body)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | Telescope deleted |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_telescope_type**
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

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **int**| TelescopeType id |

### Return type

void (empty response body)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | TelescopeType deleted |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_organisation**
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

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **int**| Organisation id |

### Return type

[**Organisation**](Organisation.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | the fetched Organisation |  -  |
**400** | bad input parameter |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_organisations**
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

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **limit** | **int**| the maximum number of results to return |

### Return type

[**[Organisation]**](Organisation.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | a list of Organisation objects |  -  |
**400** | bad input parameter |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_telescope**
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

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **int**| Telescope id |

### Return type

[**Telescope**](Telescope.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | the fetched Telescope |  -  |
**400** | bad input parameter |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_telescope_type**
> TelescopeType get_telescope_type(id)

get a TelescopeType

Get the details of a TelescopeType by passing it's id. 

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
    id = 1 # int | TelescopeType id

    # example passing only required values which don't have defaults set
    try:
        # get a TelescopeType
        api_response = api_instance.get_telescope_type(id)
        pprint(api_response)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->get_telescope_type: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **int**| TelescopeType id |

### Return type

[**TelescopeType**](TelescopeType.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | the fetched TelescopeType |  -  |
**400** | bad input parameter |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_telescope_types**
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

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **limit** | **int**| the maximum number of results to return |

### Return type

[**[TelescopeType]**](TelescopeType.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | a list of TelescopeType objects |  -  |
**400** | bad input parameter |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_telescopes**
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

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **limit** | **int**| the maximum number of results to return |
 **telescope_type_id** | **int**| filter telescopes by a TelescopeType id | [optional]
 **organisation_id** | **int**| filter telescopes by an Organisation id | [optional]

### Return type

[**[Telescope]**](Telescope.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | a list of Telescope objects |  -  |
**400** | bad input parameter |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **post_organisation**
> post_organisation(body)

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
                organisation=Organisation(Organisation),
                telescope_type=TelescopeType(
                    id=1,
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
        api_instance.post_organisation(body)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->post_organisation: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | [**Organisation**](Organisation.md)| Organisation to create |

### Return type

void (empty response body)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | Organisation created |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **post_telescope**
> post_telescope(body)

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
        api_instance.post_telescope(body)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->post_telescope: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | [**Telescope**](Telescope.md)| Telescope to create |

### Return type

void (empty response body)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | Telescope created, returning the created object&#39;s id |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **post_telescope_type**
> post_telescope_type(body)

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
        name="Scopus",
        created=dateutil_parser('1970-01-01T00:00:00.00Z'),
        modified=dateutil_parser('1970-01-01T00:00:00.00Z'),
    ) # TelescopeType | TelescopeType to create

    # example passing only required values which don't have defaults set
    try:
        # create a TelescopeType
        api_instance.post_telescope_type(body)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->post_telescope_type: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | [**TelescopeType**](TelescopeType.md)| TelescopeType to create |

### Return type

void (empty response body)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | TelescopeType created, returning the created object&#39;s id |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **put_organisation**
> put_organisation(body)

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
                organisation=Organisation(Organisation),
                telescope_type=TelescopeType(
                    id=1,
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
        api_instance.put_organisation(body)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->put_organisation: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | [**Organisation**](Organisation.md)| Organisation to create or update |

### Return type

void (empty response body)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Organisation updated |  -  |
**201** | Organisation created |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **put_telescope**
> put_telescope(body)

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
        api_instance.put_telescope(body)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->put_telescope: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | [**Telescope**](Telescope.md)| Telescope to create or update |

### Return type

void (empty response body)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Telescope updated |  -  |
**201** | Telescope created, returning the created object&#39;s id |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **put_telescope_type**
> put_telescope_type(body)

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
        name="Scopus",
        created=dateutil_parser('1970-01-01T00:00:00.00Z'),
        modified=dateutil_parser('1970-01-01T00:00:00.00Z'),
    ) # TelescopeType | TelescopeType to create or update

    # example passing only required values which don't have defaults set
    try:
        # create or update a TelescopeType
        api_instance.put_telescope_type(body)
    except observatory.api.client.ApiException as e:
        print("Exception when calling ObservatoryApi->put_telescope_type: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | [**TelescopeType**](TelescopeType.md)| TelescopeType to create or update |

### Return type

void (empty response body)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | TelescopeType updated |  -  |
**201** | TelescopeType created, returning the created object&#39;s id |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

