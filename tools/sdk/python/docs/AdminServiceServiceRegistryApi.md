# gs_interactive.AdminServiceServiceRegistryApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_service_registry_info**](AdminServiceServiceRegistryApi.md#get_service_registry_info) | **GET** /v1/service/registry/{graph_id}/{service_name} | 
[**list_service_registry_info**](AdminServiceServiceRegistryApi.md#list_service_registry_info) | **GET** /v1/service/registry | 


# **get_service_registry_info**
> GraphServiceRegistryRecord get_service_registry_info(graph_id, service_name)



Get a service registry by graph_id and service_name

### Example


```python
import time
import os
import gs_interactive
from gs_interactive.models.graph_service_registry_record import GraphServiceRegistryRecord
from gs_interactive.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = gs_interactive.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with gs_interactive.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = gs_interactive.AdminServiceServiceRegistryApi(api_client)
    graph_id = 'graph_id_example' # str | 
    service_name = 'service_name_example' # str | 

    try:
        api_response = api_instance.get_service_registry_info(graph_id, service_name)
        print("The response of AdminServiceServiceRegistryApi->get_service_registry_info:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceServiceRegistryApi->get_service_registry_info: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **service_name** | **str**|  | 

### Return type

[**GraphServiceRegistryRecord**](GraphServiceRegistryRecord.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**404** | Not found |  -  |
**500** | Internal error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_service_registry_info**
> List[GraphServiceRegistryRecord] list_service_registry_info()



List all services registry

### Example


```python
import time
import os
import gs_interactive
from gs_interactive.models.graph_service_registry_record import GraphServiceRegistryRecord
from gs_interactive.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = gs_interactive.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with gs_interactive.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = gs_interactive.AdminServiceServiceRegistryApi(api_client)

    try:
        api_response = api_instance.list_service_registry_info()
        print("The response of AdminServiceServiceRegistryApi->list_service_registry_info:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceServiceRegistryApi->list_service_registry_info: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**List[GraphServiceRegistryRecord]**](GraphServiceRegistryRecord.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**500** | Internal error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

