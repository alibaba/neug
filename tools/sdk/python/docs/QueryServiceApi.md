# gs_interactive.QueryServiceApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**call_proc**](QueryServiceApi.md#call_proc) | **POST** /v1/graph/{graph_id}/query | run queries on graph
[**call_proc_current**](QueryServiceApi.md#call_proc_current) | **POST** /v1/graph/current/query | run queries on the running graph
[**run_adhoc**](QueryServiceApi.md#run_adhoc) | **POST** /v1/graph/{graph_id}/adhoc_query | Submit adhoc query to the Interactive Query Service.
[**run_adhoc_current**](QueryServiceApi.md#run_adhoc_current) | **POST** /v1/graph/current/adhoc_query | Submit adhoc query to the Interactive Query Service.


# **call_proc**
> bytearray call_proc(graph_id, body=body)

run queries on graph

After the procedure is created, user can use this API to run the procedure. 

### Example


```python
import time
import os
import gs_interactive
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
    api_instance = gs_interactive.QueryServiceApi(api_client)
    graph_id = 'graph_id_example' # str | 
    body = None # bytearray |  (optional)

    try:
        # run queries on graph
        api_response = api_instance.call_proc(graph_id, body=body)
        print("The response of QueryServiceApi->call_proc:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling QueryServiceApi->call_proc: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **body** | **bytearray**|  | [optional] 

### Return type

**bytearray**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: text/plain
 - **Accept**: text/plain, application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully runned. |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **call_proc_current**
> bytearray call_proc_current(body=body)

run queries on the running graph

Submit a query to the running graph. 

### Example


```python
import time
import os
import gs_interactive
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
    api_instance = gs_interactive.QueryServiceApi(api_client)
    body = None # bytearray |  (optional)

    try:
        # run queries on the running graph
        api_response = api_instance.call_proc_current(body=body)
        print("The response of QueryServiceApi->call_proc_current:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling QueryServiceApi->call_proc_current: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | **bytearray**|  | [optional] 

### Return type

**bytearray**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: text/plain
 - **Accept**: text/plain, application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully runned. Empty if failed? |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **run_adhoc**
> bytearray run_adhoc(graph_id, body=body)

Submit adhoc query to the Interactive Query Service.

Submit a adhoc query to the running graph. The adhoc query should be represented by the physical plan: https://github.com/alibaba/GraphScope/blob/main/interactive_engine/executor/ir/proto/physical.proto 

### Example


```python
import time
import os
import gs_interactive
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
    api_instance = gs_interactive.QueryServiceApi(api_client)
    graph_id = 'graph_id_example' # str | 
    body = None # bytearray |  (optional)

    try:
        # Submit adhoc query to the Interactive Query Service.
        api_response = api_instance.run_adhoc(graph_id, body=body)
        print("The response of QueryServiceApi->run_adhoc:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling QueryServiceApi->run_adhoc: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **body** | **bytearray**|  | [optional] 

### Return type

**bytearray**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: text/plain
 - **Accept**: text/plain, application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully runned. |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **run_adhoc_current**
> bytearray run_adhoc_current(body=body)

Submit adhoc query to the Interactive Query Service.

Submit a adhoc query to the running graph. The adhoc query should be represented by the physical plan: https://github.com/alibaba/GraphScope/blob/main/interactive_engine/executor/ir/proto/physical.proto 

### Example


```python
import time
import os
import gs_interactive
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
    api_instance = gs_interactive.QueryServiceApi(api_client)
    body = None # bytearray |  (optional)

    try:
        # Submit adhoc query to the Interactive Query Service.
        api_response = api_instance.run_adhoc_current(body=body)
        print("The response of QueryServiceApi->run_adhoc_current:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling QueryServiceApi->run_adhoc_current: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | **bytearray**|  | [optional] 

### Return type

**bytearray**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: text/plain
 - **Accept**: text/plain, application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully runned. Empty if failed? |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

