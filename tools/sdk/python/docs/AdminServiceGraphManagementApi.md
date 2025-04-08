# gs_interactive.AdminServiceGraphManagementApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_dataloading_job**](AdminServiceGraphManagementApi.md#create_dataloading_job) | **POST** /v1/graph/{graph_id}/dataloading | 
[**create_edge_type**](AdminServiceGraphManagementApi.md#create_edge_type) | **POST** /v1/graph/{graph_id}/schema/edge | 
[**create_graph**](AdminServiceGraphManagementApi.md#create_graph) | **POST** /v1/graph | 
[**create_vertex_type**](AdminServiceGraphManagementApi.md#create_vertex_type) | **POST** /v1/graph/{graph_id}/schema/vertex | 
[**delete_edge_type**](AdminServiceGraphManagementApi.md#delete_edge_type) | **DELETE** /v1/graph/{graph_id}/schema/edge | 
[**delete_graph**](AdminServiceGraphManagementApi.md#delete_graph) | **DELETE** /v1/graph/{graph_id} | 
[**delete_vertex_type**](AdminServiceGraphManagementApi.md#delete_vertex_type) | **DELETE** /v1/graph/{graph_id}/schema/vertex | 
[**get_graph**](AdminServiceGraphManagementApi.md#get_graph) | **GET** /v1/graph/{graph_id} | 
[**get_graph_statistic**](AdminServiceGraphManagementApi.md#get_graph_statistic) | **GET** /v1/graph/{graph_id}/statistics | 
[**get_schema**](AdminServiceGraphManagementApi.md#get_schema) | **GET** /v1/graph/{graph_id}/schema | 
[**get_snapshot_status**](AdminServiceGraphManagementApi.md#get_snapshot_status) | **GET** /v1/graph/{graph_id}/snapshot/{snapshot_id}/status | 
[**list_graphs**](AdminServiceGraphManagementApi.md#list_graphs) | **GET** /v1/graph | 
[**update_edge_type**](AdminServiceGraphManagementApi.md#update_edge_type) | **PUT** /v1/graph/{graph_id}/schema/edge | 
[**update_vertex_type**](AdminServiceGraphManagementApi.md#update_vertex_type) | **PUT** /v1/graph/{graph_id}/schema/vertex | 


# **create_dataloading_job**
> JobResponse create_dataloading_job(graph_id, schema_mapping)



Create a dataloading job

### Example


```python
import time
import os
import gs_interactive
from gs_interactive.models.job_response import JobResponse
from gs_interactive.models.schema_mapping import SchemaMapping
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
    api_instance = gs_interactive.AdminServiceGraphManagementApi(api_client)
    graph_id = 'graph_id_example' # str | The id of graph to do bulk loading.
    schema_mapping = gs_interactive.SchemaMapping() # SchemaMapping | 

    try:
        api_response = api_instance.create_dataloading_job(graph_id, schema_mapping)
        print("The response of AdminServiceGraphManagementApi->create_dataloading_job:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->create_dataloading_job: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**| The id of graph to do bulk loading. | 
 **schema_mapping** | [**SchemaMapping**](SchemaMapping.md)|  | 

### Return type

[**JobResponse**](JobResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_edge_type**
> str create_edge_type(graph_id, create_edge_type=create_edge_type)



Create a edge type

### Example


```python
import time
import os
import gs_interactive
from gs_interactive.models.create_edge_type import CreateEdgeType
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
    api_instance = gs_interactive.AdminServiceGraphManagementApi(api_client)
    graph_id = 'graph_id_example' # str | 
    create_edge_type = gs_interactive.CreateEdgeType() # CreateEdgeType |  (optional)

    try:
        api_response = api_instance.create_edge_type(graph_id, create_edge_type=create_edge_type)
        print("The response of AdminServiceGraphManagementApi->create_edge_type:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->create_edge_type: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **create_edge_type** | [**CreateEdgeType**](CreateEdgeType.md)|  | [optional] 

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful created the edge type |  -  |
**400** | Bad request |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_graph**
> CreateGraphResponse create_graph(create_graph_request)



Create a new graph

### Example


```python
import time
import os
import gs_interactive
from gs_interactive.models.create_graph_request import CreateGraphRequest
from gs_interactive.models.create_graph_response import CreateGraphResponse
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
    api_instance = gs_interactive.AdminServiceGraphManagementApi(api_client)
    create_graph_request = gs_interactive.CreateGraphRequest() # CreateGraphRequest | 

    try:
        api_response = api_instance.create_graph(create_graph_request)
        print("The response of AdminServiceGraphManagementApi->create_graph:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->create_graph: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **create_graph_request** | [**CreateGraphRequest**](CreateGraphRequest.md)|  | 

### Return type

[**CreateGraphResponse**](CreateGraphResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**400** | BadRequest |  -  |
**500** | Internal error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_vertex_type**
> str create_vertex_type(graph_id, create_vertex_type)



Create a vertex type

### Example


```python
import time
import os
import gs_interactive
from gs_interactive.models.create_vertex_type import CreateVertexType
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
    api_instance = gs_interactive.AdminServiceGraphManagementApi(api_client)
    graph_id = 'graph_id_example' # str | 
    create_vertex_type = gs_interactive.CreateVertexType() # CreateVertexType | 

    try:
        api_response = api_instance.create_vertex_type(graph_id, create_vertex_type)
        print("The response of AdminServiceGraphManagementApi->create_vertex_type:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->create_vertex_type: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **create_vertex_type** | [**CreateVertexType**](CreateVertexType.md)|  | 

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully created the vertex type |  -  |
**400** | Bad request |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_edge_type**
> str delete_edge_type(graph_id, type_name, source_vertex_type, destination_vertex_type)



Delete an edge type by name

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
    api_instance = gs_interactive.AdminServiceGraphManagementApi(api_client)
    graph_id = 'graph_id_example' # str | 
    type_name = 'type_name_example' # str | 
    source_vertex_type = 'source_vertex_type_example' # str | 
    destination_vertex_type = 'destination_vertex_type_example' # str | 

    try:
        api_response = api_instance.delete_edge_type(graph_id, type_name, source_vertex_type, destination_vertex_type)
        print("The response of AdminServiceGraphManagementApi->delete_edge_type:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->delete_edge_type: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **type_name** | **str**|  | 
 **source_vertex_type** | **str**|  | 
 **destination_vertex_type** | **str**|  | 

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully deleted the edge type |  -  |
**400** | Bad request |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_graph**
> str delete_graph(graph_id)



Delete a graph by id

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
    api_instance = gs_interactive.AdminServiceGraphManagementApi(api_client)
    graph_id = 'graph_id_example' # str | The id of graph to delete

    try:
        api_response = api_instance.delete_graph(graph_id)
        print("The response of AdminServiceGraphManagementApi->delete_graph:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->delete_graph: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**| The id of graph to delete | 

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful operation |  -  |
**404** | Not Found |  -  |
**500** | Internal Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_vertex_type**
> str delete_vertex_type(graph_id, type_name)



Delete a vertex type by name

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
    api_instance = gs_interactive.AdminServiceGraphManagementApi(api_client)
    graph_id = 'graph_id_example' # str | 
    type_name = 'type_name_example' # str | 

    try:
        api_response = api_instance.delete_vertex_type(graph_id, type_name)
        print("The response of AdminServiceGraphManagementApi->delete_vertex_type:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->delete_vertex_type: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **type_name** | **str**|  | 

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully deleted the vertex type |  -  |
**400** | Bad request |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_graph**
> GetGraphResponse get_graph(graph_id)



Get a graph by name

### Example


```python
import time
import os
import gs_interactive
from gs_interactive.models.get_graph_response import GetGraphResponse
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
    api_instance = gs_interactive.AdminServiceGraphManagementApi(api_client)
    graph_id = 'graph_id_example' # str | The id of graph to get

    try:
        api_response = api_instance.get_graph(graph_id)
        print("The response of AdminServiceGraphManagementApi->get_graph:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->get_graph: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**| The id of graph to get | 

### Return type

[**GetGraphResponse**](GetGraphResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful operation |  -  |
**404** | Not found |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_graph_statistic**
> GetGraphStatisticsResponse get_graph_statistic(graph_id)



Get the statics info of a graph, including number of vertices for each label, number of edges for each label.

### Example


```python
import time
import os
import gs_interactive
from gs_interactive.models.get_graph_statistics_response import GetGraphStatisticsResponse
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
    api_instance = gs_interactive.AdminServiceGraphManagementApi(api_client)
    graph_id = 'graph_id_example' # str | The id of graph to get statistics

    try:
        api_response = api_instance.get_graph_statistic(graph_id)
        print("The response of AdminServiceGraphManagementApi->get_graph_statistic:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->get_graph_statistic: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**| The id of graph to get statistics | 

### Return type

[**GetGraphStatisticsResponse**](GetGraphStatisticsResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**500** | Server Internal Error |  -  |
**404** | Not Found |  -  |
**503** | Service Unavailable |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_schema**
> GetGraphSchemaResponse get_schema(graph_id)



Get schema by graph id

### Example


```python
import time
import os
import gs_interactive
from gs_interactive.models.get_graph_schema_response import GetGraphSchemaResponse
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
    api_instance = gs_interactive.AdminServiceGraphManagementApi(api_client)
    graph_id = 'graph_id_example' # str | The id of graph to get schema

    try:
        api_response = api_instance.get_schema(graph_id)
        print("The response of AdminServiceGraphManagementApi->get_schema:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->get_schema: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**| The id of graph to get schema | 

### Return type

[**GetGraphSchemaResponse**](GetGraphSchemaResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_snapshot_status**
> SnapshotStatus get_snapshot_status(graph_id, snapshot_id)



Get the status of a snapshot by id

### Example


```python
import time
import os
import gs_interactive
from gs_interactive.models.snapshot_status import SnapshotStatus
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
    api_instance = gs_interactive.AdminServiceGraphManagementApi(api_client)
    graph_id = 'graph_id_example' # str | 
    snapshot_id = 56 # int | 

    try:
        api_response = api_instance.get_snapshot_status(graph_id, snapshot_id)
        print("The response of AdminServiceGraphManagementApi->get_snapshot_status:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->get_snapshot_status: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **snapshot_id** | **int**|  | 

### Return type

[**SnapshotStatus**](SnapshotStatus.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | the status of a snapshot_id |  -  |
**400** | Bad request |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_graphs**
> List[GetGraphResponse] list_graphs()



List all graphs

### Example


```python
import time
import os
import gs_interactive
from gs_interactive.models.get_graph_response import GetGraphResponse
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
    api_instance = gs_interactive.AdminServiceGraphManagementApi(api_client)

    try:
        api_response = api_instance.list_graphs()
        print("The response of AdminServiceGraphManagementApi->list_graphs:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->list_graphs: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**List[GetGraphResponse]**](GetGraphResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful operation |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_edge_type**
> str update_edge_type(graph_id, create_edge_type)



Update an edge type to add more properties

### Example


```python
import time
import os
import gs_interactive
from gs_interactive.models.create_edge_type import CreateEdgeType
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
    api_instance = gs_interactive.AdminServiceGraphManagementApi(api_client)
    graph_id = 'graph_id_example' # str | 
    create_edge_type = gs_interactive.CreateEdgeType() # CreateEdgeType | 

    try:
        api_response = api_instance.update_edge_type(graph_id, create_edge_type)
        print("The response of AdminServiceGraphManagementApi->update_edge_type:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->update_edge_type: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **create_edge_type** | [**CreateEdgeType**](CreateEdgeType.md)|  | 

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully updated the edge type |  -  |
**400** | Bad request |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_vertex_type**
> str update_vertex_type(graph_id, create_vertex_type)



Update a vertex type to add more properties

### Example


```python
import time
import os
import gs_interactive
from gs_interactive.models.create_vertex_type import CreateVertexType
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
    api_instance = gs_interactive.AdminServiceGraphManagementApi(api_client)
    graph_id = 'graph_id_example' # str | 
    create_vertex_type = gs_interactive.CreateVertexType() # CreateVertexType | 

    try:
        api_response = api_instance.update_vertex_type(graph_id, create_vertex_type)
        print("The response of AdminServiceGraphManagementApi->update_vertex_type:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->update_vertex_type: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **create_vertex_type** | [**CreateVertexType**](CreateVertexType.md)|  | 

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully updated the vertex type |  -  |
**400** | Bad request |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

