# gs_interactive.GraphServiceVertexManagementApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**add_vertex**](GraphServiceVertexManagementApi.md#add_vertex) | **POST** /v1/graph/{graph_id}/vertex | Add vertex (and edge) to the graph
[**delete_vertex**](GraphServiceVertexManagementApi.md#delete_vertex) | **DELETE** /v1/graph/{graph_id}/vertex | Remove vertex from the graph
[**get_vertex**](GraphServiceVertexManagementApi.md#get_vertex) | **GET** /v1/graph/{graph_id}/vertex | Get the vertex&#39;s properties with vertex primary key.
[**update_vertex**](GraphServiceVertexManagementApi.md#update_vertex) | **PUT** /v1/graph/{graph_id}/vertex | Update vertex&#39;s property


# **add_vertex**
> str add_vertex(graph_id, vertex_edge_request)

Add vertex (and edge) to the graph

Add the provided vertex (and edge) to the specified graph. 

### Example


```python
import time
import os
import gs_interactive
from gs_interactive.models.vertex_edge_request import VertexEdgeRequest
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
    api_instance = gs_interactive.GraphServiceVertexManagementApi(api_client)
    graph_id = 'graph_id_example' # str | 
    vertex_edge_request = {"vertex_request":[{"label":"person","primary_key_value":2,"properties":{"age":24,"name":"Cindy"}}],"edge_request":[{"src_label":"person","dst_label":"software","edge_label":"created","src_primary_key_values":[{"name":"id","value":1}],"dst_primary_key_values":[{"name":"id","value":3}],"properties":[{"name":"weight","value":0.2}]}]} # VertexEdgeRequest | 

    try:
        # Add vertex (and edge) to the graph
        api_response = api_instance.add_vertex(graph_id, vertex_edge_request)
        print("The response of GraphServiceVertexManagementApi->add_vertex:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphServiceVertexManagementApi->add_vertex: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **vertex_edge_request** | [**VertexEdgeRequest**](VertexEdgeRequest.md)|  | 

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
**200** | Successfully created vertex |  -  |
**400** | Invalid input vertex |  -  |
**404** | Graph not found |  -  |
**409** | Vertex already exists |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_vertex**
> str delete_vertex(graph_id, delete_vertex_request)

Remove vertex from the graph

Remove the vertex from the specified graph. 

### Example


```python
import time
import os
import gs_interactive
from gs_interactive.models.delete_vertex_request import DeleteVertexRequest
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
    api_instance = gs_interactive.GraphServiceVertexManagementApi(api_client)
    graph_id = 'graph_id_example' # str | 
    delete_vertex_request = [gs_interactive.DeleteVertexRequest()] # List[DeleteVertexRequest] | The label and primary key values of the vertex to be deleted.

    try:
        # Remove vertex from the graph
        api_response = api_instance.delete_vertex(graph_id, delete_vertex_request)
        print("The response of GraphServiceVertexManagementApi->delete_vertex:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphServiceVertexManagementApi->delete_vertex: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **delete_vertex_request** | [**List[DeleteVertexRequest]**](DeleteVertexRequest.md)| The label and primary key values of the vertex to be deleted. | 

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
**200** | Successfully delete vertex |  -  |
**400** | Invalid input vertex |  -  |
**404** | Vertex not exists or Graph not exits. |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_vertex**
> VertexData get_vertex(graph_id, label, primary_key_value)

Get the vertex's properties with vertex primary key.

Get the properties for the specified vertex. example: ```http GET /endpoint?param1=value1&param2=value2 HTTP/1.1 Host: example.com ``` 

### Example


```python
import time
import os
import gs_interactive
from gs_interactive.models.vertex_data import VertexData
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
    api_instance = gs_interactive.GraphServiceVertexManagementApi(api_client)
    graph_id = 'graph_id_example' # str | The id of the graph
    label = 'label_example' # str | The label name of querying vertex.
    primary_key_value = None # object | The primary key value of querying vertex.

    try:
        # Get the vertex's properties with vertex primary key.
        api_response = api_instance.get_vertex(graph_id, label, primary_key_value)
        print("The response of GraphServiceVertexManagementApi->get_vertex:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphServiceVertexManagementApi->get_vertex: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**| The id of the graph | 
 **label** | **str**| The label name of querying vertex. | 
 **primary_key_value** | [**object**](.md)| The primary key value of querying vertex. | 

### Return type

[**VertexData**](VertexData.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Found vertex |  -  |
**400** | Bad input parameter |  -  |
**404** | Vertex not found or graph not found |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_vertex**
> str update_vertex(graph_id, vertex_edge_request)

Update vertex's property

Update the vertex with the provided properties to the specified graph. 

### Example


```python
import time
import os
import gs_interactive
from gs_interactive.models.vertex_edge_request import VertexEdgeRequest
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
    api_instance = gs_interactive.GraphServiceVertexManagementApi(api_client)
    graph_id = 'graph_id_example' # str | 
    vertex_edge_request = {"label":"person","primary_key_value":2,"properties":{"age":24,"name":"Cindy"}} # VertexEdgeRequest | 

    try:
        # Update vertex's property
        api_response = api_instance.update_vertex(graph_id, vertex_edge_request)
        print("The response of GraphServiceVertexManagementApi->update_vertex:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphServiceVertexManagementApi->update_vertex: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **vertex_edge_request** | [**VertexEdgeRequest**](VertexEdgeRequest.md)|  | 

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
**200** | Successfully update vertex |  -  |
**400** | Invalid input parameters |  -  |
**404** | Vertex not exists |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

