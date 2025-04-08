# GraphServiceRegistryRecord


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**graph_id** | **str** |  | [optional] 
**service_registry** | [**ServiceRegistryRecord**](ServiceRegistryRecord.md) |  | [optional] 

## Example

```python
from gs_interactive.models.graph_service_registry_record import GraphServiceRegistryRecord

# TODO update the JSON string below
json = "{}"
# create an instance of GraphServiceRegistryRecord from a JSON string
graph_service_registry_record_instance = GraphServiceRegistryRecord.from_json(json)
# print the JSON string representation of the object
print GraphServiceRegistryRecord.to_json()

# convert the object into a dict
graph_service_registry_record_dict = graph_service_registry_record_instance.to_dict()
# create an instance of GraphServiceRegistryRecord from a dict
graph_service_registry_record_form_dict = graph_service_registry_record.from_dict(graph_service_registry_record_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


