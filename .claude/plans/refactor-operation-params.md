# Refactor: Remove redundant DataType from operation_params properties tuple

## Context

In `operation_params.h`, four DDL param classes store properties as `std::vector<std::tuple<DataType, std::string, execution::Value>>`. The `DataType` is redundant because `execution::Value` already stores its type internally (accessible via `value.type()` — see `value.h:56`). All callers that build these tuples derive DataType from `prop_value.type()` anyway. The Schema classes should (and already do) retain their own `DataType` storage — this refactoring only affects the param transport layer.

## Change Summary

Change `std::tuple<DataType, std::string, execution::Value>` → `std::pair<std::string, execution::Value>` in all 4 param classes and update all callers.

## Files to Modify

### 1. `include/neug/storages/graph/operation_params.h`

For each of the 4 param classes (`CreateVertexTypeParam`, `CreateEdgeTypeParam`, `AddVertexPropertiesParam`, `AddEdgePropertiesParam`):
- Change field: `std::vector<std::tuple<DataType, std::string, execution::Value>> properties` → `std::vector<std::pair<std::string, execution::Value>> properties`
- Update `GetProperties()` return type to match

For each of the 4 builder classes:
- `Properties(...)` setter: update parameter type to `std::vector<std::pair<std::string, execution::Value>>`
- `AddProperty(const DataType& type, const std::string& name, const execution::Value& value)` → `AddProperty(const std::string& name, const execution::Value& value)`, body: `config.properties.emplace_back(name, value)`

### 2. `src/storages/graph/operation_params.cc` (serialization — keep wire format unchanged)

**Serialize** (4 methods): change structured binding from `[type, name, default_value]` to `[name, default_value]`, write `default_value.type()` instead of `type`:
```cpp
arc << default_value.type() << name << execution::value_to_property(default_value);
```

**Deserialize** (4 methods): still read DataType from archive (backward compatible), then call new `AddProperty(name, value)`:
```cpp
DataType type;
std::string name;
Property default_value;
arc >> type >> name >> default_value;
builder.AddProperty(name, execution::property_to_value(default_value, type));
```

### 3. `src/storages/graph/property_graph.cc` (4 call sites)

Change structured binding from `auto [type, name, default_value] = properties[i]` to `auto& [name, default_value] = properties[i]`, derive type from value:
```cpp
property_types.emplace_back(default_value.type());
```

Affected methods: `CreateVertexType` (~line 174), `CreateEdgeType` (~line 288), `AddVertexProperties` (~line 335), `AddEdgeProperties` (~line 372).

### 4. `src/transaction/update_transaction.cc` (2 call sites)

Change `std::get<1>(prop)` → `prop.first` in `AddVertexProperties` (~lines 280-288) and `AddEdgeProperties` (~lines 323-333).

### 5. DDL execution operators (4 files)

All change `builder.AddProperty(prop_value.type(), prop_name, prop_value)` → `builder.AddProperty(prop_name, prop_value)`:
- `src/execution/execute/ops/ddl/create_vertex_type.cc` (line 46)
- `src/execution/execute/ops/ddl/create_edge_type.cc` (line 51)
- `src/execution/execute/ops/ddl/add_vertex_property.cc` (line 43)
- `src/execution/execute/ops/ddl/add_edge_property.cc` (line 47)

### 6. `tests/storage/test_property_graph.cc`

Change `.AddProperty(DataTypeId::kInt64, "id", value)` → `.AddProperty("id", value)` for all 7 call sites (~lines 52-86).

## What stays unchanged

- `include/neug/storages/graph/schema.h` — Schema retains `std::vector<DataType> property_types` / `properties`
- `src/storages/graph/schema.cc` — Schema methods still receive separate `DataType` vectors from `property_graph.cc`
- Serialization wire format — DataType is still serialized/deserialized for WAL compatibility

## Verification

```bash
cd tools/python_bind && export BUILD_TYPE=DEBUG && make build
```
