# 导出数据
`COPY TO` 命令允许将查询结果直接导出到指定的文件格式。这对于将查询结果持久化以便在其他系统中使用，或保存查询输出以供归档非常有用。

## 导出到 CSV

COPY TO 子句可以将查询结果导出到 CSV 文件，使用方式如下：

```cypher
COPY (MATCH (p:person) RETURN p.*) TO 'person.csv' (header=true);
```

CSV 文件包含以下字段：

```csv
p.id|p.name|p.age
1|marko|29
2|vadas|27
4|josh|32
6|peter|35
```

复杂类型（如顶点和边）会以 JSON 格式的字符串形式输出。

可用参数如下：

| 参数 | 描述 | 默认值 |
| --- | --- | --- |
| `HEADER` | 是否输出表头行。| `false` |
| `DELIM` 或 `DELIMITER` | CSV 中分隔字段的字符。| `\|` |

另一个示例如下：

```cypher
COPY (MATCH (:person)-[e:knows]->(:person) RETURN e) TO 'person_knows_person.csv' (header=true);
```

这会将以下结果输出到 `person_knows_person.csv`：

```csv
e
{'_SRC': '0:0', '_DST': '0:1', '_LABEL': 'knows', 'weight': 0.500000}
{'_SRC': '0:0', '_DST': '0:2', '_LABEL': 'knows', 'weight': 1.000000}
```