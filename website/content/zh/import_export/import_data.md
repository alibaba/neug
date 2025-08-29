# 导入数据

NeuG 支持从指定文件向数据库中插入数据。向数据库插入数据的前提是，你需要先创建图 schema，即节点和关系表的结构。

通常情况下，你可以使用 `COPY FROM` 命令将数据从文件导入到数据库中。

## 从 CSV 导入

### 导入到 Node Table

创建一个 node table `person`，如下所示：

```cypher
CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));
```

CSV 文件 `person.csv` 包含以下列：

```csv
id|name|age
1|marko|29
2|vadas|27
4|josh|32
6|peter|35
...
```

以下语句将把 `person.csv` 导入到 person table 中：

```cypher
COPY User FROM "user.csv" (header=true);
```

**注意：**

- CSV 文件中的列数必须等于 node 定义的属性数量。
- CSV 文件中的列顺序必须与 node 定义的属性顺序一致。例如，上述 node 的属性顺序为：id、age、name，这与 CSV 文件中的列对应。

### 导入关系表

使用以下查询创建关系表 `knows`：

```cypher
CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);
```

CSV 文件 `person_knows_person.csv` 包含以下列：

```csv
person.id|person.id|weight
1|2|0.5
1|4|1.0
...
```

以下语句将 `person_knows_person.csv` 文件导入到 `knows` 表中：

```cypher
COPY knows from "person_knows_person.csv" (from="person", to="person", header=true);
```

**注意**
- 当导入到关系表时，NeuG 假设文件中的前两列分别是 `FROM` 和 `TO` 节点的主键；其余列对应关系属性。
- CSV 配置中必须包含 `FROM` 和 `TO` 参数，用于指定 `FROM` 和 `TO` 节点的标签。

### CSV 配置参数

支持以下配置参数：

| 参数 | 描述 | 默认值 |
|------|------|--------|
| `HEADER` | CSV 文件的第一行是否为 header。可选值为 true 或 false。 | `false` |
| `DELIM` 或 `DELIMITER` | 用于分隔行内不同列的字符。 | `\|` |
| `QUOTE` | 字符串引号开始字符。 | `"` |
| `ESCAPE` | 在字符串引号中用于转义 QUOTE 和其他字符（例如换行符）的字符。 | `\` |
| `SKIP` | 从输入文件中跳过的行数。 | `0` |
| `PARALLEL` | 是否并行读取 CSV 文件。 | `true` |
| `IGNORE_ERRORS` | 如果设置为 true，则跳过 CSV 文件中的格式错误行。 | `false` |
| `NULL_STRINGS` | 应该在 CSV 文件中被当作 null 处理的字符串。 | `""`（空字符串） |
| `FROM` | 源 vertex label 名称，仅在导入 edge 时使用 | `-` |
| `TO` | 目标 vertex label 名称，仅在导入 edge 时使用 | `-` |

**注意事项**
- 参数应以逗号分隔的键值对形式指定，例如：`param1=value1, param2=value2, ...`
- 参数不区分大小写，即 `Header`、`HEADER` 和 `header` 都是有效的，并且都会被正确识别。
- 对于布尔类型的参数（例如 `header`）：
  1. 使用 `True`、`true` 或 `1` 表示启用。
  2. 使用 `False`、`false` 或 `0` 表示禁用。
  3. 如果值为 `true`，可以省略值部分，例如 `header=true` 可简写为 `header`。
- `FROM` 和 `TO` 是一组特殊的参数。仅在从 CSV 文件导入 edge 时有效。当 edge 的源 vertex 和目标 vertex 存在多个 label 组合时（例如 Comment-replyOf->Post、Comment-replyOf->Comment），必须指定 `FROM` 和 `TO`。在其他场景中使用这些参数会抛出异常。