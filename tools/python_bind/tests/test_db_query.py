import os
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))
from errors import ERR_COMPILATION
from errors import ERR_INVALID_SCHEMA
from errors import ERR_SCHEMA_MISMATCH
from errors import ERR_TYPE_CONVERSION
from errors import ERROR_STRINGS

from nexg.database import Database


# DB-003-01 类型支持-创建schema
def test_create_schema_basic_types(tmp_path):
    db_dir = tmp_path / "schema_basic_types"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(p1 INT32, p2 INT64, p3 UINT32, p4 UINT64, "
        "p5 FLOAT, p6 DOUBLE, p7 STRING, PRIMARY KEY (p1));"
    )
    conn.close()
    db.close()


@pytest.mark.skip(reason="not all types supported yet")
def test_create_schema_all_types(tmp_path):
    db_dir = tmp_path / "schema_types"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE Type (p1 INT32, p2 INT64, p3 UINT32, p4 UINT64, "
        "p5 FLOAT, p6 DOUBLE, p7 STRING, p8 Date, p9 DateTime, p10 Interval, "
        "p11 List, p12 Map, PRIMARY KEY (p1));"
    )
    # 验证schema, TODO: support "SHOW TABLES"?
    result = conn.execute("SHOW TABLES;")
    assert len(result) == 1
    conn.close()
    db.close()


# DB-003-02 类型支持-插入数据
@pytest.mark.skip(reason="DML is not fully supported yet")
def test_insert_basic_type_check(tmp_path):
    db_dir = tmp_path / "insert_basic_type"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(id INT32, i64 INT64, u32 UINT32, u64 UINT64, "
        "f FLOAT, d DOUBLE, s STRING, PRIMARY KEY (id));"
    )
    # data insert
    conn.execute(
        "CREATE (t:person {id: 1, i64: 1234567890123, u32: 123, u64: 456, f: 1.23, d: 4.56, "
        "s: 'abc'});"
    )
    # INT32非法
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:person {id: 'abc'})")
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    # INT64非法
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:person {id: 2, i64: 'bad'})")
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    # UNSIGNED非法
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:person {id: 3, u32: -1})")
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    # FLOAT非法
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:person {id: 4, f: 'bad'})")
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    conn.close()
    db.close()


@pytest.mark.skip(reason="not all types supported yet")
def test_insert_type_check(tmp_path):
    db_dir = tmp_path / "insert_type"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    # 创建包含所有类型的表
    conn.execute(
        "CREATE NODE TABLE T("
        "id INT32, i64 INT64, u32 UINT32, u64 UINT64, "
        "f FLOAT, d DOUBLE, s STRING, dt Date, dttm DateTime, ivl Interval, "
        "l List, m Map, PRIMARY KEY(id));"
    )
    # 合法插入（所有字段）
    conn.execute(
        "CREATE (t:T {id: 1, i64: 1234567890123, u32: 123, u64: 456, f: 1.23, d: 4.56, "
        "s: 'abc', dt: date('2023-01-01'), dttm: datetime('2023-01-01T12:00:00'), "
        "ivl: interval('1 year'), l: [1,2], m: {'k':1}});"
    )
    # INT32非法
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:T {id: 'abc'})")
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    # INT64非法
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:T {id: 1, i64: 'bad'})")
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    # UNSIGNED非法
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:T {id: 2, u32: -1})")
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    # FLOAT非法
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:T {id: 3, f: 'bad'})")
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    # DATE非法
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:T {id: 4, dt: 'notadate'})")
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    # DATETIME非法
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:T {id: 5, dttm: 'notadatetime'})")
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    # INTERVAL非法
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:T {id: 6, ivl: 'notaninterval'})")
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    conn.close()
    db.close()


# DB-003-03 类型支持-计算表达式
@pytest.mark.skip(reason="expressions not yet supported")
def test_return_expression(tmp_path):
    db_dir = tmp_path / "expr"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    result = conn.execute("RETURN 1+2, date('2023-01-01'), interval('1 year 2 days');")
    assert result is not None
    assert len(result) == 1
    row = result[0]
    assert row[0] == 3  # 1 + 2
    assert row[1] == "2023-01-01"  # Date
    assert row[2] == "1 year 2 days"  # Interval
    conn.close()
    db.close()


# DB-003-04 DDL-创建点表
def test_create_node_table(tmp_path):
    db_dir = tmp_path / "create_node"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY (name));"
    )
    # conn.execute("CREATE NODE TABLE city(name STRING, PRIMARY KEY (name));")
    conn.close()
    db.close()


def test_create_node_table_with_default_value(tmp_path):
    db_dir = tmp_path / "create_node_with_default"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person (name STRING, age INT64 DEFAULT 0, PRIMARY KEY (name));"
    )
    conn.close()
    db.close()


# TODO: error codes are not correctly handled yet
def test_create_node_table_errors(tmp_path):
    db_dir = tmp_path / "create_node_errors"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY (name));"
    )
    # 1. 重复创建点表
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY (name));")
    assert str(ERR_INVALID_SCHEMA) in str(excinfo.value)
    # 2. 创建点表，主键缺失
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE NODE TABLE person1(name STRING, age INT64);")
    assert str("Failed to compile DDL plan") in str(excinfo.value)
    # 3. 创建点表，属性默认值不合法
    with pytest.raises(Exception) as excinfo:
        conn.execute(
            "CREATE NODE TABLE person2(name STRING, age INT64 DEFAULT 'abc', PRIMARY KEY (name));"
        )
    assert str("Failed to compile DDL plan") in str(excinfo.value)
    conn.close()
    db.close()


# DB-003-05 DDL-创建边表
def test_create_rel_table(tmp_path):
    db_dir = tmp_path / "create_rel"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    # 先创建点表
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    # conn.execute("CREATE NODE TABLE city(name STRING, PRIMARY KEY(name));")
    # 1. 创建单关系边表
    conn.execute(
        "CREATE REL TABLE follows(FROM person TO person, weight DOUBLE, MANY_MANY);"
    )
    # 2. 创建多关系边表
    # conn.execute("CREATE REL TABLE Knows(FROM person TO person, FROM person TO city);")
    conn.close()
    db.close()


# TODO: error codes are not correctly handled yet
def test_create_rel_table_errors(tmp_path):
    db_dir = tmp_path / "create_rel_errors"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute(
        "CREATE REL TABLE follows(FROM person TO person, weight DOUBLE, MANY_MANY);"
    )
    # 1. 已有边表重复创建
    with pytest.raises(Exception) as excinfo:
        conn.execute(
            "CREATE REL TABLE follows(FROM person TO person, weight DOUBLE, MANY_MANY);"
        )
    assert str(ERR_INVALID_SCHEMA) in str(excinfo.value)
    # 2. 创建边表，FROM/TO端点表不存在
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE REL TABLE NewFollows(FROM person TO user, MANY_MANY);")
    assert str("Failed to compile DDL plan") in str(excinfo.value)
    conn.close()
    db.close()


# TODO: confirm if this is as expected behavior
def test_create_duplicated_rel_table_between_same_vertex_tables(tmp_path):
    db_dir = tmp_path / "create_duplicated_rel"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute(
        "CREATE REL TABLE follows(FROM person TO person, weight DOUBLE, MANY_MANY);"
    )
    conn.execute("CREATE REL TABLE knows(FROM person TO person, MANY_MANY);")
    conn.close()
    db.close()


# DB-003-06 DDL-ALTER TABLE
@pytest.mark.skip(reason="success in commit 2e71a77, but failed after commit 92b3d80")
def test_alter_vertex_table(tmp_path):
    db_dir = tmp_path / "alter_table"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
    # 1. add property
    # correctly add a new property
    conn.execute("ALTER TABLE person ADD grade INT64;")
    # incorrectly add a property that already exists
    with pytest.raises(Exception) as excinfo:
        conn.execute("ALTER TABLE person ADD age INT64;")
    assert str(ERR_INVALID_SCHEMA) in str(excinfo.value)
    # 2. rename property
    # correctly rename a property
    conn.execute("ALTER TABLE person RENAME age TO newAge;")
    # incorrectly rename a property that does not exist
    with pytest.raises(Exception) as excinfo:
        conn.execute("ALTER TABLE person RENAME age1 TO newAge1;")
    assert str(ERR_INVALID_SCHEMA) in str(excinfo.value)
    # 3. drop property
    # correctly drop a property
    conn.execute("ALTER TABLE person DROP newAge;")
    # incorrectly drop a property that does not exist
    with pytest.raises(Exception) as excinfo:
        conn.execute("ALTER TABLE person DROP age1;")
    assert str(ERR_INVALID_SCHEMA) in str(excinfo.value)
    conn.close()
    db.close()


@pytest.mark.skip(reason="success in commit 2e71a77, but failed after commit 92b3d80")
def test_alter_edge_table(tmp_path):
    db_dir = tmp_path / "alter_edge_table"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute(
        "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE, MANY_MANY);"
    )
    # 1. add property
    # correctly add a new property
    conn.execute("ALTER TABLE knows ADD since INT64;")
    # incorrectly add a property that already exists
    with pytest.raises(Exception) as excinfo:
        conn.execute("ALTER TABLE knows ADD weight DOUBLE;")
    assert str(ERR_INVALID_SCHEMA) in str(excinfo.value)
    # 2. rename property
    # correctly rename a property
    conn.execute("ALTER TABLE knows RENAME weight TO newWeight;")
    # incorrectly rename a property that does not exist
    with pytest.raises(Exception) as excinfo:
        conn.execute("ALTER TABLE knows RENAME weight1 TO newWeight1;")
    assert str(ERR_INVALID_SCHEMA) in str(excinfo.value)
    conn.close()
    db.close()


@pytest.mark.skip(reason="drop property on edge table not yet supported")
def test_alter_edge_table_drop_property(tmp_path):
    db_dir = tmp_path / "alter_edge_table_drop_property"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute(
        "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE, MANY_MANY);"
    )
    # correctly drop a property
    conn.execute("ALTER TABLE knows DROP weight;")
    # incorrectly drop a property that does not exist
    with pytest.raises(Exception) as excinfo:
        conn.execute("ALTER TABLE knows DROP weight1;")
    assert str(ERR_INVALID_SCHEMA) in str(excinfo.value)
    conn.close()
    db.close()


# DB-003-07 DDL-DROP TABLE
def test_drop_table(tmp_path):
    db_dir = tmp_path / "drop_table"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    # 创建点表和边表
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute(
        "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE, MANY_MANY);"
    )
    # 1. DROP edge table
    conn.execute("DROP TABLE knows;")
    # 2. DROP vertex table
    conn.execute("DROP TABLE person;")


# TODO: Currently drop vertex table will also drop all edges connected to it by default.
# Is this the expected behavior?
def test_drop_table_errors(tmp_path):
    db_dir = tmp_path / "drop_table_errors"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    # 创建点表和边表
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute(
        "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE, MANY_MANY);"
    )
    # 1. DROP vertex table will also drop all edges connected to it by default
    conn.execute("DROP TABLE person;")
    # the edge table has already been dropped, so this will fail
    with pytest.raises(Exception) as excinfo:
        conn.execute("DROP TABLE knows;")
    assert str(ERR_INVALID_SCHEMA) in str(excinfo.value)
    # 2. DROP table that does not exist
    with pytest.raises(Exception) as excinfo:
        conn.execute("DROP TABLE person;")
    assert str(ERR_INVALID_SCHEMA) in str(excinfo.value)
    conn.close()
    db.close()


# DB-003-08 DML-创建节点
@pytest.mark.skip(reason="DML语法暂未支持")
def test_insert_node(tmp_path):
    db_dir = tmp_path / "insert_node"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    # 准备schema
    conn.execute("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
    # case 1: 合法插入（全属性）
    conn.execute("CREATE (u:person{name:'Alice',age:35});")
    # case 2: 合法插入（缺省age，nullable，默认为NULL）
    conn.execute("CREATE (u:person{name:'Josh'});")
    # case 3: 主键缺失，失败
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (u:person{age:36});")
    assert ERROR_STRINGS[ERR_SCHEMA_MISMATCH] in str(excinfo.value)
    # case 4: 主键重复，失败
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (u:person{name:'Alice', age:26});")
    assert ERROR_STRINGS[ERR_SCHEMA_MISMATCH] in str(excinfo.value)
    # case 5: schema不一致，多余属性，失败
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (u:person{name:'Alice', age:26, addr:'aa'});")
    assert ERROR_STRINGS[ERR_SCHEMA_MISMATCH] in str(excinfo.value)
    conn.close()
    db.close()


# DB-003-09 DML-创建边
@pytest.mark.skip(reason="DML语法暂未支持")
def test_insert_edge(tmp_path):
    db_dir = tmp_path / "insert_edge"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    # 准备schema
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute(
        "CREATE REL TABLE follows(FROM person TO person, since INT64, MANY_MANY);"
    )
    # 插入端点
    conn.execute("CREATE (u:person{name:'Alice'});")
    conn.execute("CREATE (u:person{name:'Josh'});")
    # case 1: 匹配端点插入边
    conn.execute(
        "MATCH (u1:person), (u2:person) WHERE u1.name = 'Alice' "
        "AND u2.name = 'Josh' CREATE (u1)-[:follows {since: 2011}]->(u2);"
    )
    # case 2: 只匹配一个端点
    conn.execute(
        "MATCH (a:person), (b:person) WHERE a.name = 'Alice' CREATE (a)-[:follows {since:2022}]->(b);"
    )
    # case 3: 匹配不到端点，不新增边
    conn.execute(
        "MATCH (a:person), (b:person) WHERE a.name = 'nobody' CREATE (a)-[:follows {since:2022}]->(b);"
    )
    # case 4: 直接创建端点和边
    conn.execute(
        "CREATE (u:person {name: 'Alice1'})-[:follows {since:2022}]->(b:person {name: 'Josh1'});"
    )
    # case 5: 端点已存在，主键冲突
    with pytest.raises(Exception) as excinfo:
        conn.execute(
            "CREATE (u:person {name: 'Alice'})-[:follows {since:2022}]->(b:person {name: 'Josh2'});"
        )
    assert ERROR_STRINGS[ERR_SCHEMA_MISMATCH] in str(excinfo.value)
    # case 6: 边属性schema不一致
    with pytest.raises(Exception) as excinfo:
        conn.execute(
            "CREATE (u:person {name: 'Alice'})-[:follows {nonprop:2022}]->(b:person {name: 'Josh2'});"
        )
    assert ERROR_STRINGS[ERR_SCHEMA_MISMATCH] in str(excinfo.value)
    conn.close()
    db.close()


# DB-003-10 DML-SET节点属性
@pytest.mark.skip(reason="DML语法暂未支持")
def test_set_node_property(tmp_path):
    db_dir = tmp_path / "set_node_prop"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    # 准备schema和数据
    conn.execute("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
    conn.execute("CREATE (u:person{name:'Alice',age:35});")
    # case 1: 合法更新
    result = conn.execute(
        "MATCH (u:person) WHERE u.name = 'Alice' SET u.age = 50 RETURN u.*;"
    )
    assert result[0][1] == 50
    # case 2: 设置为NULL
    result = conn.execute(
        "MATCH (u:person) WHERE u.name = 'Alice' SET u.age = NULL RETURN u.*;"
    )
    assert result[0][1] is None
    # case 3: 新增属性
    result = conn.execute(
        "MATCH (u) SET u.population = 0 RETURN label(u), u.name, u.population;"
    )
    assert result[0][2] == 0
    # case 4: 匹配不到节点
    result = conn.execute(
        "MATCH (u:person) WHERE u.name = 'nobody' SET u.age = 50 RETURN u.*;"
    )
    assert result == []
    # case 5: 属性schema不一致
    with pytest.raises(Exception) as excinfo:
        conn.execute(
            "MATCH (u:person) WHERE u.name = 'Alice' SET u.addr = '' RETURN u.*;"
        )
    assert ERROR_STRINGS[ERR_SCHEMA_MISMATCH] in str(excinfo.value)
    conn.close()
    db.close()


# DB-003-11 DML-SET边属性
@pytest.mark.skip(reason="DML语法暂未支持")
def test_set_edge_property(tmp_path):
    db_dir = tmp_path / "set_edge_prop"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    # 准备schema和数据
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute(
        "CREATE REL TABLE follows(FROM person TO person, since INT64, MANY_MANY);"
    )
    conn.execute("CREATE (u:person{name:'Alice'});")
    conn.execute("CREATE (u:person{name:'Josh'});")
    conn.execute(
        "MATCH (u1:person), (u2:person) WHERE u1.name = 'Alice' AND"
        " u2.name = 'Josh' CREATE (u1)-[:follows {since: 2011}]->(u2);"
    )
    # case 1: 合法更新
    result = conn.execute(
        "MATCH (u0:person)-[f:follows]->(u1:person) WHERE u0.name = 'Alice' AND u1.name = 'Josh' SET f.since = 1999 RETURN f;"
    )
    assert result[0][0] == 1999 or result[0][1] == 1999  # 取决于返回格式
    # case 2: 多label关系
    result = conn.execute(
        "MATCH (u0)-[f]->() WHERE u0.name = 'Alice' SET f.since = 1999 RETURN f;"
    )
    assert result[0][0] == 1999 or result[0][1] == 1999
    # case 3: 属性schema不一致
    with pytest.raises(Exception) as excinfo:
        conn.execute(
            "MATCH (u0)-[f]->() WHERE u0.name = 'Alice' SET f.noprop = 1999 RETURN f;"
        )
    assert ERROR_STRINGS[ERR_SCHEMA_MISMATCH] in str(excinfo.value)
    conn.close()
    db.close()


# DB-003-13 查询-同步/异步
@pytest.mark.skip(reason="DML语法暂未支持")
def test_query_sync(tmp_path):
    db_dir = tmp_path / "query_sync"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")
    conn.execute("INSERT INTO T(id) VALUES (1);")
    result = conn.execute("MATCH (n:T) RETURN n;")
    assert result is not None
    conn.close()
    db.close()


@pytest.mark.skip(reason="async_execute未实现")
def test_query_async(tmp_path):
    db_dir = tmp_path / "query_async"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    fut = conn.async_execute("MATCH (n) RETURN n;")
    result = fut.result()
    assert result is not None
    conn.close()
    db.close()


# DB-003-20 查询-错误处理
def test_query_syntax_error(tmp_path):
    db_dir = tmp_path / "syntax_error"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    with pytest.raises(Exception) as excinfo:
        conn.execute("MATCH (n RETURN n;")  # 括号不匹配
    assert str(ERR_COMPILATION) in str(excinfo.value)
    conn.close()
    db.close()
