import os
import sys
import time

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))
from errors import ERR_CONNECTION_BROKEN
from errors import ERR_INVALID_ARGUMENT
from errors import ERR_LOAD_OVERFLOW
from errors import ERR_NETWORK
from errors import ERR_POOL_EXHAUSTED
from errors import ERROR_STRINGS

from nexg.database import Database


# DB-002-01 本地连接
# DB-002-02 关闭本地连接
def test_local_connection(tmp_path):
    db_dir = tmp_path / "local_conn_db"
    db = Database(str(db_dir), "w")
    conn = db.connect()
    assert conn is not None
    conn.close()
    db.close()


# DB-002-03 本地连接参数
# TODO: more connection parameters to test
def test_local_connection_params(tmp_path):
    db_dir = tmp_path / "local_conn_param_db"
    db = Database(str(db_dir), "w", max_thread_num=4)
    conn = db.connect()
    assert conn is not None
    conn.close()
    db.close()


# DB-002-04 本地连接异常
@pytest.mark.xfail(reason="Invalid parameters not handled")
def test_local_connection_invalid_param(tmp_path):
    db_dir = tmp_path / "local_conn_invalid_db"
    with pytest.raises(Exception) as excinfo:
        db = Database(str(db_dir), "w", max_thread_num=-1)
    assert ERROR_STRINGS[ERR_INVALID_ARGUMENT] in str(excinfo.value)
    db.close()


@pytest.fixture
def started_server(tmp_path, unused_tcp_port):
    db_dir = tmp_path / "remote_db"
    db = Database(str(db_dir), "w")
    port = unused_tcp_port
    db.serve("127.0.0.1", port)
    # sleep to ensure server is ready
    time.sleep(5)
    yield db, port
    db.close()


# DB-002-05 远程连接（需实现db.serve/Session.open，未实现时skip）
# DB-002-06 关闭远程session
@pytest.mark.skip(reason="db.serve/Session.open not implemented")
def test_remote_connection(started_server):
    db, port = started_server
    from nexg.session import Session

    session = Session.open(f"neug://user:pass@127.0.0.1:{port}/")
    assert session is not None
    session.close()


# DB-002-07 远程连接参数
@pytest.mark.skip(reason="Session not implemented")
def test_remote_connection_params(started_server):
    db, port = started_server
    from nexg.session import Session

    session = Session.open(
        f"neug://user:pass@127.0.0.1:{port}/",
        timeout=10,
        query_timeout=5,
        num_threads=2,
    )
    assert session is not None
    session.close()


# DB-002-08 远程连接错误IP/端口
@pytest.mark.skip(reason="Session not implemented")
def test_remote_connection_wrong_ip_port(started_server):
    db, port = started_server
    from nexg.session import Session

    with pytest.raises(Exception) as excinfo:
        Session.open(f"neug://user:pass@256.256.256.256:{port}/")
    assert ERROR_STRINGS[ERR_NETWORK] in str(excinfo.value)
    with pytest.raises(Exception) as excinfo:
        Session.open(f"neug://user:pass@127.0.0.1:{port + 1}/")
    assert ERROR_STRINGS[ERR_NETWORK] in str(excinfo.value)


# DB-002-09 远程连接中断
@pytest.mark.skip(reason="Session not implemented")
def test_remote_connection_broken(started_server):
    db, port = started_server
    from nexg.session import Session

    session = Session.open(f"neug://user:pass@127.0.0.1:{port}/")
    # simulate server disconnect
    db.close()
    time.sleep(5)
    with pytest.raises(Exception) as excinfo:
        session.execute("MATCH (n) RETURN n")
    assert ERROR_STRINGS[ERR_CONNECTION_BROKEN] in str(excinfo.value)
    session.close()


# DB-002-10 事务未提交连接中断
@pytest.mark.skip(reason="Session not implemented")
def test_tx_not_commit_connection_broken(started_server):
    db, port = started_server
    from nexg.session import Session

    session = Session.open(f"neug://user:pass@127.0.0.1:{port}/")
    session.begin()
    session.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")
    # simulate server disconnect
    db.close()
    time.sleep(5)
    with pytest.raises(Exception) as excinfo:
        session.commit()
    assert ERROR_STRINGS[ERR_CONNECTION_BROKEN] in str(excinfo.value)
    # reconnect and check if the transaction is rolled back
    session2 = Session.open(f"neug://user:pass@127.0.0.1:{port}/")
    # TODO: do we support to query schema directly?
    session2.execute("MATCH (n:T) RETURN n")
    result = session2.execute("SHOW TABLES;")
    assert len(result) == 0
    # close the session
    session2.close()


# DB-002-11 服务端限流
@pytest.mark.skip(reason="Not supported in current version")
def test_server_load_overflow(started_server):
    db, port = started_server
    from nexg.session import Session

    sessions = []
    try:
        for _ in range(10000):
            try:
                s = Session.open(f"neug://user:pass@127.0.0.1:{port}/")
                sessions.append(s)
            except Exception as exc:
                assert ERROR_STRINGS[ERR_LOAD_OVERFLOW] in str(exc)
                break
        else:
            pytest.fail("Expected ERR_LOAD_OVERFLOW but did not get exception")
    finally:
        for s in sessions:
            try:
                s.close()
            except Exception:
                pass


# DB-002-12 本地连接关闭后再操作
@pytest.mark.skip(reason="No exception is raised when executing after close")
def test_local_connection_after_close(tmp_path):
    # local connection after close
    db_dir = tmp_path / "conn_after_close_db"
    db = Database(str(db_dir), "w")
    conn = db.connect()
    conn.close()
    with pytest.raises(Exception) as excinfo:
        conn.execute("MATCH (n) RETURN n")
    assert ERROR_STRINGS[ERR_CONNECTION_BROKEN] in str(excinfo.value)
    db.close()


# DB-002-12 远程连接关闭后再操作
@pytest.mark.skip(reason="Session not implemented")
def test_remote_connection_after_close(started_server):
    # remote connection after close
    from nexg.session import Session

    db, port = started_server
    session = Session.open(f"neug://user:pass@127.0.0.1:{port}/")
    session.close()
    with pytest.raises(Exception) as excinfo:
        session.execute("MATCH (n) RETURN n")
    assert ERROR_STRINGS[ERR_CONNECTION_BROKEN] in str(excinfo.value)
    db.close()


# DB-002-13 服务端重启
@pytest.mark.skip(reason="Session not implemented")
def test_server_restart(started_server):
    db, port = started_server
    from nexg.session import Session

    session = Session.open(f"neug://user:pass@127.0.0.1:{port}/")
    db.close()
    time.sleep(2)
    db2 = Database("remote_db", "w")
    db2.serve("127.0.0.1", port)
    time.sleep(2)
    try:
        try:
            session.execute("MATCH (n) RETURN n")
        except Exception as e:
            assert ERROR_STRINGS[ERR_CONNECTION_BROKEN] in str(e)
    finally:
        session.close()
        db2.close()


# DB-002-14 连接池耗尽
@pytest.mark.skip(reason="Session not implemented")
def test_connection_pool_exhausted(started_server):
    db, port = started_server
    from nexg.session import Session

    # suppose the server has a connection pool limit of 8
    s1 = Session.open(f"neug://user:pass@127.0.0.1:{port}/", num_threads=8)
    # try to open more connections than the pool limit
    with pytest.raises(Exception) as excinfo:
        Session.open(f"neug://user:pass@127.0.0.1:{port}/")
    assert ERROR_STRINGS[ERR_POOL_EXHAUSTED] in str(excinfo.value)
    s1.close()


# DB-002-15 连接参数边界
@pytest.mark.xfail(reason="no exception is raised for max_thread_num > os.cpu_count()")
def test_connection_param_boundary(tmp_path):
    db_dir = tmp_path / "conn_param_boundary_db"
    # test with maximum cores
    max_cores = os.cpu_count() or 1
    db = Database(str(db_dir), "w", max_thread_num=max_cores)
    db.close()
    with pytest.raises(Exception) as excinfo:
        # test with more than maximum cores
        Database(str(db_dir), "w", max_thread_num=max_cores + 1)
    assert ERROR_STRINGS[ERR_INVALID_ARGUMENT] in str(excinfo.value)
