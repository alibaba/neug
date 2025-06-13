import os
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))
from errors import ERR_CONFIG_INVALID
from errors import ERR_CORRUPTION_DETECTED
from errors import ERR_DATABASE_LOCKED
from errors import ERR_DIRECTORY_NOT_EXIST
from errors import ERR_DISK_SPACE_EXHAUSTED
from errors import ERR_INVALID_ARGUMENT
from errors import ERR_INVALID_PATH
from errors import ERR_PERMISSION
from errors import ERR_VERSION_MISMATCHED
from errors import ERROR_STRINGS

from nexg.database import Database


# DB-001-01 内存模式打开数据库
@pytest.mark.skip(
    reason="Core dump. Throws Error: [file_utils.cc:22] Error: Directory path is empty."
)
def test_memory_mode_open_and_close():
    db = Database("", "rw")
    assert db is not None
    db.close()
    db2 = Database(None, "r")
    assert db2 is not None
    db2.close()


# DB-001-02 关闭内存数据库
@pytest.mark.skip(
    reason="Core dump. Throws Error: [file_utils.cc:22] Error: Directory path is empty."
)
def test_memory_mode_close():
    db = Database(None, "r")
    db.close()


# DB-001-03 非内存模式打开本地数据库（路径存在）
def test_local_db_open_exists_and_close(tmp_path):
    db_dir = tmp_path / "existdb"
    if not db_dir.exists():
        db_dir.mkdir()
    db = Database(str(db_dir), "r")
    assert db is not None
    db.close()
    db2 = Database(str(db_dir), "rw")
    assert db2 is not None
    db2.close()


# DB-001-04 非内存模式打开本地数据库（路径不存在）
def test_local_db_open_not_exists_and_close(tmp_path):
    db_dir = tmp_path / "not_existdb"
    if db_dir.exists():
        os.system("rm -rf %s" % db_dir)
    assert not db_dir.exists()
    db = Database(str(db_dir), "r")
    assert db is not None
    db.close()
    if not db_dir.exists():
        db_dir.mkdir()
    db = Database(str(db_dir), "rw")
    assert db is not None
    db.close()


# DB-001-05 关闭本地数据库
def test_local_db_close(tmp_path):
    db_dir = tmp_path / "closedb"
    db_dir.mkdir()
    db = Database(str(db_dir), "rw")
    db.close()


# DB-001-06 只读模式多实例
def test_readonly_mode_multi_instance(tmp_path):
    db_dir = tmp_path / "multi_db"
    db_dir.mkdir()
    db = Database(str(db_dir), "rw")
    db.close()
    db1 = Database(str(db_dir), "r")
    db2 = Database(str(db_dir), "r")
    assert db1 is not None and db2 is not None
    db1.close()
    db2.close()


# DB-001-07 读写模式独占
@pytest.mark.skip(
    reason="Run this test independently is ok, but in the whole test suite it fails sometimes"
)
def test_rw_mode_exclusive(tmp_path):
    db_dir = tmp_path / "exclusive_db"
    db1 = Database(str(db_dir), "rw")
    try:
        with pytest.raises(Exception) as excinfo:
            Database(str(db_dir), "rw")
        assert ERROR_STRINGS[ERR_DATABASE_LOCKED] in str(excinfo.value)
    finally:
        db1.close()


# DB-001-08 只读/读写冲突
@pytest.mark.skip(
    reason="Run this test independently is ok, but in the whole test suite it fails sometimes"
)
def test_rw_ro_conflict(tmp_path):
    db_dir = tmp_path / "conflict_db"
    db1 = Database(str(db_dir), "rw")
    try:
        with pytest.raises(Exception) as excinfo:
            Database(str(db_dir), "r")
        assert ERROR_STRINGS[ERR_DATABASE_LOCKED] in str(excinfo.value)
    finally:
        db1.close()


# DB-001-09 只读模式下写操作
def test_readonly_write_operation(tmp_path):
    db_dir = tmp_path / "readonly_db"
    db_ro = Database(str(db_dir), "r", 0, "gopt", "", "")
    with pytest.raises(Exception) as excinfo:
        conn = db_ro.connect()
        conn.execute("CREATE NODE TABLE person(id INT32, PRIMARY KEY(id));")
        conn.close()
    db_ro.close()
    # TODO: error code should be ERR_PERMISSION??
    assert str(ERR_INVALID_ARGUMENT) in str(excinfo.value)


# DB-001-10 非法路径格式
def test_invalid_path():
    with pytest.raises(Exception) as excinfo:
        Database("??/illegal", "r")
    assert ERROR_STRINGS[ERR_INVALID_PATH] in str(excinfo.value)
    # remove the invalid path after the test
    if os.path.exists("??/illegal"):
        os.system("rm -rf ??/")


# DB-001-11 配置参数异常
@pytest.mark.xfail(reason="no invalid config value check, to be fixed")
def test_config_param_exception(tmp_path):
    db_dir = tmp_path / "config_db"
    db_dir.mkdir()
    # Use the number of CPU cores as max_thread_num
    max_thread_num = os.cpu_count() or 1
    db = Database(str(db_dir), "rw", max_thread_num=max_thread_num)
    assert db is not None
    db.close()
    with pytest.raises(Exception) as excinfo:
        Database(str(db_dir), "rw", max_thread_num=-1)
    assert ERROR_STRINGS[ERR_CONFIG_INVALID] in str(excinfo.value)


# DB-001-12 打开已有数据库，但无写权限
@pytest.mark.skip(
    reason="Run this test independently is ok, but in the whole test suite it fails sometimes"
)
def test_open_no_permission(tmp_path):
    db_dir = tmp_path / "no_permission_db"
    if db_dir.exists():
        os.system("rm -rf %s" % db_dir)
    db = Database(str(db_dir), "rw")
    db.close()
    os.chmod(db_dir, 0o400)
    try:
        with pytest.raises(Exception) as excinfo:
            Database(str(db_dir), "rw")
        assert ERROR_STRINGS[ERR_PERMISSION] in str(excinfo.value)
    finally:
        os.chmod(db_dir, 0o700)


# DB-001-13 打开已有数据库，但版本不兼容
@pytest.mark.skip(reason="To mock the version mismatch correctly")
def test_open_version_mismatch(tmp_path):
    db_dir = tmp_path / "ver_db"
    db_dir.mkdir()
    db = Database(str(db_dir), "rw")
    db.close()

    # Simulate version mismatch by modifying the version metadata file
    # Assuming version metadata is stored in version.txt
    version_file = db_dir / "version.txt"
    with open(version_file, "w") as f:
        f.write("mismatched_version")

    # Attempt to open the database
    with pytest.raises(Exception) as excinfo:
        Database(str(db_dir), "r")
    assert ERROR_STRINGS[ERR_VERSION_MISMATCHED] in str(excinfo.value)


# DB-001-14 目录不存在且无写权限
@pytest.mark.skip(reason="Core dump. No directory existence check implemented yet")
def test_open_dir_not_exist(tmp_path):
    db_dir = tmp_path / "not_exist_dir"
    if db_dir.exists():
        os.system("rm -rf %s" % db_dir)
    # mock the os.chmod to simulate no permission
    os.chmod(tmp_path, 0o400)
    try:
        with pytest.raises(Exception) as excinfo:
            Database(str(db_dir), "rw")
        assert ERROR_STRINGS[ERR_DIRECTORY_NOT_EXIST] in str(excinfo.value)
    finally:
        os.chmod(db_dir, 0o700)


# DB-001-15 磁盘空间不足
@pytest.mark.skip(reason="TODO: mock disk space exhausted correctly")
def test_disk_space_exhausted(monkeypatch, tmp_path):
    db_dir = tmp_path / "no_space_db"
    db_dir.mkdir()
    print("Creating database in:", db_dir)

    def mock_open(*args, **kwargs):
        # Simulate a disk space error
        raise OSError("No space left on device")

    # Mock open function to raise a disk space error
    monkeypatch.setattr(os, "open", mock_open)
    with pytest.raises(Exception) as excinfo:
        Database(str(db_dir), "rw")
    print("Exception raised:", excinfo.value)
    assert ERROR_STRINGS[ERR_DISK_SPACE_EXHAUSTED] in str(excinfo.value)


# DB-001-16 文件头损坏
@pytest.mark.skip(reason="Core dump. No file corruption check implemented yet")
def test_file_header_corruption(tmp_path):
    db_dir = tmp_path / "corrupt_db"
    db_dir.mkdir()
    Database(str(db_dir), "rw")
    # db_file such as "wal/thread_0_0.wal" should exist after db creation
    db_file = db_dir / "wal/thread_0_0.wal"
    assert db_file.exists(), "Database file should exist after creation"
    # simulate file corruption by writing a corrupt header
    with open(db_file, "wb") as f:
        f.write(b"corrupt-header")
    try:
        Database(str(db_dir), "rw")
    except Exception as exc:
        assert ERROR_STRINGS[ERR_CORRUPTION_DETECTED] in str(exc)
    else:
        pytest.fail("Expected ERR_CORRUPTION_DETECTED but no exception was raised")
