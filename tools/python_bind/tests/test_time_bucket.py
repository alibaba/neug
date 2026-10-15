# Copyright 2026 Alibaba Group Holding Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""TIME_BUCKET end-to-end contract, including binding and execution types."""

from datetime import date
from datetime import datetime
from datetime import timedelta

import pytest

from neug.database import Database


@pytest.fixture
def conn(tmp_path):
    db = Database(str(tmp_path / "time_bucket"), "w", checkpoint_on_close=False)
    connection = db.connect()
    try:
        yield connection
    finally:
        connection.close()
        db.close()


def scalar(conn, expression, parameters=None):
    return list(conn.execute("RETURN " + expression, parameters=parameters))[0][0]


@pytest.mark.parametrize("kind", ["DATE", "TIMESTAMP"])
@pytest.mark.parametrize(
    "third",
    [
        None,
        "INTERVAL('6 hours')",
        "DATE('2024-03-01')",
        "TIMESTAMP('2024-03-01 06:00:00')",
    ],
)
def test_overloads(conn, kind, third):
    ts = "DATE('2024-03-20')"
    if kind == "TIMESTAMP":
        ts = "TIMESTAMP('2024-03-20 00:00:00')"
    expression = f"time_bucket(INTERVAL('1 day'), {ts}"
    if third is not None:
        expression += ", " + third
    expression += ")"
    shifted = third in {"INTERVAL('6 hours')", "TIMESTAMP('2024-03-01 06:00:00')"}
    expected = datetime(2024, 3, 19, 6) if shifted else datetime(2024, 3, 20)
    if kind == "DATE" and not (third and third.startswith("TIMESTAMP")):
        expected = expected.date()
    actual = scalar(conn, expression)
    assert actual == expected
    assert type(actual) is type(expected)


@pytest.mark.parametrize(
    "width,ts,expected",
    [
        ("1 millisecond", "2024-03-20 15:26:12.345", "2024-03-20 15:26:12.345"),
        ("1000 microseconds", "2024-03-20 15:26:12.345", "2024-03-20 15:26:12.345"),
        ("10 milliseconds", "2024-03-20 15:26:12.349", "2024-03-20 15:26:12.340"),
        ("10 milliseconds", "2024-03-20 15:26:12.350", "2024-03-20 15:26:12.350"),
        ("10 milliseconds", "2024-03-20 15:26:12.351", "2024-03-20 15:26:12.350"),
        ("1 second", "1969-12-31 23:59:59.999", "1969-12-31 23:59:59"),
        ("10 minutes", "2024-03-20 15:26:12.345", "2024-03-20 15:20:00"),
        ("1 hour", "2024-03-20 15:26:12.345", "2024-03-20 15:00:00"),
        ("1 week", "2024-03-20 15:26:12.345", "2024-03-18 00:00:00"),
        ("1 week", "2000-01-02 23:59:59.999", "1999-12-27 00:00:00"),
        ("1 month", "2024-02-29 23:59:59.999", "2024-02-01 00:00:00"),
        ("2 months", "2024-02-29 23:59:59.999", "2024-01-01 00:00:00"),
        ("3 months", "2024-05-31 23:59:59.999", "2024-04-01 00:00:00"),
        ("1 year", "1969-12-31 23:59:59.999", "1969-01-01 00:00:00"),
        ("2 years", "1999-12-31 23:59:59.999", "1998-01-01 00:00:00"),
    ],
)
def test_default_buckets(conn, width, ts, expected):
    assert scalar(
        conn, f"time_bucket(INTERVAL('{width}'), TIMESTAMP('{ts}'))"
    ) == datetime.fromisoformat(expected)


@pytest.mark.parametrize(
    "width,millis",
    [
        ("1 hour 1 minute", 3660000),
        ("1 day 2 hours 30 minutes", 95400000),
        ("1 day -1 hour", 82800000),
    ],
)
def test_composite_fixed_width(conn, width, millis):
    origin = datetime(2024, 1, 1)
    for delta in [-1, 0, millis - 1, millis, millis + 1]:
        ts = origin + timedelta(milliseconds=delta)
        result = scalar(
            conn,
            f"time_bucket(INTERVAL('{width}'), CAST($ts, 'TIMESTAMP'), TIMESTAMP('2024-01-01 00:00:00'))",
            {"ts": ts.isoformat(sep=" ")},
        )
        expected = origin + timedelta(
            milliseconds=(-millis if delta < 0 else (millis if delta >= millis else 0))
        )
        assert result == expected


@pytest.mark.parametrize(
    "origin",
    ["2024-02-01 00:00:00", "2024-02-15 12:34:56.789", "2024-02-29 23:59:59.999"],
)
def test_month_origin_ignores_day_and_time(conn, origin):
    assert scalar(
        conn,
        f"time_bucket(INTERVAL('2 months'), DATE('2024-03-20'), TIMESTAMP('{origin}'))",
    ) == datetime(2024, 2, 1)


def test_month_is_not_thirty_days(conn):
    assert scalar(
        conn, "time_bucket(INTERVAL('1 month'), DATE('2024-03-15'), DATE('2024-01-01'))"
    ) == date(2024, 3, 1)
    assert scalar(
        conn, "time_bucket(INTERVAL('30 days'), DATE('2024-03-15'), DATE('2024-01-01'))"
    ) == date(2024, 3, 1)
    assert scalar(
        conn, "time_bucket(INTERVAL('1 month'), DATE('2024-02-15'), DATE('2024-01-01'))"
    ) == date(2024, 2, 1)
    assert scalar(
        conn, "time_bucket(INTERVAL('30 days'), DATE('2024-02-15'), DATE('2024-01-01'))"
    ) == date(2024, 1, 31)


@pytest.mark.parametrize(
    "width,ts,offset,expected",
    [
        ("10 minutes", "2024-03-20 15:26:12.345", "5 minutes", "2024-03-20 15:25:00"),
        ("10 minutes", "2024-03-20 15:26:12.345", "-5 minutes", "2024-03-20 15:25:00"),
        ("10 minutes", "2024-03-20 15:26:12.345", "25 minutes", "2024-03-20 15:25:00"),
        ("10 minutes", "2024-03-20 15:26:12.345", "0 seconds", "2024-03-20 15:20:00"),
        ("1 millisecond", "2024-03-31 12:00:00", "1 month", "2024-03-29 12:00:00"),
        ("1 millisecond", "1969-03-28 12:00:00", "1 month", "1969-03-28 12:00:00"),
        ("1 millisecond", "1969-02-28 12:00:00", "-1 month", "1969-02-28 12:00:00"),
        ("2 months", "2000-01-01 00:00:00", "1 month 7 days", "1999-12-08 00:00:00"),
        ("2 months", "2000-01-03 00:00:00", "-1 month 7 days", "1999-12-08 00:00:00"),
    ],
)
def test_offsets(conn, width, ts, offset, expected):
    assert scalar(
        conn,
        f"time_bucket(INTERVAL('{width}'), TIMESTAMP('{ts}'), INTERVAL('{offset}'))",
    ) == datetime.fromisoformat(expected)


def test_date_offset_keeps_intermediate_time(conn):
    assert scalar(
        conn, "time_bucket(INTERVAL('8 hour'), DATE('2024-03-20'), INTERVAL('8 hour'))"
    ) == date(2024, 3, 20)
    assert scalar(
        conn,
        "time_bucket(INTERVAL('10 hours'), DATE('2024-03-20'), INTERVAL('6 hours'))",
    ) == date(2024, 3, 19)
    assert scalar(
        conn, "time_bucket(INTERVAL('1 day'), DATE('2024-03-20'), INTERVAL('6 hours'))"
    ) == date(2024, 3, 19)


@pytest.mark.parametrize(
    "arguments",
    [
        "CAST(NULL, 'INTERVAL'), DATE('2024-01-01')",
        "NULL, TIMESTAMP('2024-01-01 00:00:00')",
        "INTERVAL('1 day'), CAST(NULL, 'DATE')",
        "INTERVAL('0 seconds'), CAST(NULL, 'TIMESTAMP')",
        "INTERVAL('1 microsecond'), CAST(NULL, 'TIMESTAMP')",
        "INTERVAL('1 day'), DATE('2024-01-01'), CAST(NULL, 'INTERVAL')",
        "INTERVAL('1 day'), DATE('2024-01-01'), CAST(NULL, 'DATE')",
        "INTERVAL('1 day'), DATE('2024-01-01'), CAST(NULL, 'TIMESTAMP')",
        "INTERVAL('1 day'), NULL, DATE('2024-01-01')",
        "INTERVAL('1 day'), NULL, TIMESTAMP('2024-01-01 00:00:00')",
    ],
)
def test_null_propagation(conn, arguments):
    assert scalar(conn, f"time_bucket({arguments})") is None


@pytest.mark.parametrize(
    "width,reason",
    [
        ("0 seconds", "greater than zero"),
        ("-1 second", "greater than zero"),
        ("-1 month", "greater than zero"),
        ("1 day -25 hours", "greater than zero"),
        ("1 microsecond", "whole milliseconds"),
        ("1500 microseconds", "whole milliseconds"),
        ("1 month 1 day", "month width"),
        ("1 month 1 hour", "month width"),
    ],
)
@pytest.mark.parametrize(
    "third", ["", ", INTERVAL('0 seconds')", ", DATE('2024-01-01')"]
)
def test_invalid_width(conn, width, reason, third):
    with pytest.raises(Exception, match=reason):
        scalar(conn, f"time_bucket(INTERVAL('{width}'), DATE('2024-01-01'){third})")


@pytest.mark.parametrize("offset", ["1 microsecond", "1500 microseconds"])
def test_invalid_offset_precision(conn, offset):
    with pytest.raises(Exception, match="whole milliseconds"):
        scalar(
            conn,
            f"time_bucket(INTERVAL('1 day'), DATE('2024-01-01'), INTERVAL('{offset}'))",
        )


@pytest.mark.parametrize(
    "arguments",
    [
        "",
        "INTERVAL('1 day')",
        "INTERVAL('1 day'), DATE('2024-01-01'), DATE('2024-01-01'), 1",
        "1, DATE('2024-01-01')",
        "'1 day', DATE('2024-01-01')",
        "INTERVAL('1 day'), '2024-01-01'",
        "INTERVAL('1 day'), 1",
        "INTERVAL('1 day'), NULL",
        "INTERVAL('1 day'), DATE('2024-01-01'), NULL",
        "INTERVAL('1 day'), DATE('2024-01-01'), '2024-01-01'",
    ],
)
def test_invalid_binding(conn, arguments):
    with pytest.raises(Exception, match="(?i)TIME_BUCKET"):
        scalar(conn, f"time_bucket({arguments})")


def test_arrow_types_and_typed_null(conn):
    pa = pytest.importorskip("pyarrow")
    table = conn.execute(
        "RETURN time_bucket(INTERVAL('1 day'), DATE('2024-03-20')) AS d, "
        "time_bucket(INTERVAL('1 day'), DATE('2024-03-20'), TIMESTAMP('2024-03-01 06:00:00')) AS t, "
        "time_bucket(INTERVAL('0 seconds'), CAST(NULL, 'DATE')) AS nd, "
        "time_bucket(INTERVAL('1 day'), DATE('2024-03-20'), CAST(NULL, 'TIMESTAMP')) AS nt"
    ).to_arrow()
    # The existing Arrow API exports DATE as timestamp[ms]. Row-result tests
    # separately verify that DATE values retain their Python datetime.date type.
    assert table.schema.field("d").type == pa.timestamp("ms")
    assert table.schema.field("t").type == pa.timestamp("ms")
    assert table.schema.field("nd").type == pa.timestamp("ms")
    assert table.schema.field("nt").type == pa.timestamp("ms")
    assert table.to_pylist() == [
        {
            "d": datetime(2024, 3, 20),
            "t": datetime(2024, 3, 19, 6),
            "nd": None,
            "nt": None,
        }
    ]


def test_columns_parameters_and_grouping(conn):
    conn.execute(
        "CREATE NODE TABLE Event(id INT64 PRIMARY KEY, ts TIMESTAMP, width INTERVAL, origin DATE, shift INTERVAL)"
    )
    for i, (ts, width, shift) in enumerate(
        [
            ("2024-03-20 15:26:12.345", "10 minutes", "5 minutes"),
            ("2024-03-20 15:29:00", "10 minutes", "5 minutes"),
            ("2024-03-21 01:00:00", "1 day", "6 hours"),
        ]
    ):
        conn.execute(
            f"CREATE (:Event {{id: {i}, ts: TIMESTAMP('{ts}'), "
            f"width: INTERVAL('{width}'), origin: DATE('2024-03-01'), "
            f"shift: INTERVAL('{shift}')}})"
        )
    rows = list(
        conn.execute(
            "MATCH (e:Event) RETURN time_bucket(e.width, e.ts, e.shift) ORDER BY e.id"
        )
    )
    assert rows == [
        [datetime(2024, 3, 20, 15, 25)],
        [datetime(2024, 3, 20, 15, 25)],
        [datetime(2024, 3, 20, 6)],
    ]
    rows = list(
        conn.execute(
            "MATCH (e:Event) RETURN time_bucket(e.width, e.ts, e.origin) ORDER BY e.id"
        )
    )
    assert rows == [
        [datetime(2024, 3, 20, 15, 20)],
        [datetime(2024, 3, 20, 15, 20)],
        [datetime(2024, 3, 21)],
    ]
    rows = list(
        conn.execute(
            "MATCH (e:Event) RETURN time_bucket(INTERVAL('1 day'), e.ts) AS bucket, count(*) ORDER BY bucket"
        )
    )
    assert rows == [[datetime(2024, 3, 20), 2], [datetime(2024, 3, 21), 1]]
    assert scalar(
        conn,
        "time_bucket(CAST($width, 'INTERVAL'), CAST($ts, 'TIMESTAMP'), CAST($origin, 'DATE'))",
        {"width": "1 day", "ts": "2024-03-20 15:26:12.345", "origin": "2024-03-01"},
    ) == datetime(2024, 3, 20)


# Expected results selected from the local DuckDB time_bucket DATE suite;
# no optional DuckDB package is needed to run these compatibility regressions.
@pytest.mark.parametrize(
    "width,ts,origin,expected",
    [
        ("10 days", "1700-01-01", "1700-01-05", "1699-12-26"),
        ("1 week", "1832-05-03", "1970-05-05", "1832-05-01"),
        ("10 days", "1897-07-05", "1970-06-07", "1897-06-29"),
        ("10 days", "1906-02-08", "1970-10-03", "1906-02-01"),
        ("2 months", "1946-09-14", "1970-07-05", "1946-09-01"),
        ("2 months", "2000-01-03", "1970-08-20", "1999-12-01"),
        ("2 years", "2004-05-20", "1970-12-31", "2002-12-01"),
        ("2 years", "2032-12-06", "1969-07-01", "2031-07-01"),
    ],
)
def test_duckdb_compatibility(conn, width, ts, origin, expected):
    assert scalar(
        conn, f"time_bucket(INTERVAL('{width}'), DATE('{ts}'), DATE('{origin}'))"
    ) == date.fromisoformat(expected)


@pytest.mark.parametrize(
    "ts,origin",
    [
        (9223372036854775807, -9223372036854775808),
        (-9223372036854775808, 9223372036854775807),
        (-9223372036854775807, 9223372036854775807),
        (9223372036854775807, 0),
        (-9223372036854775808, 0),
        (0, -9223372036854775808),
    ],
)
@pytest.mark.parametrize("width", [1, 2, 3, 10, 9223372036854775])
def test_int64_timestamp_bucket_boundaries(conn, ts, origin, width):
    # Compare in the engine: Python datetime cannot represent these timestamps.
    # Python's arbitrary-precision arithmetic supplies an independent oracle.
    expected = origin + ((ts - origin) // width) * width
    expression = (
        f"time_bucket(INTERVAL('{width} milliseconds'), CAST($ts, 'TIMESTAMP'), "
        "CAST($origin, 'TIMESTAMP'))"
    )
    parameters = {"ts": ts, "origin": origin}
    if expected < -(2**63):
        with pytest.raises(Exception, match="out of range"):
            scalar(conn, expression, parameters)
    else:
        parameters["expected"] = expected
        assert scalar(conn, expression + " = CAST($expected, 'TIMESTAMP')", parameters)


@pytest.mark.parametrize(
    "expression,parameters",
    [
        (
            "time_bucket(INTERVAL('10 milliseconds'), CAST($ts, 'TIMESTAMP'), TIMESTAMP('1970-01-01 00:00:00'))",
            {"ts": -9223372036854775808},
        ),
        (
            "time_bucket(INTERVAL('1 millisecond'), CAST($ts, 'TIMESTAMP'), INTERVAL('-1 millisecond'))",
            {"ts": 9223372036854775807},
        ),
        (
            "time_bucket(INTERVAL('1 month'), CAST($ts, 'TIMESTAMP'))",
            {"ts": 9223372036854775807},
        ),
        (
            "time_bucket(INTERVAL('1 day'), TIMESTAMP('2024-01-01 00:00:00'), CAST($offset, 'INTERVAL'))",
            {"offset": "-2147483648 days"},
        ),
        (
            "time_bucket(INTERVAL('1 day'), TIMESTAMP('2024-01-01 00:00:00'), CAST($offset, 'INTERVAL'))",
            {"offset": "-2147483648 months"},
        ),
        (
            "time_bucket(INTERVAL('1 day'), TIMESTAMP('2024-01-01 00:00:00'), INTERVAL('2147483647 months'))",
            {},
        ),
    ],
)
def test_checked_overflow(conn, expression, parameters):
    with pytest.raises(Exception, match="(?i)out of range"):
        scalar(conn, expression, parameters)


@pytest.mark.parametrize("runtime", [False, True])
@pytest.mark.parametrize(
    "compact,spaced",
    [
        (
            "1year2months3days4hours5minutes6seconds",
            "1 year 2 months 3 days 4 hours 5 minutes 6 seconds",
        ),
        ("6months15days", "6 months 15 days"),
        ("1week2days", "9 days"),
        ("1day-2hours+3minutes", "1 day -2 hours 3 minutes"),
        ("1millisecond2microseconds3us", "1005 microseconds"),
        ("1DAY2HOURS", "1 day 2 hours"),
    ],
)
def test_interval_compact_components(conn, runtime, compact, spaced):
    expression = "CAST($value, 'INTERVAL')" if runtime else f"INTERVAL('{compact}')"
    assert scalar(conn, f"{expression} = INTERVAL('{spaced}')", {"value": compact})


@pytest.mark.parametrize("runtime", [False, True])
@pytest.mark.parametrize(
    "value",
    ["1dayjunk", "junk1day", "1dayjunk2hours", "1day_2hours", "1day++2hours"],
)
def test_interval_rejects_invalid_components(conn, runtime, value):
    expression = "CAST($value, 'INTERVAL')" if runtime else f"INTERVAL('{value}')"
    with pytest.raises(Exception, match="Invalid interval format"):
        scalar(conn, expression, {"value": value})


@pytest.mark.parametrize("runtime", [False, True])
@pytest.mark.parametrize(
    "value,expected",
    [
        ("2147483647 months", "2147483647 months"),
        ("-2147483648 days", "-2147483648 days"),
        ("9223372036854775807 microseconds", "9223372036854775807 microseconds"),
        ("-9223372036854775808 microseconds", "-9223372036854775808 microseconds"),
        ("-2147483648 months 178956971 years", "4 months"),
        ("-2147483648 days 306783379 weeks", "5 days"),
        (
            "-9223372036854775808 microseconds 9223372036854775 milliseconds",
            "-808 microseconds",
        ),
        (
            "9223372036854775807 microseconds -9223372036854775 milliseconds",
            "807 microseconds",
        ),
    ],
)
def test_interval_checked_accumulation(conn, runtime, value, expected):
    expression = "CAST($value, 'INTERVAL')" if runtime else f"INTERVAL('{value}')"
    assert scalar(conn, f"{expression} = INTERVAL('{expected}')", {"value": value})


@pytest.mark.parametrize("runtime", [False, True])
@pytest.mark.parametrize(
    "value",
    [
        "2147483648 months",
        "-2147483649 days",
        "9223372036854776 milliseconds",
        "-9223372036854776 milliseconds",
        "9223372036854775807 microseconds 1 microsecond",
        "-9223372036854775808 microseconds -1 microsecond",
        "9223372036854775807 years",
        "-9223372036854775808 weeks",
        "-9223372036854775808 microseconds 9223372036854776 milliseconds",
        "9223372036854775807 microseconds -9223372036854776 milliseconds",
    ],
)
def test_interval_accumulation_overflow(conn, runtime, value):
    expression = "CAST($value, 'INTERVAL')" if runtime else f"INTERVAL('{value}')"
    with pytest.raises(Exception, match="Interval component out of range"):
        scalar(conn, expression, {"value": value})


def test_interval_parser_and_runtime_temporal_cast_regressions(conn):
    assert scalar(conn, "CAST(INTERVAL('-5 minutes'), 'INT64')") == -300000
    conn.execute("CREATE NODE TABLE D(id INT64 PRIMARY KEY, d DATE)")
    conn.execute("CREATE (:D {id: 1, d: DATE('2024-03-20')})")
    assert list(conn.execute("MATCH (d:D) RETURN CAST(d.d, 'TIMESTAMP')")) == [
        [datetime(2024, 3, 20)]
    ]
    assert scalar(
        conn, "time_bucket(INTERVAL('1 week'), CAST($ts, 'DATE'))", {"ts": "2024-03-20"}
    ) == date(2024, 3, 18)


def test_explicit_transaction_and_write_expressions(conn):
    conn.execute("CREATE NODE TABLE Bucket(id INT64 PRIMARY KEY, ts TIMESTAMP)")
    conn.begin_transaction()
    try:
        conn.execute(
            "CREATE (:Bucket {id: 1, ts: time_bucket(INTERVAL('1 hour'), TIMESTAMP('2024-03-20 15:26:00'))})",
            access_mode="insert",
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    conn.execute(
        "MATCH (b:Bucket) SET b.ts = time_bucket(INTERVAL('1 day'), b.ts)",
        access_mode="update",
    )
    conn.begin_transaction(read_only=True)
    try:
        assert list(
            conn.execute(
                "MATCH (b:Bucket) RETURN time_bucket(INTERVAL('1 day'), b.ts, DATE('2024-03-01'))",
                access_mode="read",
            )
        ) == [[datetime(2024, 3, 20)]]
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def test_row_null_precedes_width_validation(conn):
    rows = list(
        conn.execute(
            "UNWIND [0, 1] AS id "
            "WITH id, CASE WHEN id = 0 THEN INTERVAL('0 seconds') ELSE INTERVAL('1 day') END AS w, "
            "CASE WHEN id = 0 THEN CAST(NULL, 'TIMESTAMP') ELSE TIMESTAMP('2024-03-20 12:00:00') END AS ts "
            "RETURN id, time_bucket(w, ts) ORDER BY id"
        )
    )
    assert rows == [[0, None], [1, datetime(2024, 3, 20)]]
