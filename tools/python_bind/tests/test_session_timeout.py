#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest
import requests

import neug.session as session_module
from neug.proto.error_pb2 import ERR_QUERY_TIMEOUT
from neug.session import Session


class _SuccessfulResponse:
    status_code = 200
    text = ""
    _content = b""


def _session_that_times_out(monkeypatch, timeout):
    monkeypatch.setattr(
        requests.Session, "get", lambda self, url, timeout: _SuccessfulResponse()
    )

    def raise_timeout(self, url, data, timeout):
        raise requests.exceptions.ReadTimeout("legacy requests timeout detail")

    monkeypatch.setattr(requests.Session, "post", raise_timeout)
    monkeypatch.setattr(
        session_module.PyQueryRequest,
        "serialize_request",
        lambda query, access_mode: b"request",
    )
    return Session("http://localhost:10000", timeout=timeout)


@pytest.mark.parametrize(
    ("configured_timeout", "seconds"), [("2s", "2"), ("250ms", "0.25")]
)
def test_execute_timeout_has_actionable_error(monkeypatch, configured_timeout, seconds):
    session = _session_that_times_out(monkeypatch, configured_timeout)

    with pytest.raises(TimeoutError) as excinfo:
        session.execute("MATCH (n) RETURN n")

    message = str(excinfo.value)
    assert f"timed out after {seconds} seconds" in message
    assert f"session timeout: {configured_timeout}" in message
    assert "increasing the Session timeout" in message
    assert "optimizing the query" in message
    assert 'Session(endpoint, timeout="30s")' in message
    assert f"Error code: {ERR_QUERY_TIMEOUT}" in message
    assert "Error code: 2001" not in message
