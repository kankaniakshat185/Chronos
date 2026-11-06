"""Structured logging (P7.2): one JSON object per line, with the active context."""

import json
import logging

import pytest

from chronos.common.logs import JsonFormatter, log_context


def test_json_lines_carry_the_context(caplog: pytest.LogCaptureFixture) -> None:
    caplog.handler.setFormatter(JsonFormatter())
    log = logging.getLogger("chronos.controllers.scheduler")
    with caplog.at_level(logging.INFO), log_context(node="n1"):
        with log_context(controller="scheduler", key="web-1", trace_id="scheduler-7"):
            log.info("pod %s: bound to %s", "web-1", "n1")
        log.info("outside the reconcile")
    inner, outer = (json.loads(line) for line in caplog.text.splitlines())
    assert inner["component"] == "chronos.controllers.scheduler"
    assert inner["msg"] == "pod web-1: bound to n1"
    assert (inner["node"], inner["controller"], inner["trace_id"]) == (
        "n1",
        "scheduler",
        "scheduler-7",
    )
    assert outer["node"] == "n1" and "trace_id" not in outer  # the reconcile's context ended
