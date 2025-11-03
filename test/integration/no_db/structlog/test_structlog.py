from test.integration.no_db.structlog.structlog_utils import configure_structlog

import pytest
import structlog


def test_structlog_success():
    configure_structlog(__file__)
    structlog.get_logger().exception("Simulated exception")


def test_structlog_exception():
    structlog.configure(processors=[structlog.processors.CallsiteParameterAdder()])
    with pytest.raises(AttributeError):
        structlog.get_logger().exception("Simulated exception")
