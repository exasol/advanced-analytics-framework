import pytest
import structlog

from test.integration.no_db.structlog.structlog_utils import configure_structlog


def test_structlog_success():
    configure_structlog(__file__)
    structlog.get_logger().exception("Simulated exception")


def test_structlog_exception():
    structlog.configure(processors=[structlog.processors.CallsiteParameterAdder()])
    logger = structlog.get_logger()
    with pytest.raises(TypeError):
        logger.exception("Simulated exception")
