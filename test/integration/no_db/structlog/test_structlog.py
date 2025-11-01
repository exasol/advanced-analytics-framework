from test.integration.no_db.structlog.structlog_utils import configure_structlog

import structlog

configure_structlog()


def test_structlog():
    structlog.get_logger().exception("Simulated exception")
