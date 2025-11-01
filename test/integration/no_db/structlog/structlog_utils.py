from pathlib import Path
from test.integration.no_db.udf_communication.peer_communication.conditional_method_dropper import (
    ConditionalMethodDropper,
)

import structlog
from structlog import WriteLoggerFactory
from structlog.processors import CallsiteParameter
from structlog.tracebacks import ExceptionDictTransformer

CALLSITE_PARAMETER_ADDER_PARAMETERS = {
    CallsiteParameter.FILENAME,
    CallsiteParameter.FUNC_NAME,
    CallsiteParameter.LINENO,
    CallsiteParameter.MODULE,
    CallsiteParameter.PATHNAME,
    CallsiteParameter.PROCESS,
    CallsiteParameter.PROCESS_NAME,
    # CallsiteParameter.QUAL_NAME, #  Requires Python 3.11+
    CallsiteParameter.THREAD,
    CallsiteParameter.THREAD_NAME,
}


def configure_structlog():
    structlog.configure(
        context_class=dict,
        logger_factory=WriteLoggerFactory(
            file=Path(__file__).with_suffix(".log").open("wt")
        ),
        processors=[
            structlog.contextvars.merge_contextvars,
            ConditionalMethodDropper(method_name="debug"),
            ConditionalMethodDropper(method_name="info"),
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="ISO"),
            structlog.processors.ExceptionRenderer(
                exception_formatter=ExceptionDictTransformer(locals_max_string=320)
            ),
            structlog.processors.CallsiteParameterAdder(
                CALLSITE_PARAMETER_ADDER_PARAMETERS
            ),
            structlog.processors.JSONRenderer(),
        ],
    )
