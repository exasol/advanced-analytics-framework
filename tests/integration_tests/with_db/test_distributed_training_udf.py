import json
import textwrap
from typing import Tuple, List

import pyexasol
import pytest

from exasol_advanced_analytics_framework.udf_communication.ip_address import Port, IPAddress
from exasol_advanced_analytics_framework.udf_communication.udf_communicator import UDFCommunicatorConfig
from tests.test_package.test_query_handlers.query_handler_test import \
    FINAL_RESULT, QUERY_LIST, TEST_INPUT
from tests.utils.parameters import db_params

MY_CONN = "my_conn"


def test(
        setup_database, pyexasol_connection, upload_language_container):
    bucketfs_connection_name, schema_name = setup_database
    config = UDFCommunicatorConfig(
        listen_port=Port(port=16789),
        multi_node_discovery_ip=IPAddress(ip_address="127.0.0.1", network_prefix=8),
        group_identifier_suffix="test",
        number_of_instances_per_node=2
    )

    pyexasol_connection.execute(f"""
    CREATE OR REPLACE CONNECTION {MY_CONN} TO '{config.json()}';
    """)
    udf = textwrap.dedent(f"""
--/
CREATE OR REPLACE  PYTHON3_AAF SET SCRIPT {schema_name}.train_model(i INTEGER, x DOUBLE, y DOUBLE)
EMITS (i INTEGER, loss DOUBLE) AS
from exasol_advanced_analytics_framework.tensorflow.distributed_training_udf import DistributedTrainingUDF

def run(ctx):
    distributed_training_udf = DistributedTrainingUDF()
    distributed_training_udf.run(ctx=ctx, exa=exa, connection_name="{MY_CONN}")
/
    """)
    pyexasol_connection.execute(udf)
    rs = pyexasol_connection.execute(
        f"""
        SELECT {schema_name}.train_model(i, x, y) 
        FROM (
            SELECT 1 as i, x, x*x as y FROM VALUES BETWEEN 1 AND 100 as v(x)
            UNION ALL
            SELECT 2 as i, x, x*x as y FROM VALUES BETWEEN 101 AND 200 as v(x)
        )
        GROUP BY i
        """
    )
    for row in rs:
        print(row)
