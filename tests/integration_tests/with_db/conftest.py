from dataclasses import dataclass

import pyexasol
import pytest
from exasol.python_extension_common.deployment.language_container_builder import (
    LanguageContainerBuilder,
    find_path_backwards,
)
from exasol.python_extension_common.deployment.temp_schema import temp_schema

from exasol.analytics.query_handler.deployment.slc import (
    LANGUAGE_ALIAS,
    custom_slc_builder,
)


@pytest.fixture(scope="session")
def language_alias():
    return LANGUAGE_ALIAS


@pytest.fixture(scope="session")
def slc_builder(use_onprem, use_saas) -> LanguageContainerBuilder:
    """
    Overrides default definition from pytest-exasol-slc.

    This slc_builder uses the default builder of the AAF, defined in
    exasol.analytics.slc.custom_slc_builder and adds
    another wheel and its pip requirements on top.

    In result the SLC will contain AAF and the subproject from directory
    tests/test_package.
    """
    if use_saas or use_onprem:
        test_package = find_path_backwards("tests/test_package", __file__)
        with custom_slc_builder() as builder:
            builder.prepare_flavor(test_package)
            yield builder
    else:
        yield None


@pytest.fixture(scope="session")
def db_schema(pyexasol_connection):
    with temp_schema(pyexasol_connection) as db_schema:
        yield db_schema


@dataclass
class ExaAllColumns:
    connection: pyexasol.ExaConnection
    schema: str

    def query(self, table_name: str) -> dict[str, str]:
        raw = self.connection.execute(
            "SELECT COLUMN_NAME, COLUMN_TYPE FROM EXA_ALL_COLUMNS"
            f" WHERE COLUMN_SCHEMA='{self.schema}'"
            f" AND COLUMN_TABLE='{table_name}'"
        ).fetchall()
        return {c[0]: c[1] for c in raw}


@pytest.fixture
def exa_all_columns(pyexasol_connection, db_schema):
    return ExaAllColumns(pyexasol_connection, db_schema)
