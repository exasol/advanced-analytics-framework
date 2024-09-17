pytest_plugins = [
    "tests.integration_tests.with_db.fixtures.database_connection_fixture",
    # "tests.integration_tests.with_db.fixtures.build_language_container_fixture", # obsolete
    # "tests.integration_tests.with_db.fixtures.upload_language_container_fixture", # obsolete
    "tests.integration_tests.with_db.fixtures.slc_fixtures",
    "tests.integration_tests.with_db.fixtures.setup_database_fixture",
    "tests.unit_tests.query_handler.fixtures"
]
