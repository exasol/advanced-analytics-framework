
pytest_plugins = [
    "tests.fixtures.database_connection_fixture",
    "tests.fixtures.build_language_container_fixture",
    "tests.fixtures.upload_language_container_fixture",
    "tests.fixtures.setup_database_fixture",
    "tests.unit_tests.event_handler.fixtures"
]
