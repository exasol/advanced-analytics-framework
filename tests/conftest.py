# the commented out plugins are obsolete after removing
# tests/deployment/test_scripts_deployer.py and
# tests/deployment/test_scripts_deployer_cli.py

pytest_plugins = [
    # "tests.integration_tests.with_db.fixtures.build_language_container_fixture",
    # "tests.integration_tests.with_db.fixtures.upload_language_container_fixture",
    "tests.integration_tests.with_db.fixtures.setup_database_fixture",
    "tests.unit_tests.query_handler.fixtures"
]
