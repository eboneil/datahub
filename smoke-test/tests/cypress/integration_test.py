import pytest
import subprocess
import os

from tests.utils import ingest_file_via_rest
from tests.utils import delete_urns_from_file


def ingest_data():
    print("ingesting test data")
    ingest_file_via_rest("tests/cypress/data.json")
    ingest_file_via_rest("tests/cypress/cypress_dbt_data.json")
    ingest_file_via_rest("tests/cypress/schema-blame-data.json")
    # acryl-main-data is for data specific to tests in the acryl-main-branch to avoid merge conflicts with OSS
    ingest_file_via_rest("tests/cypress/acryl-main-data.json")


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data():
    ingest_data()
    yield
    print("removing test data")
    delete_urns_from_file("tests/cypress/data.json")
    delete_urns_from_file("tests/cypress/cypress_dbt_data.json")
    delete_urns_from_file("tests/cypress/schema-blame-data.json")
    delete_urns_from_file("tests/cypress/acryl-main-data.json")


def test_run_cypress(frontend_session, wait_for_healthchecks):
    # Run with --record option only if CYPRESS_RECORD_KEY is non-empty
    record_key = os.getenv("CYPRESS_RECORD_KEY")
    if record_key:
        print('Running Cypress tests with recording')
        command = "NO_COLOR=1 npx cypress run --record"
    else:
        print('Running Cypress tests without recording')
        # command = "NO_COLOR=1 npx cypress --version"
        command = "NO_COLOR=1 npx cypress run"
        # Add --headed --spec '**/mutations/mutations.js' (change spec name)
        # in case you want to see the browser for debugging
    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd="tests/cypress")
    stdout = proc.stdout.read()
    stderr = proc.stderr.read()
    return_code = proc.wait()
    print(stdout.decode("utf-8"))
    print('stderr output:')
    print(stderr.decode("utf-8"))
    print('return code', return_code)
    assert(return_code == 0)
