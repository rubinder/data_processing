import pytest

from trino_deployment import client


@pytest.fixture(scope="session")
def conn():
    if not client.reachable():
        pytest.skip("Trino not reachable on localhost:8085; ./deploy.sh up first")
    conn = client.connect()
    yield conn
    conn.close()
