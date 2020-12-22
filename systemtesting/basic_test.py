import pytest
from client import Client

@pytest.fixture
def client():
    c = Client("127.0.0.1", 4243)
    c.connect()
    return c

def test_simple_put(client):
    assert(client.put(1, 3) ==1)

def test_simple_get(client):
    assert(client.put(2, 5) == 1)
    assert(client.get(2) == 5)


