import pytest
from client import Client

@pytest.fixture
def client():
    c = Client("127.0.0.1", 4242)
    c.connect()
    return c

def test_simple_put(client):
    assert(client.put(42, 3))
    assert(not client.put(41, 3))
    client.disconnect()

def test_simple_get(client):
    assert(not client.get(2))
    assert(client.get(42) == 42)
    client.disconnect()


