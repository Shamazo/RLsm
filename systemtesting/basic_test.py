import pytest
from client import Client
import grpc
import kv_pb2
import kv_pb2_grpc
import numpy as np

@pytest.fixture
def client():
    client = Client("127.0.0.1:4242")
    return client

def test_simple_put(client):
    # these are magic numbers hard coded into a mockserver
    assert(client.put(42, np.int32(3)) == kv_pb2.StatusCode.Success)
    client.disconnect()


def test_internal_fail_put(client):
    assert(client.put(41, np.int32(3)) == kv_pb2.StatusCode.InternalError)
    client.disconnect()

def test_simple_get(client):
    assert(client.get(42) == (kv_pb2.StatusCode.Success, np.int32(42)))
    client.disconnect()

def test_internal_error_get(client):
    assert(client.get(41) == (kv_pb2.StatusCode.InternalError, None))
    client.disconnect()

def test_no_value_get(client):
    assert(client.get(7) == (kv_pb2.StatusCode.NoValue, None))
    client.disconnect()


