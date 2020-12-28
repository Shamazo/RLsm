#!/usr/bin/env python3

import logging
import grpc
import kv_pb2
import kv_pb2_grpc
import numpy as np


class Client():
    def __init__(self, endpoint: str):
        self.endpoint = endpoint
        self.channel = grpc.insecure_channel(endpoint)
        self.stub = kv_pb2_grpc.kvStub(self.channel)
        self.connected = True

    def disconnect(self):
        self.channel.close()
        self.connected = False

    def get(self, key: np.int32):
        if (not self.connected):
            raise RuntimeError("Not connected to server!")

        pb_key = kv_pb2.Key()
        pb_key.int_key = key
        request = kv_pb2.GetRequest(key=pb_key)
        response = self.stub.Get(request)
        print(f"reposns satete {response.status_code}")
        if response.status_code == kv_pb2.StatusCode.Success:
            val = np.frombuffer(response.value, dtype=np.int32)[0]
            return (response.status_code, val)
        else:
            logging.warning(f"Got non success status code with get request for key: {key}. code: {response.status_code}")
            return (response.status_code, None)

    def put(self, key: np.int32, value: np.int32) -> bool:
        if (not self.connected):
            raise RuntimeError("Not connected to server!")
        pb_key = kv_pb2.Key()
        pb_key.int_key = key

        request = kv_pb2.PutRequest(key=pb_key, value=np.ndarray.tobytes(np.array(value)))
        response = self.stub.Put(request)
        if response.status_code == kv_pb2.StatusCode.Success:
            return response.status_code
        else:
            logging.warning(f"Got non success status code with put request for key: {key}. code: {response.status_code}")
            return response.status_code




