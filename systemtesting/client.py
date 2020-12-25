#!/usr/bin/env python3

import socket
import logging
import flatbuffers
import api.GetRequest
import api.PutRequest
import api.GetResponse
import api.PutResponse
import api.Message
import api.Payload
import api.IntData
import api.Keys
import api.Values
import api.ResultType


class Client():
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.connected = False

    def connect(self):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = self.s.connect_ex((self.host, self.port))
        if (result != 0):
            raise OSError(result)
        self.connected = True

    def disconnect(self):
        self.s.close()
        self.connected = False

    def get(self, key):
        if (not self.connected):
            raise RuntimeError("Not connected to server!")

        # build a message and send it to the server
        builder = flatbuffers.Builder(1024)

        api.IntData.IntDataStartDataVector(builder, 1)
        builder.PrependInt32(key)
        key_vec = builder.EndVector(1)
        api.IntData.IntDataStart(builder)
        api.IntData.IntDataAddData(builder, key_vec)
        key_vec = api.IntData.IntDataEnd(builder)

        api.GetRequest.GetRequestStart(builder)
        api.GetRequest.GetRequestAddKeys(builder, key_vec)
        api.GetRequest.GetRequestAddKeysType(builder, api.Keys.Keys.IntData)
        api.GetRequest.GetRequestAddLength(builder, 1)
        get_request = api.GetRequest.GetRequestEnd(builder)

        api.Message.MessageStart(builder)
        api.Message.MessageAddPayload(builder, get_request)
        api.Message.MessageAddPayloadType(builder, api.Payload.Payload.GetRequest)
        api.Message.MessageAddResult(builder, api.ResultType.ResultType.Success)
        message = api.Message.MessageEnd(builder)
        builder.FinishSizePrefixed(message)

        # we send the message and then wait for a response
        self.s.sendall(builder.Output())
        return self._handle_response(api.Payload.Payload.GetResponse)

    def put(self, key, value) -> bool:
        if (not self.connected):
            raise RuntimeError("Not connected to server!")

        # build a message and send it to the server
        builder = flatbuffers.Builder(1024)

        api.IntData.IntDataStartDataVector(builder, 1)
        builder.PrependInt32(key)
        key_vec = builder.EndVector(1)
        api.IntData.IntDataStart(builder)
        api.IntData.IntDataAddData(builder, key_vec)
        key_vec = api.IntData.IntDataEnd(builder)

        api.IntData.IntDataStartDataVector(builder, 1)
        builder.PrependInt32(value)
        val_vec = builder.EndVector(1)
        api.IntData.IntDataStart(builder)
        api.IntData.IntDataAddData(builder, val_vec)
        val_vec = api.IntData.IntDataEnd(builder)

        api.PutRequest.PutRequestStart(builder)
        api.PutRequest.PutRequestAddKeys(builder, key_vec)
        api.PutRequest.PutRequestAddKeysType(builder, api.Keys.Keys.IntData)
        api.PutRequest.PutRequestAddValues(builder, val_vec)
        api.PutRequest.PutRequestAddValuesType(builder, api.Values.Values.IntData)
        api.PutRequest.PutRequestAddLength(builder, 1)
        put_request = api.PutRequest.PutRequestEnd(builder)

        api.Message.MessageStart(builder)
        api.Message.MessageAddPayload(builder, put_request)
        api.Message.MessageAddPayloadType(builder, api.Payload.Payload.PutRequest)
        api.Message.MessageAddResult(builder, api.ResultType.ResultType.Success)
        message = api.Message.MessageEnd(builder)
        builder.FinishSizePrefixed(message)

        # we send the message and then wait for a response
        self.s.sendall(builder.Output())

        return self._handle_response(api.Payload.Payload.PutResponse)


    def _handle_response(self, expected_payload: api.Payload.Payload) -> bool:
        buf = self.s.recv(4)
        while (len(buf) < 4):
            buf += self.s.recv(4)
        length = int.from_bytes(buf[0:4], 'little', signed=False)
        buf = buf[4:]
        while (len(buf) < length):
            buf += self.s.recv(length)

        message = api.Message.Message.GetRootAsMessage(buf, 0)

        logging.warning("Handling response")
        if (message.PayloadType() != expected_payload):
            logging.error(f"received payload was {message.PayloadType()} not the expected {expected_payload}")
            return False

        if (message.Result() != api.ResultType.ResultType.Success):
            logging.warning("Request Failed")
            return False

        if (message.PayloadType() == api.Payload.Payload.GetResponse):
            logging.debug("received GetResponse")
            get_response = api.GetResponse.GetResponse()
            get_response.Init(message.Payload().Bytes, message.Payload().Pos)
            values = api.IntData.IntData()
            #TODO actually care what type is passed here instead of assuming in32
            values.Init(get_response.Values().Bytes, get_response.Values().Pos)
            return values.Data(0)

        elif (message.PayloadType() == api.Payload.Payload.PutResponse):
            logging.debug("received PutResponse")
            return True

if __name__ == "__main__":
    c = Client("127.0.0.1", 87922)


