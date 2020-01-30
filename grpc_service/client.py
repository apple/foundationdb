#!/usr/bin/env python3

import grpc
import sys

import kv_service_pb2
import kv_service_pb2_grpc

def run(key):
    with grpc.insecure_channel('localhost:50051') as channel:
        service = kv_service_pb2_grpc.FdbKvClientStub(channel,)
        response = service.GetValue(kv_service_pb2.GetValueRequest(key=key))

    print('Got key %r: %r' % (key, response.value))

if __name__ == '__main__':
    if len(sys.argv) == 1:
        key = b'foo'
    else:
        key = bytes(sys.argv[1], 'utf-8')

    run(key)
