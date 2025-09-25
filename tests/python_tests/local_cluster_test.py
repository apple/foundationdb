# client.py
import grpc
import mapi_service_pb2
import mapi_service_pb2_grpc

def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = mapi_service_pb2_grpc.MapiServiceStub(channel)

    response = stub.Hello(mapi_service_pb2.HelloRequest(name='Alice'))
    print("Server replied:", response.message)

if __name__ == '__main__':
    run()
