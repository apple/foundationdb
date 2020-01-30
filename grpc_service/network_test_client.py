#!/usr/bin/env python3

import grpc
import sys
import threading
import time

import network_test_pb2
import network_test_pb2_grpc

def run(replySize, counter):
    with grpc.insecure_channel('localhost:50051') as channel:
        service = network_test_pb2_grpc.NetworkTestStub(channel,)
        while True:
            response = service.GetValue(network_test_pb2.NetworkTestRequest(key=b'.', replySize=replySize))
            counter.count += 1

class Counter:
    def __init__(self):
        self.count = 0

if __name__ == '__main__':
    global sent
    if len(sys.argv) == 1:
        replySize = 600000
    else:
        replySize = int(sys.argv[1])

    threads = []
    counters = []
    num_threads = 30
    for i in range(num_threads):
        counters.append(Counter())
        threads.append(threading.Thread(target=run, args=(replySize,counters[i])))
        threads[i].start()

    last_time = time.time()
    while True:
        time.sleep(1)
        now = time.time()
        speed = sum([c.count for c in counters]) / (now - last_time)
        for c in counters:
            c.count = 0
        last_time = now
        print('messages per second: %f' % speed)

        
    for i in range(num_threads):
        threads[i].join()
