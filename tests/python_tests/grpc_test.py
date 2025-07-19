#!/usr/bin/python
#
# grpc_test.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# python -m grpc_tools.protoc   --proto_path=/root/src/foundationdb/fdbcli_lib/protos   --python_out=/root/src/foundationdb/tests/python_tests/   --grpc_python_out=/root/src/foundationdb/tests/python_tests/   cli_service.proto

import subprocess
import signal
import sys
import os
import random
import time

import grpc
import cli_service_pb2
import cli_service_pb2_grpc

FDB_CLUSTER_SCRIPT = "/root/src/foundationdb/tests/loopback_cluster/run_custom_cluster.sh"
FDB_BUILD_DIRECTORY = "/root/build_output"

# Track the shell subprocess
shell_process = None

def kill_fdbserver_processes():
    """Kill all processes named fdbserver."""
    try:
        subprocess.run(["pkill", "-9", "fdbserver"], check=True)
        print("Killed all fdbserver processes.")
    except subprocess.CalledProcessError:
        # print("No fdbserver processes found or failed to kill.")
        pass

def handle_exit(signum=None, frame=None):
    print("\nExiting. Cleaning up...")
    kill_fdbserver_processes()
    if shell_process and shell_process.poll() is None:
        shell_process.terminate()
    sys.exit(0)

def test_get_workers():
    print("GetWorkers:")
    channel = grpc.insecure_channel('127.0.0.1:2501')
    stub = cli_service_pb2_grpc.CliServiceStub(channel)
    response = stub.GetWorkers(cli_service_pb2.GetWorkersRequest())
    print("Server replied:", response)

def test_get_coordinators():
    print("GetCoordinators:")
    channel = grpc.insecure_channel('127.0.0.1:2501')
    stub = cli_service_pb2_grpc.CliServiceStub(channel)
    response = stub.GetCoordinators(cli_service_pb2.GetCoordinatorsRequest())
    print("Server replied:", response)

def test_change_coordinators():
    print("ChangeCoordinators:")
    channel = grpc.insecure_channel('127.0.0.1:2501')
    stub = cli_service_pb2_grpc.CliServiceStub(channel)

    req = cli_service_pb2.ChangeCoordinatorsRequest()
    # req.automatic_coordinators = True
    req.new_coordinator_addresses.append("127.0.0.1:1501")
    req.new_coordinator_addresses.append("127.0.0.1:1516")
    response = stub.ChangeCoordinators(req)
    print("Server replied:", response)

def test_get_version():
    print("GetReadVersion:")
    channel = grpc.insecure_channel('127.0.0.1:2501')
    stub = cli_service_pb2_grpc.CliServiceStub(channel)

    req = cli_service_pb2.GetReadVersionRequest()
    response = stub.GetReadVersion(req)
    print("Read Version:", response.version)

def test_exclude_include():
    print("Testing Exclude/Include functionality:")
    channel = grpc.insecure_channel('127.0.0.1:2501')
    stub = cli_service_pb2_grpc.CliServiceStub(channel)
    
    # Get initial list of workers
    print("Getting initial worker list...")
    response = stub.GetWorkers(cli_service_pb2.GetWorkersRequest())
    initial_workers = [worker.address for worker in response.workers]
    print(f"Initial workers: {initial_workers}")
    
    if len(initial_workers) == 0:
        print("No workers found, skipping exclude/include test")
        return
    
    # Pick a random worker to exclude. Make sure its not a coordinator.
    worker_to_exclude = random.choice(initial_workers)
    print(f"Excluding worker: {worker_to_exclude}")
    
    # Exclude the worker
    exclude_req = cli_service_pb2.ExcludeRequest()
    exclude_req.addresses.append(worker_to_exclude)
    exclude_req.no_wait = True  # Don't wait for exclusion to complete
    
    try:
        exclude_response = stub.Exclude(exclude_req)
        print(f"Successfully excluded {worker_to_exclude}")
    except grpc.RpcError as e:
        print(f"Failed to exclude {worker_to_exclude}: {e}")
        return
    
    # Wait a bit for the exclusion to take effect
    time.sleep(2)
    
    # Verify the worker was excluded (note: worker might still appear but be marked as excluded)
    while True:
        # Get updated worker list to verify exclusion
        print("Getting worker list after exclusion...")
        response = stub.GetWorkers(cli_service_pb2.GetWorkersRequest())
        excluded_workers = [worker.address for worker in response.workers]
        print(f"Workers after exclusion: {excluded_workers}")

        if worker_to_exclude in excluded_workers:
            print(f"Note: {worker_to_exclude} still appears in worker list (may be marked as excluded)")
            continue

        print(f"Confirmed: {worker_to_exclude} removed from worker list")
        break
    
    # Include the worker back
    print(f"Including worker back: {worker_to_exclude}")
    try:
        include_req = cli_service_pb2.IncludeRequest()
        include_req.addresses.append(worker_to_exclude)
        include_response = stub.Include(include_req)
        print(f"Successfully included {worker_to_exclude}")
    except grpc.RpcError as e:
        print(f"Failed to include {worker_to_exclude}: {e}")
        return
    
    # Wait a bit for the inclusion to take effect
    time.sleep(2)
    
    # Get final worker list to verify inclusion
    print("Getting final worker list after inclusion...")
    response = stub.GetWorkers(cli_service_pb2.GetWorkersRequest())
    final_workers = [worker.address for worker in response.workers]
    print(f"Final workers: {final_workers}")
    
    # Verify the worker was included back
    if worker_to_exclude in final_workers:
        print(f"Confirmed: {worker_to_exclude} successfully included back")
    else:
        print(f"Warning: {worker_to_exclude} not found in final worker list")
    
    print("Exclude/Include test completed\n")

def main():
    global shell_process

    # Handle signals for graceful shutdown
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    print(f"Running FDB Cluster: {FDB_CLUSTER_SCRIPT}")
    kill_fdbserver_processes()
    time.sleep(1)
    shell_process = subprocess.Popen([FDB_CLUSTER_SCRIPT, FDB_BUILD_DIRECTORY])
    time.sleep(1)

    try:
        shell_process.wait()
        test_get_workers()
        test_get_coordinators()
        test_get_version()
        # test_change_coordinators()
        test_exclude_include()
        time.sleep(1000000)
    except KeyboardInterrupt:
        pass

    handle_exit()

if __name__ == "__main__":
    main()
