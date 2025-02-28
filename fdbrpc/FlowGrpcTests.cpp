/**
 * FlowGrpcTests.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <sstream>
#ifdef FLOW_GRPC_ENABLED
#include <chrono>
#include <thread>
#include <mutex>
#include "fdbrpc/FlowGrpc.h"
#include "fdbrpc/FlowGrpcTests.h"
#include "fdbrpc/FileTransfer.h"
#include "flow/UnitTest.h"
#include "flow/flow.h"

// So that tests are not optimized out. :/
void forceLinkGrpcTests2() {}

namespace fdbrpc_test {

void generate_random_string(std::string* buffer, int size) {
	buffer->clear();
	const std::string characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
	for (int i = 0; i < size; ++i) {
		buffer->push_back(characters[deterministicRandom()->randomInt(0, characters.size())]);
	}
}

TEST_CASE("/fdbrpc/grpc/basic_coro") {
	NetworkAddress addr(NetworkAddress::parse("127.0.0.1:50500"));
	GrpcServer server(addr);
	server.registerService(make_shared<TestEchoServiceImpl>());
	Future<Void> _ = server.run();
	co_await server.onRunning();

	auto pool = make_shared<AsyncTaskExecutor>(4);
	AsyncGrpcClient<TestEchoService> client(addr.toString(), pool);

	try {
		EchoRequest request;
		request.set_message("Ping!");
		EchoResponse response = co_await client.call(&TestEchoService::Stub::Echo, request);
		std::cout << "Echo received: " << response.message() << std::endl;
		ASSERT_EQ(response.message(), "Echo: Ping!");
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_grpc_error);
		ASSERT(false);
	}
}

TEST_CASE("/fdbrpc/grpc/basic_stream_server") {
	NetworkAddress addr(NetworkAddress::parse("127.0.0.1:50501"));
	GrpcServer server(addr);
	server.registerService(make_shared<TestEchoServiceImpl>());
	Future<Void> _ = server.run();
	co_await server.onRunning();

	auto pool = make_shared<AsyncTaskExecutor>(4);
	AsyncGrpcClient<TestEchoService> client(addr.toString(), pool);

	int count = 0;
	try {
		EchoRequest request;
		request.set_message("Ping!");
		auto stream = client.call(&TestEchoService::Stub::EchoRecvStream10, request);
		while (true) {
			auto response = co_await stream;
			ASSERT_EQ(response.message(), "Echo: Ping!");
			count += 1;
		}
	} catch (Error& e) {
		if (e.code() == error_code_end_of_stream) {
			ASSERT_EQ(count, 10); // Should send 10 reponses.
			co_return;
		}
		ASSERT(false);
	}
	co_return;
}

TEST_CASE("/fdbrpc/grpc/future_destroy") {
	NetworkAddress addr(NetworkAddress::parse("127.0.0.1:50502"));
	GrpcServer server(addr);
	server.registerService(make_shared<TestEchoServiceImpl>());
	Future<Void> _ = server.run();
	co_await server.onRunning();

	auto pool = make_shared<AsyncTaskExecutor>(4);
	AsyncGrpcClient<TestEchoService> client(addr.toString(), pool);

	try {
		EchoRequest request;
		request.set_message("Ping!");
		{
			auto w = client.call(&TestEchoService::Stub::Echo, request);
			// Out-of-scope
		}
	} catch (Error& e) {
		ASSERT(false);
	}
	co_await delay(1); // So that lifetime of client stays.
	co_return;
}

TEST_CASE("/fdbrpc/grpc/stream_destroy") {
	NetworkAddress addr(NetworkAddress::parse("127.0.0.1:50503"));
	GrpcServer server(addr);
	server.registerService(make_shared<TestEchoServiceImpl>());
	Future<Void> _ = server.run();
	co_await server.onRunning();

	auto pool = make_shared<AsyncTaskExecutor>(4);
	AsyncGrpcClient<TestEchoService> client(addr.toString(), pool);

	int count = 0;
	try {
		EchoRequest request;
		request.set_message("Ping!");
		{
			auto stream = client.call(&TestEchoService::Stub::EchoRecvStream10, request);
			auto response = co_await stream;
			ASSERT_EQ(response.message(), "Echo: Ping!");
		}
		// TODO: Test if server cancels.
	} catch (Error& e) {
		if (e.code() == error_code_end_of_stream) {
			ASSERT_EQ(count, 10); // Should send 10 reponses.
			co_return;
		}
		ASSERT(false);
	}
	co_return;
}

// T_EST_CASE("/fdbrpc/grpc/basic_stream_client") {
// 	NetworkAddress addr(NetworkAddress::parse("127.0.0.1:50502"));
// 	GrpcServer server(addr);
// 	server.registerService(make_shared<TestEchoServiceImpl>());
// 	Future<Void> _ = server.run();

// 	shared_ptr<asio::thread_pool> pool = make_shared<asio::thread_pool>(4);
// 	AsyncGrpcClient<TestEchoService> client(addr.toString(), pool);

// 	int count = 0;
// 	try {
// 		EchoRequest request;
// 		request.set_message("Ping!");
// 		auto stream = client.call(&TestEchoService::Stub::EchoSendStream10, request);
// 		while (true) {
// 			auto response = co_await stream;
// 		    ASSERT_EQ(response.message(), "Echo: Ping!");
// 			count += 1;
// 		}
// 	} catch (Error& e) {
// 		if (e.code() == error_code_end_of_stream) {
// 			ASSERT_EQ(count, 10); // Should send 10 reponses.
// 			co_return;
// 		}
// 		ASSERT(false);
// 	}
// 	co_return;
// }

int WriteTestFile(platform::TmpFile* file, int size) {
	std::cout << "Writing random to source file of size = " << size << " bytes\n";
	int block_size = 1024 * 1024 * 4; // 4MB at a time.
	int iterations = size / block_size;
	int bytes_written = 0;
	std::string buffer;
	buffer.reserve(block_size + 1);
	for (int i = 0; i <= iterations; ++i) {
		int to_write = block_size;
		if (i == iterations && size % block_size == 0) {
			break;
		} else if (i == iterations) {
			to_write = size % block_size;
		}
		generate_random_string(&buffer, to_write);
		file->append((const uint8_t*)buffer.c_str(), to_write);
		bytes_written += to_write;
	}
	std::cout << "Finished writing " << bytes_written << " bytes.\n";
	ASSERT_EQ(bytes_written, size);
	return bytes_written;
}

const int GPRC_FILE_TRANSFER_TEST_FILE_SIZE = 1024 * 1024 * 40;

TEST_CASE("/fdbrpc/grpc/file_transfer") {
	using platform::TmpFile;

	// -- Server --
	std::string server_address("127.0.0.1:50504");
	FileTransferServiceImpl service;

	grpc::ServerBuilder builder;
	builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
	builder.RegisterService(&service);

	std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
	std::cout << "Server listening on " << server_address << std::endl;

	// -- Client --
	auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
	auto client = FileTransferClient(channel);
	TmpFile src;
	TmpFile dest;
	int bytes_written = WriteTestFile(&src, GPRC_FILE_TRANSFER_TEST_FILE_SIZE);

	// -- Start file transfer --
	std::cout << "Invoking gRPC File Transfer call.\n";
	auto start = std::chrono::high_resolution_clock::now();
	auto res = client.DownloadFile(src.getFileName(), dest.getFileName());
	std::cout << "Src = " << src.getFileName() << ", Dest = " << dest.getFileName() << "\n";
	auto end = std::chrono::high_resolution_clock::now();
	auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
	std::cout << "Time taken: " << diff.count() << std::endl;
	ASSERT(res.has_value());
	std::cout << "Bytes downloaded: " << res.value() << std::endl;
	ASSERT_EQ(res.value(), bytes_written);
	src.destroyFile();
	dest.destroyFile();

	server->Shutdown();
	return Void();
}

TEST_CASE("/fdbrpc/grpc/file_transfer_byte_flip") {
	using platform::TmpFile;

	// -- Server --
	std::string server_address("127.0.0.1:50505");
	FileTransferServiceImpl service;
	service.SetErrorInjection(FileTransferServiceImpl::FLIP_BYTE);

	grpc::ServerBuilder builder;
	builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
	builder.RegisterService(&service);

	std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
	std::cout << "Server listening on " << server_address << std::endl;

	// -- Client --
	auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
	auto client = FileTransferClient(channel);
	TmpFile src;
	TmpFile dest;
	std::cout << "Writing 1GB random bytes to source file.\n";
	WriteTestFile(&src, GPRC_FILE_TRANSFER_TEST_FILE_SIZE);

	// -- Start file transfer --
	std::cout << "Invoking gRPC File Transfer call.\n";
	auto res = client.DownloadFile(src.getFileName(), dest.getFileName());
	std::cout << "Src = " << src.getFileName() << ", Dest = " << dest.getFileName() << "\n";
	ASSERT(!res.has_value());
	src.destroyFile();
	dest.destroyFile();

	server->Shutdown();
	return Void();
}

TEST_CASE("/fdbrpc/grpc/file_transfer_fail_random") {
	using platform::TmpFile;

	// -- Server --
	std::string server_address("127.0.0.1:50506");
	FileTransferServiceImpl service;
	service.SetErrorInjection(FileTransferServiceImpl::FAIL_RANDOMLY);

	grpc::ServerBuilder builder;
	builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
	builder.RegisterService(&service);

	std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
	std::cout << "Server listening on " << server_address << std::endl;

	// -- Client --
	auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
	auto client = FileTransferClient(channel);
	TmpFile src;
	TmpFile dest;
	WriteTestFile(&src, GPRC_FILE_TRANSFER_TEST_FILE_SIZE);

	// -- Start file transfer --
	std::cout << "Invoking gRPC File Transfer call.\n";
	auto res = client.DownloadFile(src.getFileName(), dest.getFileName());
	std::cout << "Src = " << src.getFileName() << ", Dest = " << dest.getFileName() << "\n";
	ASSERT(!res.has_value());
	src.destroyFile();
	dest.destroyFile();

	server->Shutdown();
	return Void();
}

TEST_CASE("/fdbrpc/grpc/basic_thread_pool") {
	using namespace std;

	AsyncTaskExecutor pool(4);
	unordered_map<string, size_t> state;
	mutex m;

	Future<string> f = pool.post([&]() -> string {
		this_thread::sleep_for(chrono::seconds(1));
		auto id = std::hash<std::thread::id>{}(std::this_thread::get_id());
		lock_guard<mutex> lg(m);
		state["thread"] = id;
		return "done";
	});
	{
		auto id = std::hash<std::thread::id>{}(std::this_thread::get_id());
		lock_guard<mutex> lg(m);
		state["main"] = id;
	}

	auto r = co_await f;
	ASSERT(state["main"] != state["thread"]);
	ASSERT_EQ(r, "done");
}

TEST_CASE("/fdbrpc/grpc/server_lifecycle_basic") {
	// Server
	NetworkAddress addr(NetworkAddress::parse("127.0.0.1:50507"));
	GrpcServer server(addr);

	std::cout << "Start gRPC server\n";
	Future<Void> _ = server.run();

	std::cout << "Register TestEchoService\n";
	server.registerService(make_shared<TestEchoServiceImpl>());
	co_await server.onNextStart();
	ASSERT(server.hasStarted());
	ASSERT_EQ(server.numStarts(), 1);

	std::cout << "Deregister TestEchoService\n";
	co_await server.deregisterRoleServices(UID());
	ASSERT(!server.hasStarted());

	std::cout << "Register FileTransferService\n";
	server.registerService(make_shared<FileTransferServiceImpl>());
	co_await server.onNextStart();
	ASSERT(server.hasStarted());
	ASSERT_EQ(server.numStarts(), 2);

	// Client
	auto pool = make_shared<AsyncTaskExecutor>(1);
	AsyncGrpcClient<TestEchoService> client(addr.toString(), pool);

	auto make_request = [&]() {
		EchoRequest request;
		request.set_message("Ping!");
		return client.call(&TestEchoService::Stub::Echo, request);
	};

	std::cout << "Make EchoRequest to server without service\n";
	try {
		co_await make_request();
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_grpc_error); // Service was deregistered.
	}

	std::cout << "Register EchoService\n";
	server.registerService(make_shared<TestEchoServiceImpl>());
	co_await server.onNextStart();
	ASSERT_EQ(server.numStarts(), 3);
	std::cout << "Make EchoRequest to server withservice\n";
	try {
		auto res = co_await make_request();
		ASSERT_EQ(res.message(), "Echo: Ping!");
	} catch (Error& e) {
		ASSERT(false);
	}
}

TEST_CASE("/fdbrpc/grpc/server_lifecycle_combine_register_into_one_start") {
	// Server
	NetworkAddress addr(NetworkAddress::parse("127.0.0.1:50508"));
	GrpcServer server(addr);

	Future<Void> _ = server.run();

	std::cout << "Register TestEchoService\n";
	server.registerService(make_shared<TestEchoServiceImpl>());
	co_await delay(0.1);
	server.registerService(make_shared<FileTransferServiceImpl>());
	co_await server.onNextStart();
	ASSERT(server.hasStarted());
	ASSERT_EQ(server.numStarts(), 1);

	// Client
	auto pool = make_shared<AsyncTaskExecutor>(1);
	AsyncGrpcClient<TestEchoService> client(addr.toString(), pool);

	try {
		EchoRequest request;
		request.set_message("Ping!");
		co_await client.call(&TestEchoService::Stub::Echo, request);
	} catch (Error& e) {
		ASSERT(false);
	}
}

//--- THREAD POOL TESTS --//

} // namespace fdbrpc_test

#endif
