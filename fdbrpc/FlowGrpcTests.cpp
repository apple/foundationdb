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

#ifdef FLOW_GRPC_ENABLED
#include <ctime>
#include "fdbrpc/FlowGrpc.h"
#include "fdbrpc/FlowGrpcTests.h"
#include "fdbrpc/FileTransfer.h"
#include "flow/UnitTest.h"
#include "flow/flow.h"

// So that tests are not optimized out. :/
void forceLinkGrpcTests2() {}

namespace fdbrpc_test {
namespace asio = boost::asio;

std::string generate_random_string(int size) {
	const std::string characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
	std::string random_string;
	for (int i = 0; i < size; ++i) {
		random_string += characters[deterministicRandom()->randomInt(0, characters.size())];
	}

	return random_string;
}

TEST_CASE("/fdbrpc/grpc/basic_coro") {
	NetworkAddress addr(NetworkAddress::parse("127.0.0.1:50500"));
	GrpcServer server(addr);
	server.registerService(make_shared<TestEchoServiceImpl>());
	Future<Void> _ = server.run();

	shared_ptr<asio::thread_pool> pool = make_shared<asio::thread_pool>(4);
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

	shared_ptr<asio::thread_pool> pool = make_shared<asio::thread_pool>(4);
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

	shared_ptr<asio::thread_pool> pool = make_shared<asio::thread_pool>(4);
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

	shared_ptr<asio::thread_pool> pool = make_shared<asio::thread_pool>(4);
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
	std::cout << "Writing 1GB random bytes to source file.\n";
	int bytes_written = 0;
	for (int i = 0; i < 1024; ++i) {
		const int size = i < 1023 ? 1024 * 1024 : (1024 * 1024) / 2; // Make last block smaller.
		auto rand_str = generate_random_string(size);
		src.append((const uint8_t*)rand_str.c_str(), size);
		bytes_written += size;
	}
	std::cout << "Finished writing " << bytes_written << " bytes.\n";

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
	int bytes_written = 0;
	for (int i = 0; i < 1024; ++i) {
		const int size = i < 1023 ? 1024 * 1024 : (1024 * 1024) / 2; // Make last block smaller.
		auto rand_str = generate_random_string(size);
		src.append((const uint8_t*)rand_str.c_str(), size);
		bytes_written += size;
	}
	std::cout << "Finished writing " << bytes_written << " bytes.\n";

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
	std::cout << "Writing 1GB random bytes to source file.\n";
	int bytes_written = 0;
	for (int i = 0; i < 1024; ++i) {
		const int size = i < 1023 ? 1024 * 1024 : (1024 * 1024) / 2; // Make last block smaller.
		auto rand_str = generate_random_string(size);
		src.append((const uint8_t*)rand_str.c_str(), size);
		bytes_written += size;
	}
	std::cout << "Finished writing " << bytes_written << " bytes.\n";

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

//--- THREAD POOL TESTS --//

} // namespace fdbrpc_test

#endif
