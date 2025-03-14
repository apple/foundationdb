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
#include <chrono>
#include <thread>
#include <mutex>
#include "fdbrpc/FlowGrpc.h"
#include "fdbrpc/FlowGrpcTests.h"
#include "fdbrpc/FileTransfer.h"
#include "flow/UnitTest.h"
#include "flow/TLSConfig.actor.h"
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
		std::cout << "Error: " << e.name() << std::endl;
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

	// Check Void return.
	{

		Future<Void> f = pool.post([]() -> Void {
			this_thread::sleep_for(100ms);
			return Void();
		});
		co_await f;
	}

	// Check return T where T != Void.
	{
		Future<string> f = pool.post([&]() -> string {
			this_thread::sleep_for(1s);
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

	// Check void return where we don't care about result.
	{
		pool.post([]() noexcept { this_thread::sleep_for(100ms); });
	}

	// Forward Flow errors.
	{
		try {
			co_await pool.post([]() -> Void {
				throw task_interrupted();
				ASSERT(false);
				return Void();
			});
		} catch (Error& e) {
			ASSERT(e.code() == error_code_task_interrupted);
		}
	}

	this_thread::sleep_for(200ms);
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

// Generate key/cert pairs.
// # Generate Root CA
// openssl genpkey -algorithm RSA -out ca.key # Private Key
// openssl req -new -x509 -key ca.key -out ca.crt -days 3650 -subj "/CN=Test Root CA" # Self-signed CA certificate
//
// # Generate Server Certificate
// openssl genpkey -algorithm RSA -out server.key # Private Key
// openssl req -new -key server.key -out server.csr -subj "/CN=localhost" # Certificate Signing Request (CSR)
// openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 3650 -sha256
//
// # Generate Client Certificate (for mTLS)
// openssl genpkey -algorithm RSA -out client.key # Private Key
// openssl req -new -key client.key -out client.csr -subj "/CN=Test Client" # Certificate Signing Request (CSR)
// openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 3650 -sha256
TEST_CASE("/fdbrpc/grpc/basic_tls") {
	struct TLSCreds {
		std::string cert;
		std::string key;
		std::string ca;
	};

	// TODO: Generate certs automatically.
	auto serverCreds = TLSCreds{
		R"(-----BEGIN CERTIFICATE-----
MIICsjCCAZoCFGH6xUKDCe/xv11Ra6F2xKvmd+gUMA0GCSqGSIb3DQEBCwUAMBcx
FTATBgNVBAMMDFRlc3QgUm9vdCBDQTAeFw0yNTAzMTIyMDExNDNaFw0zNTAzMTAy
MDExNDNaMBQxEjAQBgNVBAMMCWxvY2FsaG9zdDCCASIwDQYJKoZIhvcNAQEBBQAD
ggEPADCCAQoCggEBALpsEnX/jRWq6FWa65ywZ1NGXtPVPjaw7oJvs2NSrtsoRaYx
ZCxwbl0nM6tAnKnHoG7BKeRq9jYaumtf9e9437TeRyh3KO3wXgGnmeXshXKIQzx+
Nzif7NDXn2qu4Ak6ywOp163rPJh5v0k+Jai3b6dZnYTECokYk+8/UbDqodevMN/y
Z4NLHY5jWoFx0swai53/3YVSoHH1LSvYXysZmR2TlsXFC3CfNSFu+uzGuwHCzqIn
UbDHao+iW8n5yHQjmEfh41gyVic26fAenpkCtQJufESPx3PgxkWOBAEKIZ2123Sr
NM7A5u4/h56BXHLcBKY5KzbyMLw7CLiKO0NtcXcCAwEAATANBgkqhkiG9w0BAQsF
AAOCAQEAYu0Qr0eN9lnOyWiKQMAjPIMgGtJ85TvYK2tMZbFWYZTM+JZ2fd9lqz8o
H8MwAELYzho6QrMoibErmr7BUjeTXSM9HBVqumomAammmneMy/qh2xsemNp0Hqgk
Sv6bxw8g8SGmzURmiWfzqZV3UOqQDNVS93o0MV/Z9gp2TGsP9aqYaAyUhZv3e9QL
SvOJ93H/fkpJ980FlGIrqe/pwjhDrCPzsT1b2msdolWJgCThNQd49cGB7FDMqe5y
l6cALt6HLWwqC36Cu104eQmC6wjLYySt8wzYpeJb+7DdQb5k5+D/OkK0gwp/+DY/
7ZWe8mDOVuuSUgJOJOxJK2fBxxDlUQ==
-----END CERTIFICATE-----)",
		R"(-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC6bBJ1/40VquhV
muucsGdTRl7T1T42sO6Cb7NjUq7bKEWmMWQscG5dJzOrQJypx6BuwSnkavY2Grpr
X/XveN+03kcodyjt8F4Bp5nl7IVyiEM8fjc4n+zQ159qruAJOssDqdet6zyYeb9J
PiWot2+nWZ2ExAqJGJPvP1Gw6qHXrzDf8meDSx2OY1qBcdLMGoud/92FUqBx9S0r
2F8rGZkdk5bFxQtwnzUhbvrsxrsBws6iJ1Gwx2qPolvJ+ch0I5hH4eNYMlYnNunw
Hp6ZArUCbnxEj8dz4MZFjgQBCiGdtdt0qzTOwObuP4eegVxy3ASmOSs28jC8Owi4
ijtDbXF3AgMBAAECggEAXoSk54bmNXIt0hl2FD+sY6BuO+EGZhxXEXMo9NbbKOrG
RXkNXSq0Ci7yF4Xu6HX6da0iXFmO8+ehVQeWXhBe/Akm8vFXoywXvacIdHbzQhqs
XGNyBBexrkFk5mY72PIiNH1MiTWXx4cS4UPNPDmlNqhNIR90aAY6GzdzZ8vOuteO
tyLm9LRlWUsuu0QxuHkmLFfvpOzVRQG2wiX53302nUB+b0ZnB7CjLiBEo/fUFyy4
cXz6nNhXUdjY5wKoxE9Dn1ng0w8ZEkKemZK2rsRVHs+zNgT+zX2evYOz4Bl0Mi66
/Whlm5GI3BJOb5kVmyonSkVLWBl1JZL1606UQeVrgQKBgQDirQV+I3HCMuk0kK/f
6YckSl1tF/XdkM7x3x9jT3tq/95dNQVNrvESS3huFiEUn1brZcOS8cGvLGhOkI9B
io6NP/pN6rhFleeRWYLLi2A9bsmuQxW8OPDgW3scf8T0j08/9fwRWrGcB0QsSBS+
XweksN0rYeTxq1LQiMEEkuvf5wKBgQDSifD2oQCCi0VTt1plDu5DgWHQsa5Jgadz
Y23pmkrFqC1iXpJkH5053avzZZr0svjJo/S1lFFLgO71lqRfeWA4/YHyVbebJJau
gXU1UYhtYf2a1e4gadaAt3vTqPvMK1DXXzlsdcLkm2Sr2UdRtcbboBVKtS4uN2Wu
UrLASRHv8QKBgQDU9kFm1N4X2cUHwbe64qg5DDTiPdScqWQEEeBnaVFGocXERAAL
kASlb4phwyCpQ1piJmWsNL4HWe1VgrNUnFx5kFYVxiQCHGhHQpWDrhppHSEWO6i8
AbMs1kW/7LqaGdoW+YAsvcyIDruZ7s6uXJHNxOzFyV9TPea8TP8q0O4SmQKBgQDJ
l4tCm6cBWJlpL4c+fK1ntdsTGQ7pE1xjADGeHD68Q+ww5J0Hgb3qSnb6968H3taZ
PIRz2TB+O1aTkUqSoIWiDBsBD1JfGwmszgiMpSUcl1ddiroYDz90wDuwz31KYa9m
iadSUu/qNWCPq9y+QG/Im9/HSPSPDDwwTr8Znt2uAQKBgEYpUTAnBg0ps11Kz3Yd
0eOMc/42CugzvIvubBGaaIwItqA7gotk3l9rrvi5HUFJXTp/xteOi4t7U2chD0qz
1PnvQMK9NG5iAEsP6M4eLLim/Yb83cuE9RBPPdaeXCsjO/qEkWDfhvq3Pg+v9Uqz
0IiSeXnAnliSzXUPGoV8drB+
-----END PRIVATE KEY-----)",
		R"(-----BEGIN CERTIFICATE-----
MIIDDzCCAfegAwIBAgIUYZs4XIcYkT9NaTuVJSRJWFxwqiAwDQYJKoZIhvcNAQEL
BQAwFzEVMBMGA1UEAwwMVGVzdCBSb290IENBMB4XDTI1MDMxMjIwMTE0M1oXDTM1
MDMxMDIwMTE0M1owFzEVMBMGA1UEAwwMVGVzdCBSb290IENBMIIBIjANBgkqhkiG
9w0BAQEFAAOCAQ8AMIIBCgKCAQEA2XAfdLc4APgRqjGI1w1+mi3LCNsC5Ymby/Y0
lsi0L3Kuu5CPPjBeOtf8IKZbDKqRkNWi6KeBI/+BCWBDk+mCRFS8IQ+icTP/ZpGz
lr7efIK0GN37cyYYtftv2F4L9bklsTbGZ0hbVy7wzVggiJzEt0yyiWfU95kxuMuO
zFT7aZshVmvz8MopP/gI0CWpi/bX0Qt0c6kbJL//rhRSD5tO6r6wet1Dgge38KDb
7zddcaR8arlnnGqbw6R8me/bmAlugwA1RXqSlvSz7LqWosuOB54iYivN4aC6TG05
ul/X0fG0fi2w+K+pI1dLHvCDefa7fiHPgvqfdKHYxSafnaHe/wIDAQABo1MwUTAd
BgNVHQ4EFgQUKl6kB14nQDhmQHEOK9030m1Phn8wHwYDVR0jBBgwFoAUKl6kB14n
QDhmQHEOK9030m1Phn8wDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOC
AQEAzWnx7xgCJrJDzxy+H8cl0q86FlUdhGroqdBfswXzLPtLYWKouAhN7thy7bUJ
v+Nn0WE2vFl8wS9244k5U5bQUlmlFyWBFgc7aRiqSyff1teIRI0xX6iuN4KKAYiT
lppeuHDNCi1E1fRGTSIy6Pq07KfQ4jHd574EOGG2P56vYko4JHWph500Y2vj9mnK
Vw31eZuD0KTL0rLubXvwUgSXVw/wKmBTgGQKH9ZDJcxlzr9QDIvVtSDqiQrYFHfH
l8+2ue5YUrcuvaBHuYv7qpYWrU51BJmznyt0QO94E1f1R9YVdfCNnFdlFaIAb6Nr
b1qkb+bH9VO0emLeRx9/hhdbCw==
-----END CERTIFICATE-----)"
	};

	auto clientCreds = TLSCreds{
		R"(-----BEGIN CERTIFICATE-----
MIICtDCCAZwCFGH6xUKDCe/xv11Ra6F2xKvmd+gVMA0GCSqGSIb3DQEBCwUAMBcx
FTATBgNVBAMMDFRlc3QgUm9vdCBDQTAeFw0yNTAzMTIyMDExNDNaFw0zNTAzMTAy
MDExNDNaMBYxFDASBgNVBAMMC1Rlc3QgQ2xpZW50MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEA6yxN7pTFABIURggGPtNUueEqKDRn5AYVJQeBHuKgP58L
MPeAZCIrQQyyu/IP2lBEEw7xpDvTJtwIp/g46D19CVLTTm5ium4dZrl9oQ9sZyCY
e51S9WD3W+zPAzb7WJOGZjm0vdixqwTdL6GNf78DjQ0BrOR5K63M/vF6aI3T5BSx
jspA17bfo2b+GIP1AqAD71wV7QdyS3RrD/k0DaEpd6zmjIUJAg52K909AVpmPmHl
4Q3jWqk2wvzmJKatyVpKGoLgH4zxv7lTGD4jndUExaLDLP8KeV3OeAbPDQ9f24Bz
SHrNKHsroSFFLJsWxhnnl73Vbd7NHvg87mr2DyY9QQIDAQABMA0GCSqGSIb3DQEB
CwUAA4IBAQALO3GMtuccIqAK7bRO33cPWIke9F+O6vOse9uIIadw3atCsG6LPPMQ
DfI8ddKgEbLLz0mQRxGgSFSjrFYBXYWXdOddrvtUZIEuWjY9+/Nj6pydprybLSIL
cT0hO3RkyyA2t7nXBxNSDCDC4i5dMxYS6z3y2puzi1PZw4igBjbA6hQwTo6cJOt/
t6Z2t7Sxis9O7Gf8WVx5oIYB0VJVy8DO1ZEdEY8i566EwCmG2EyZD1aotugtte+e
Rw0VoJwswwMNYhYMiS/RRMardtxuThYvi681yXLvzYDrg4Hv+lGZMifiozB+Aium
IlFaW1sDwY3c6PPdO4ifiuxpGEgElub+
-----END CERTIFICATE-----)",
		R"(-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDrLE3ulMUAEhRG
CAY+01S54SooNGfkBhUlB4Ee4qA/nwsw94BkIitBDLK78g/aUEQTDvGkO9Mm3Ain
+DjoPX0JUtNObmK6bh1muX2hD2xnIJh7nVL1YPdb7M8DNvtYk4ZmObS92LGrBN0v
oY1/vwONDQGs5Hkrrcz+8XpojdPkFLGOykDXtt+jZv4Yg/UCoAPvXBXtB3JLdGsP
+TQNoSl3rOaMhQkCDnYr3T0BWmY+YeXhDeNaqTbC/OYkpq3JWkoaguAfjPG/uVMY
PiOd1QTFosMs/wp5Xc54Bs8ND1/bgHNIes0oeyuhIUUsmxbGGeeXvdVt3s0e+Dzu
avYPJj1BAgMBAAECggEBAIeQ85/LzMBcq6kvKQKAan/4h4fDpfvxcbD14X3/YCcs
tJPWqQGQHOUTv9jOEktkJ8ngvHVTM0ZcYvUJBy0BfRmp0DuPMISNw48a8Iuuy2ur
g5sGtvLSoeUw3qjYwg1uhXRaYZFq4AhY9nRlqZ7xasedrtCOqDcAqoJwi4LH91Sr
aiNGskpuk55bUEbeUqHaT5Zup3euRByju0diIA6ABdmmEBdzHi8TUItfMV5n47EY
LhT/SgfZmCWkkhWRMhedTgZ0bE1sYwfiGT0r34/s8sz224l4uR0228V/x3S2RBKC
CDv4eN2C8Hj3W95zio3cs+dL8S5xOz1sxg0lnb+uKBECgYEA/VUAV6wJ9Rn804v3
Ts22/iXOu/Q5ipkEZ11xi0oDY3X6I7Iw7ssdK75pOZMvrDMxoJ+M67t0G6HhENc+
cdGShKKwVSlVNToGLgnU5BmRA6VWPw/3sCCKvsaq42iDXEsb4nBtBXtpk6JWb6QT
chHT6F1TWzGSjhZ1sI3mVQTPff0CgYEA7aZYaxc+9uB0xEmRohehZQd+Ar0rF+5X
NsMu5qJDV45TkDeb5jZc+Ycru55L/1nvrZhrlMvN83yVUBuBRJ2c5Mhh2vvL80XF
kYyM32dXVQYF3QypjVT4DY5NQckx3TgAsT97U3Mse2Szlf5i7D7AS94apmaJxthf
EkozCXQhXZUCgYAbtTBUZnywUidU+/oQpG9fXMM/y3Z/sjzJEW9ZzL6SIlU5kkPH
2m9WWX0ozvBn5TGIX+sJ3XbVjt8O+Hvb9xAPcbvXlK29JJuIbYrbZ/B0daD5RMXS
fbyvIQuP096KOazTF3jVIKpre1X43/lAgLKst4hmcQWWhN5acSrAIcaQAQKBgEBE
WubjC3EB9DHc31hhYZELvKUK+StolgdGM1nFicaUw75de3h/PRdx2X7MaSrt7GYa
sQU1NEXjbBGUzpl6siIgmm346Aeq16nrw3Dq5nAkx9MmiHejAc3QrM3clfKIIY/N
ZhGENQRNkE9A0wmmUqRxtO1JD3tJqjQtAq5MzHUdAoGBAL/PzcesK980B/LF1zZj
YHK0jToNO/eBzmq1m0dyNFNLD//HmRsLnEWRbRel7WnzWxG9v9y52BgP5L8A8IC6
UpY13mkgCZZknppjHSCvBCUIebKx+mY0lsVyuICG4xhDKia+G13vKv8ak24zsOuD
sUaLXDnl86zauNGS63nyQ8Y9
-----END PRIVATE KEY-----)",
		R"(-----BEGIN CERTIFICATE-----
MIIDDzCCAfegAwIBAgIUYZs4XIcYkT9NaTuVJSRJWFxwqiAwDQYJKoZIhvcNAQEL
BQAwFzEVMBMGA1UEAwwMVGVzdCBSb290IENBMB4XDTI1MDMxMjIwMTE0M1oXDTM1
MDMxMDIwMTE0M1owFzEVMBMGA1UEAwwMVGVzdCBSb290IENBMIIBIjANBgkqhkiG
9w0BAQEFAAOCAQ8AMIIBCgKCAQEA2XAfdLc4APgRqjGI1w1+mi3LCNsC5Ymby/Y0
lsi0L3Kuu5CPPjBeOtf8IKZbDKqRkNWi6KeBI/+BCWBDk+mCRFS8IQ+icTP/ZpGz
lr7efIK0GN37cyYYtftv2F4L9bklsTbGZ0hbVy7wzVggiJzEt0yyiWfU95kxuMuO
zFT7aZshVmvz8MopP/gI0CWpi/bX0Qt0c6kbJL//rhRSD5tO6r6wet1Dgge38KDb
7zddcaR8arlnnGqbw6R8me/bmAlugwA1RXqSlvSz7LqWosuOB54iYivN4aC6TG05
ul/X0fG0fi2w+K+pI1dLHvCDefa7fiHPgvqfdKHYxSafnaHe/wIDAQABo1MwUTAd
BgNVHQ4EFgQUKl6kB14nQDhmQHEOK9030m1Phn8wHwYDVR0jBBgwFoAUKl6kB14n
QDhmQHEOK9030m1Phn8wDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOC
AQEAzWnx7xgCJrJDzxy+H8cl0q86FlUdhGroqdBfswXzLPtLYWKouAhN7thy7bUJ
v+Nn0WE2vFl8wS9244k5U5bQUlmlFyWBFgc7aRiqSyff1teIRI0xX6iuN4KKAYiT
lppeuHDNCi1E1fRGTSIy6Pq07KfQ4jHd574EOGG2P56vYko4JHWph500Y2vj9mnK
Vw31eZuD0KTL0rLubXvwUgSXVw/wKmBTgGQKH9ZDJcxlzr9QDIvVtSDqiQrYFHfH
l8+2ue5YUrcuvaBHuYv7qpYWrU51BJmznyt0QO94E1f1R9YVdfCNnFdlFaIAb6Nr
b1qkb+bH9VO0emLeRx9/hhdbCw==
-----END CERTIFICATE-----)"
	};

	auto clientCredsDiffRoot = TLSCreds{
		R"(-----BEGIN CERTIFICATE-----
MIICtDCCAZwCFEvBIX6p+QGfL6iHL5so0kpiVsy5MA0GCSqGSIb3DQEBCwUAMBcx
FTATBgNVBAMMDFRlc3QgUm9vdCBDQTAeFw0yNTAzMTQxODU5MzhaFw0zNTAzMTIx
ODU5MzhaMBYxFDASBgNVBAMMC1Rlc3QgQ2xpZW50MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAuZAuPS1V9ZMJUyJqr4PcRDS8S2mNmZZdtisfdNiajEuw
Czf8NnyWlcNdIhqgQE3Yjs8lNea8YlGJy3EKJY56l3Otv3uke5zUBBPvMmCcoC08
RDhvxuiKV6ezKr+v9J5h/v9oyAv1DhuMJfONaJiBpsy+wb6sDbxZ1IXMRqZ0F9Kj
LYZvdfNwdDJf4zOWtOhwwAddi9kvH1doUWBhXCW9kmip0SnQFiNR7Ye9ZQsF0MGL
mE5tmdl23YtYq6BJnIz3r332pLpQ7TY23zjjcHN2JhoRQrzorjCFEN5MLRL0YCxA
O/5OtXDccHzcGHJ00B5+4fWsUGNfns5Ab/YkVMpo6QIDAQABMA0GCSqGSIb3DQEB
CwUAA4IBAQCcPftM/lF4ayg2cGRBXK9i5tsb6KMj7uSSt0yyHBH8P6Vt8aQNIOGl
0/SeTeaGitmizP+hAg27wLDrTnhonwT0/05PLlLDVo+Th0fmt8nDaU9msABipiwE
7AMhEQmJBoqBHtNLUTIcnccJ3cBwhanew4EhavJLb9qg6fjXV9Yf46otadoVtc7g
OyYLUfJLKXYkUtqFFPWT1Ko0E45KxUoYXHpQvFbtB4qjr/WgNnD8f9N2M77CTNxa
t1dCQA8s/UStzX1tFgJbe9cS9HSTZlN/iQ5cX9ru/PW1bS8nYxTK1FD0/diV5CN5
GFkfHVhCAS3/qFqyYQmqJF5phbWRGy7F
-----END CERTIFICATE-----)",
		R"(-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC5kC49LVX1kwlT
Imqvg9xENLxLaY2Zll22Kx902JqMS7ALN/w2fJaVw10iGqBATdiOzyU15rxiUYnL
cQoljnqXc62/e6R7nNQEE+8yYJygLTxEOG/G6IpXp7Mqv6/0nmH+/2jIC/UOG4wl
841omIGmzL7BvqwNvFnUhcxGpnQX0qMthm9183B0Ml/jM5a06HDAB12L2S8fV2hR
YGFcJb2SaKnRKdAWI1Hth71lCwXQwYuYTm2Z2Xbdi1iroEmcjPevffakulDtNjbf
OONwc3YmGhFCvOiuMIUQ3kwtEvRgLEA7/k61cNxwfNwYcnTQHn7h9axQY1+ezkBv
9iRUymjpAgMBAAECggEAF+2quMLW2dL7GdhPT5C+X6E5QrQ6yTplnS8UYFnqG/tK
mTe/8zq/pFJL+TDbx08ChO/bSYcWdcsz7KMH/u3PiyYkmpOUE0iW4mu7pZBlwhXz
NozchRbJeCIs7W9Ifk/KLTq8RUedcxNEUESG0J0R4vN2Cb/PqfnGz6vtOpMOV1/e
iev+buETtcZxlNsEQ4JrbmkTOmtLu8+smht6uDJEe0Xh/R3siS4MenHID7CqAZBL
WSZzl3GurMjOW2sh9S8lXp0vPPa4T0qaHqYj9DT+8WRIYk3pu9N0qUVNPu07wPd1
cWy10Ge8vKP4ixgTJWIdWIW0dEGloEzPbBPMM0mgfQKBgQD0wXRYJusDmiMhuQbr
173+k938QhTUO/Mi5uyfvSqDisULHhZF9tqwgzjRE4F+93ZUGheBkKXt6F4S74gH
KD2KjAygge751cC+hCramSYIESsI/hR0Ykx5Bm1VoyqpuvxAY9n3XcskvvkTtsTz
nYcPKDcj2J54bG7ttEPtGCz9nwKBgQDCFpHaZRH6zk0uAoi419BMSDJrBvyp6uRN
1/yPvU3nb82gm6H7/NqejYfEMn72xZhAsDuvi6GskTpiea3kW2BnTJ+Y5UWfL9jJ
OnuVWzoRlnTmkL5DeSRIPWTcW/hD6hDQ9JTuKgKuI9D9/zWKPbswhD8f3qs/xeHr
7LPQFsj8dwKBgQDwblWW/uwghrr6NqN47F8EhUcstdF2R0npwEU0CgSIYbp82Fpy
8jMT8GPQ5sNv8TH5HgR3T5vgYEKBgXHz7fC+eCMzTVBvdi44OOdMA+PbStompEl9
ps6OmOSwmm+fekKwEiadHDMhbsSJPCKvWdDB7/RyrynFDGfP9CRB9VzE6wKBgDjX
FUXpXZee/VlhIptHkNgWOLXBaRN2GAB4JZ4O6ZbC1myXx5udlITknAMoaqhoe8tO
EVy6vtFkAr4+Nl/vJs950C+dzYLuJ0XfW7wE6H+oGXpAn4fxHWAOotAqGw4AqYYY
atlI8ln/YUBRkmN26rAturLhqjNWB3DpdEuXsKrHAoGBAMJdyAP5k6dXvaEirjY+
wlwIrEoUeHCLEBncnPo5LKvqncmeUDJuPWoOlzzZQW6rdsYKyNPwdM8JP41nLdSG
/HTPmg8ZSLMDtReSPqxlTNSzif1/a3A6xqz4LrD3KQAxm2mDRTAUPo07Z0AanyMq
FnVUsGvf3ougWXZMs5PL6kfZ
-----END PRIVATE KEY-----)",
		R"(-----BEGIN CERTIFICATE-----
MIIDDzCCAfegAwIBAgIUYE0u33FGPYmGtIoekRc8GPhZgdswDQYJKoZIhvcNAQEL
BQAwFzEVMBMGA1UEAwwMVGVzdCBSb290IENBMB4XDTI1MDMxNDE4NTkxMVoXDTM1
MDMxMjE4NTkxMVowFzEVMBMGA1UEAwwMVGVzdCBSb290IENBMIIBIjANBgkqhkiG
9w0BAQEFAAOCAQ8AMIIBCgKCAQEAuGajedenzUAOW+vmp64sYnsiashUuBD7jZ0+
0ZUwkk/tZBvu4VP+/MC35p/dQZLyH9PkQkX8flWd5zMepDjmvLU7uJhpb8ZhDzbh
k3PV/UwsTUMyc3eZKgHejc4yTfV9SB663mhh+FfT320MPXab6Mpfa1ZksbbZu/Fi
XePu9LypxIrgGNxIpt1AkEExh/VVJokImaPyMp0OsH1wP2krxybJ/dt5E5YaOSn8
62iG3pUqfSbuOH/5MNOhNKMAldcVUOENbGlL0pH6EgjS5bF86U6z+71L+4oY83jo
xPK6PbbwKuSIENRLfl4wyLna/WBXNtPKKDA7Yjn5vX/9UZtMJQIDAQABo1MwUTAd
BgNVHQ4EFgQUxL6PWSMSGY0A00Y41AuxJHzVvhUwHwYDVR0jBBgwFoAUxL6PWSMS
GY0A00Y41AuxJHzVvhUwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOC
AQEAYUNqgbF54QpK1EVRKBZoZsjrKKnDf3Z/SlSLg7UQ/s1GR7stJRXyxta/ty+B
PZ/xuKZoiPp2aO8ZuOUTLPlgUMlIv0uMJTmyjmYZgES2iol5u9Wfg01WfHlX3Jlm
MO3ZdWmpySmUKSNPw22N/eQqYGMyUhADy8deV39C9NZlQ4F/1LUcbwVyWqtwSatA
Tvge+a6zM/DZAS4V6oKg1Dbues5Pgu1S6IznPFkeWbVNuVs2bFB5pNk6LXEQjQ2s
S70C3DaU2iJgFTuG+ZySAJmLL35jOGH+JUk+c22ZHYw7u1D8kSmlCNX/LzPHFb/D
/d6RoBi35uQcyHM3CnRPrhmT6A==
-----END CERTIFICATE-----)"
	};

	auto provider =
	    std::make_shared<GrpcTlsCredentialStaticProvider>(serverCreds.key, serverCreds.cert, serverCreds.ca);
	ASSERT(provider->validate());

	std::string addr_with_host = "localhost:50509";
	NetworkAddress addr(NetworkAddress::parse("127.0.0.1:50509"));
	GrpcServer server(addr, provider);
	server.registerService(make_shared<TestEchoServiceImpl>());
	Future<Void> _ = server.run();
	co_await server.onRunning();

	auto pool = make_shared<AsyncTaskExecutor>(4);

	// Invoke RPC from client without using any credentials.
	{
		AsyncGrpcClient<TestEchoService> client(addr_with_host, pool);

		try {
			EchoRequest request;
			request.set_message("Ping!");
			EchoResponse response = co_await client.call(&TestEchoService::Stub::Echo, request);
			ASSERT(false);
		} catch (Error& e) {
			ASSERT_EQ(e.code(), error_code_grpc_error);
		}
	}

	// Invoke RPC from client with correct credentials.
	{
		auto provider =
		    std::make_shared<GrpcTlsCredentialStaticProvider>(clientCreds.key, clientCreds.cert, clientCreds.ca);
		AsyncGrpcClient<TestEchoService> client(addr_with_host, provider, pool);

		try {
			EchoRequest request;
			request.set_message("Ping!");
			EchoResponse response = co_await client.call(&TestEchoService::Stub::Echo, request);
			std::cout << "Echo received: " << response.message() << std::endl;
			ASSERT_EQ(response.message(), "Echo: Ping!");
		} catch (Error& e) {
			std::cout << "Echo not received. " << e.name() << std::endl;
			ASSERT_EQ(e.code(), error_code_grpc_error);
			ASSERT(false);
		}
	}

	// Invoke RPC from client with incorrect credentials.
	{
		auto provider =
		    std::make_shared<GrpcTlsCredentialStaticProvider>(clientCreds.key, serverCreds.cert, clientCreds.ca);
		AsyncGrpcClient<TestEchoService> client(addr_with_host, provider, pool);
		try {
			EchoRequest request;
			request.set_message("Ping!");
			EchoResponse response = co_await client.call(&TestEchoService::Stub::Echo, request);
			std::cout << "Echo received: " << response.message() << std::endl;
			ASSERT(false);
		} catch (Error& e) {
			ASSERT_EQ(e.code(), error_code_grpc_error);
		}
	}

	// Invoke RPC from client with credentials signed by different root.
	{
		auto provider = std::make_shared<GrpcTlsCredentialStaticProvider>(
		    clientCredsDiffRoot.key, clientCredsDiffRoot.cert, clientCredsDiffRoot.ca);
		AsyncGrpcClient<TestEchoService> client(addr_with_host, provider, pool);
		try {
			EchoRequest request;
			request.set_message("Ping!");
			EchoResponse response = co_await client.call(&TestEchoService::Stub::Echo, request);
			std::cout << "Echo received: " << response.message() << std::endl;
			ASSERT(false);
		} catch (Error& e) {
			ASSERT_EQ(e.code(), error_code_grpc_error);
		}
	}
}

//--- THREAD POOL TESTS --//

} // namespace fdbrpc_test

#endif
