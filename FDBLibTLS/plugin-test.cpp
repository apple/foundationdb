/*
 * plugin-test.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include <exception>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <stdarg.h>
#include <dlfcn.h>

#include <boost/circular_buffer.hpp>

#include "fdbrpc/ITLSPlugin.h"

#include "FDBLibTLS/FDBLibTLSPlugin.h"

#define TESTDATA "./testdata/"

static std::string load_file(std::string path) {
	std::ifstream fs(path);
	std::stringstream ss;

	ss << fs.rdbuf();
	fs.close();

	return ss.str();
}

struct client_server_test {
	std::string ca_path;

	bool client_success;
	std::string client_path;
	const char* client_password;
	std::vector<std::string> client_verify;
	const char* servername;

	bool server_success;
	std::string server_path;
	const char* server_password;
	std::vector<std::string> server_verify;
};

struct FDBLibTLSPluginTest {
	FDBLibTLSPluginTest(Reference<ITLSPlugin> plugin);
	~FDBLibTLSPluginTest();

	Reference<ITLSPlugin> plugin;

	boost::circular_buffer<uint8_t> client_buffer;
	boost::circular_buffer<uint8_t> server_buffer;

	int circular_read(boost::circular_buffer<uint8_t>* cb, uint8_t* buf, int len);
	int circular_write(boost::circular_buffer<uint8_t>* cb, const uint8_t* buf, int len);
	int client_read(uint8_t* buf, int len);
	int client_write(const uint8_t* buf, int len);
	int server_read(uint8_t* buf, int len);
	int server_write(const uint8_t* buf, int len);

	Reference<ITLSPolicy> create_policy(void);
	Reference<ITLSSession> create_client_session(Reference<ITLSPolicy> policy, const char* servername);
	Reference<ITLSSession> create_server_session(Reference<ITLSPolicy> policy);

	void circular_reset(void);
	void circular_self_test(void);

	int client_server_test(const struct client_server_test* cst);
	int set_cert_data_test(void);
};

FDBLibTLSPluginTest::FDBLibTLSPluginTest(Reference<ITLSPlugin> plugin) : plugin(plugin) {
	circular_reset();
	circular_self_test();
}

FDBLibTLSPluginTest::~FDBLibTLSPluginTest() {}

int FDBLibTLSPluginTest::circular_read(boost::circular_buffer<uint8_t>* cb, uint8_t* buf, int len) {
	int n = 0;

	for (n = 0; n < len; n++) {
		if (cb->empty())
			break;
		buf[n] = (*cb)[0];
		cb->pop_front();
	}

	return n;
}

int FDBLibTLSPluginTest::circular_write(boost::circular_buffer<uint8_t>* cb, const uint8_t* buf, int len) {
	int n = 0;

	for (n = 0; n < len; n++) {
		if (cb->full())
			break;
		cb->push_back(buf[n]);
	}

	return n;
}

int FDBLibTLSPluginTest::client_read(uint8_t* buf, int len) {
	// Read bytes from the server from the client's buffer.
	return circular_read(&client_buffer, buf, len);
}

int FDBLibTLSPluginTest::client_write(const uint8_t* buf, int len) {
	// Write bytes from the client into the server's buffer.
	return circular_write(&server_buffer, buf, len);
}

int FDBLibTLSPluginTest::server_read(uint8_t* buf, int len) {
	// Read bytes from the client from the server's buffer.
	return circular_read(&server_buffer, buf, len);
}

int FDBLibTLSPluginTest::server_write(const uint8_t* buf, int len) {
	// Write bytes from the server into the client's buffer.
	return circular_write(&client_buffer, buf, len);
}

void FDBLibTLSPluginTest::circular_reset() {
	client_buffer = boost::circular_buffer<uint8_t>(1024);
	server_buffer = boost::circular_buffer<uint8_t>(1024);
}

void FDBLibTLSPluginTest::circular_self_test() {
	uint8_t buf[1024] = { 1, 2, 3 };

	std::cerr << "INFO: running circular buffer self tests...\n";

	assert(server_read(buf, 3) == 0);

	buf[0] = 1, buf[1] = 2, buf[2] = 3;
	assert(client_write(buf, 2) == 2);

	buf[0] = buf[1] = buf[2] = 255;
	assert(server_read(buf, 3) == 2);
	assert(buf[0] == 1 && buf[1] == 2 && buf[2] == 255);

	assert(client_write(buf, 1024) == 1024);
	assert(client_write(buf, 1) == 0);
	assert(server_read(buf, 1) == 1);
	assert(client_write(buf, 1) == 1);
	assert(client_write(buf, 1) == 0);
	assert(server_read(buf, 1024) == 1024);
	assert(server_read(buf, 1024) == 0);

	assert(client_read(buf, 3) == 0);

	buf[0] = 1, buf[1] = 2, buf[2] = 3;
	assert(server_write(buf, 2) == 2);

	buf[0] = buf[1] = buf[2] = 255;
	assert(client_read(buf, 3) == 2);
	assert(buf[0] == 1 && buf[1] == 2 && buf[2] == 255);

	assert(server_write(buf, 1024) == 1024);
	assert(server_write(buf, 1) == 0);
	assert(client_read(buf, 1) == 1);
	assert(server_write(buf, 1) == 1);
	assert(server_write(buf, 1) == 0);
	assert(client_read(buf, 1024) == 1024);
	assert(client_read(buf, 1024) == 0);
}

Reference<ITLSPolicy> FDBLibTLSPluginTest::create_policy(void) {
	return Reference<ITLSPolicy>(plugin->create_policy());
}

static int client_send_func(void* ctx, const uint8_t* buf, int len) {
	FDBLibTLSPluginTest* pt = (FDBLibTLSPluginTest*)ctx;
	try {
		return pt->client_write(buf, len);
	} catch (const std::runtime_error& e) {
		return -1;
	}
}

static int client_recv_func(void* ctx, uint8_t* buf, int len) {
	FDBLibTLSPluginTest* pt = (FDBLibTLSPluginTest*)ctx;
	try {
		return pt->client_read(buf, len);
	} catch (const std::runtime_error& e) {
		return -1;
	}
}

Reference<ITLSSession> FDBLibTLSPluginTest::create_client_session(Reference<ITLSPolicy> policy,
                                                                  const char* servername) {
	return Reference<ITLSSession>(
	    policy->create_session(true, servername, client_send_func, this, client_recv_func, this, NULL));
}

static int server_send_func(void* ctx, const uint8_t* buf, int len) {
	FDBLibTLSPluginTest* pt = (FDBLibTLSPluginTest*)ctx;
	try {
		return pt->server_write(buf, len);
	} catch (const std::runtime_error& e) {
		return -1;
	}
}

static int server_recv_func(void* ctx, uint8_t* buf, int len) {
	FDBLibTLSPluginTest* pt = (FDBLibTLSPluginTest*)ctx;
	try {
		return pt->server_read(buf, len);
	} catch (const std::runtime_error& e) {
		return -1;
	}
}

Reference<ITLSSession> FDBLibTLSPluginTest::create_server_session(Reference<ITLSPolicy> policy) {
	return Reference<ITLSSession>(
	    policy->create_session(false, NULL, server_send_func, this, server_recv_func, this, NULL));
}

#define MAX_VERIFY_RULES 5

static void convert_verify_peers(const std::vector<std::string>* verify_rules,
                                 const uint8_t* verify_peers[],
                                 int verify_peers_len[]) {
	if (verify_rules->size() > MAX_VERIFY_RULES)
		throw std::runtime_error("verify");
	int i = 0;
	for (auto& verify_rule : *verify_rules) {
		verify_peers[i] = (const uint8_t*)&verify_rule[0];
		verify_peers_len[i] = verify_rule.size();
		i++;
	}
}

int FDBLibTLSPluginTest::client_server_test(const struct client_server_test* cst) {
	const uint8_t* verify_peers[MAX_VERIFY_RULES];
	int verify_peers_len[MAX_VERIFY_RULES];

	circular_reset();

	std::string ca_data = load_file(TESTDATA + cst->ca_path);
	std::string client_data = load_file(TESTDATA + cst->client_path);
	std::string server_data = load_file(TESTDATA + cst->server_path);

	Reference<ITLSPolicy> client_policy = create_policy();
	if (!client_policy->set_ca_data((const uint8_t*)&ca_data[0], ca_data.size())) {
		std::cerr << "FAIL: failed to set client ca data\n";
		return 1;
	}
	if (!client_policy->set_cert_data((const uint8_t*)&client_data[0], client_data.size())) {
		std::cerr << "FAIL: failed to set client cert data\n";
		return 1;
	}
	if (!client_policy->set_key_data((const uint8_t*)&client_data[0], client_data.size(), cst->client_password)) {
		std::cerr << "FAIL: failed to set client key data\n";
		return 1;
	}
	if (!cst->client_verify.empty()) {
		convert_verify_peers(&cst->client_verify, verify_peers, verify_peers_len);
		if (!client_policy->set_verify_peers(cst->client_verify.size(), verify_peers, verify_peers_len)) {
			std::cerr << "FAIL: failed to set client verify peers\n";
			return 1;
		}
	}

	Reference<ITLSPolicy> server_policy = create_policy();
	if (!server_policy->set_ca_data((const uint8_t*)&ca_data[0], ca_data.size())) {
		std::cerr << "FAIL: failed to set server ca data\n";
		return 1;
	}
	if (!server_policy->set_cert_data((const uint8_t*)&server_data[0], server_data.size())) {
		std::cerr << "FAIL: failed to set server cert data\n";
		return 1;
	}
	if (!server_policy->set_key_data((const uint8_t*)&server_data[0], server_data.size(), cst->server_password)) {
		std::cerr << "FAIL: failed to set server key data\n";
		return 1;
	}
	convert_verify_peers(&cst->server_verify, verify_peers, verify_peers_len);
	if (!server_policy->set_verify_peers(cst->server_verify.size(), verify_peers, verify_peers_len)) {
		std::cerr << "FAIL: failed to set server verify peers\n";
		return 1;
	}

	Reference<ITLSSession> client_session = create_client_session(client_policy, cst->servername);
	Reference<ITLSSession> server_session = create_server_session(server_policy);

	if (client_session.getPtr() == NULL || server_session.getPtr() == NULL)
		return 1;

	std::cerr << "INFO: starting TLS handshake...\n";

	bool client_done = false, server_done = false;
	bool client_failed = false, server_failed = false;
	int rc, i = 0;
	do {
		if (!client_done) {
			rc = client_session->handshake();
			if (rc == ITLSSession::SUCCESS) {
				client_done = true;
			} else if (rc == ITLSSession::FAILED) {
				if (cst->client_success) {
					std::cerr << "FAIL: failed to complete client handshake\n";
					return 1;
				} else {
					std::cerr << "INFO: failed to complete client handshake (as expected)\n";
					client_failed = true;
					client_done = true;
				}
			} else if (rc != ITLSSession::WANT_READ && rc != ITLSSession::WANT_WRITE) {
				std::cerr << "FAIL: client handshake returned unknown value: " << rc << "\n";
				return 1;
			}
		}
		if (!server_done) {
			rc = server_session->handshake();
			if (rc == ITLSSession::SUCCESS) {
				server_done = true;
			} else if (rc == ITLSSession::FAILED) {
				if (cst->server_success) {
					std::cerr << "FAIL: failed to complete server handshake\n";
					return 1;
				} else {
					std::cerr << "INFO: failed to complete server handshake (as expected)\n";
					server_failed = true;
					server_done = true;
				}
			} else if (rc != ITLSSession::WANT_READ && rc != ITLSSession::WANT_WRITE) {
				std::cerr << "FAIL: server handshake returned unknown value: " << rc << "\n";
				return 1;
			}
		}
	} while (i++ < 100 && (!client_done || !server_done));

	if (!client_done || !server_done) {
		std::cerr << "FAIL: failed to complete handshake\n";
		return 1;
	}

	if (!cst->client_success && !client_failed) {
		std::cerr << "FAIL: client handshake succeeded when it should have failed\n";
		return 1;
	}
	if (!cst->server_success && !server_failed) {
		std::cerr << "FAIL: server handshake succeeded when it should have failed\n";
		return 1;
	}
	if (!cst->client_success || !cst->server_success)
		return 0;

	std::cerr << "INFO: handshake completed successfully\n";

	//
	// Write on client and read on server.
	//
	std::cerr << "INFO: starting client write test...\n";

	std::string client_msg("FDBLibTLSPlugin Client Write Test");
	std::string server_msg;
	size_t cn = 0, sn = 0;
	uint8_t buf[16];

	client_done = false, server_done = false;
	i = 0;
	do {
		if (!client_done) {
			rc = client_session->write((const uint8_t*)&client_msg[cn], client_msg.size() - cn);
			if (rc > 0) {
				cn += rc;
				if (cn >= client_msg.size())
					client_done = true;
			} else if (rc == ITLSSession::FAILED) {
				std::cerr << "FAIL: failed to complete client write\n";
				return 1;
			} else if (rc != ITLSSession::WANT_READ && rc != ITLSSession::WANT_WRITE) {
				std::cerr << "FAIL: client write returned unknown value: " << rc << "\n";
				return 1;
			}
		}
		if (!server_done) {
			rc = server_session->read(buf, sizeof(buf));
			if (rc > 0) {
				sn += rc;
				for (int j = 0; j < rc; j++)
					server_msg += buf[j];
				if (sn >= client_msg.size())
					server_done = true;
			} else if (rc == ITLSSession::FAILED) {
				std::cerr << "FAIL: failed to complete server read\n";
				return 1;
			} else if (rc != ITLSSession::WANT_READ && rc != ITLSSession::WANT_WRITE) {
				std::cerr << "FAIL: server read returned unknown value: " << rc << "\n";
				return 1;
			}
		}
	} while (i++ < 100 && (!client_done || !server_done));

	if (client_msg != server_msg) {
		std::cerr << "FAIL: got client msg '" << server_msg << "' want '" << client_msg << "'\n";
		return 1;
	}

	std::cerr << "INFO: client write test completed successfully\n";

	//
	// Write on server and read on client.
	//
	std::cerr << "INFO: starting server write test...\n";

	server_msg = "FDBLibTLSPlugin Server Write Test";
	client_msg.clear();
	cn = 0, sn = 0;

	client_done = false, server_done = false;
	i = 0;
	do {
		if (!server_done) {
			rc = server_session->write((const uint8_t*)&server_msg[cn], server_msg.size() - cn);
			if (rc > 0) {
				cn += rc;
				if (cn >= server_msg.size())
					server_done = true;
			} else if (rc == ITLSSession::FAILED) {
				std::cerr << "FAIL: failed to complete server write\n";
				return 1;
			} else if (rc != ITLSSession::WANT_READ && rc != ITLSSession::WANT_WRITE) {
				std::cerr << "FAIL: server write returned unknown value: " << rc << "\n";
				return 1;
			}
		}
		if (!client_done) {
			rc = client_session->read(buf, sizeof(buf));
			if (rc > 0) {
				sn += rc;
				for (int j = 0; j < rc; j++)
					client_msg += buf[j];
				if (sn >= server_msg.size())
					client_done = true;
			} else if (rc == ITLSSession::FAILED) {
				std::cerr << "FAIL: failed to complete client read\n";
				return 1;
			} else if (rc != ITLSSession::WANT_READ && rc != ITLSSession::WANT_WRITE) {
				std::cerr << "FAIL: client read returned unknown value: " << rc << "\n";
				return 1;
			}
		}
	} while (i++ < 100 && (!client_done || !server_done));

	if (server_msg != client_msg) {
		std::cerr << "FAIL: got server msg '" << client_msg << "' want '" << server_msg << "'\n";
		return 1;
	}

	std::cerr << "INFO: server write test completed successfully\n";

	return 0;
}

static void logf(const char* event, void* uid, bool is_error, ...) {
	va_list args;

	std::string log_type("INFO");
	if (is_error)
		log_type = "ERROR";

	std::cerr << log_type << ": " << event;

	va_start(args, is_error);

	const char* s = va_arg(args, const char*);
	while (s != NULL) {
		std::cerr << " " << s;
		s = va_arg(args, const char*);
	}

	std::cerr << "\n";

	va_end(args);
}

const struct client_server_test client_server_tests[] = {
	// Single root CA.
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-1.pem",
	    .server_password = NULL,
	    .server_verify = { "" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-2.pem",
	    .server_password = NULL,
	    .server_verify = { "" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-2.pem",
	    .client_password = NULL,
	    .client_verify = { "" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-1.pem",
	    .server_password = NULL,
	    .server_verify = { "" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-2.pem",
	    .client_password = NULL,
	    .client_verify = { "" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-2.pem",
	    .server_password = NULL,
	    .server_verify = { "" },
	},

	// Multiple root CAs.
	{
	    .ca_path = "test-ca-all.pem",
	    .client_success = true,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-4.pem",
	    .server_password = "fdb123",
	    .server_verify = { "" },
	},
	{
	    .ca_path = "test-ca-all.pem",
	    .client_success = true,
	    .client_path = "test-client-4.pem",
	    .client_password = "fdb321",
	    .client_verify = { "" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-1.pem",
	    .server_password = "fdb123",
	    .server_verify = { "" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = false,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-4.pem",
	    .server_password = "fdb123",
	    .server_verify = { "" },
	},
	{
	    .ca_path = "test-ca-2.pem",
	    .client_success = true,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "" },
	    .servername = NULL,
	    .server_success = false,
	    .server_path = "test-server-4.pem",
	    .server_password = "fdb123",
	    .server_verify = { "" },
	},

	// Expired certificates.
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = false,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-3.pem",
	    .server_password = NULL,
	    .server_verify = { "" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-3.pem",
	    .client_password = NULL,
	    .client_verify = { "" },
	    .servername = NULL,
	    .server_success = false,
	    .server_path = "test-server-1.pem",
	    .server_password = NULL,
	    .server_verify = { "" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "Check.Unexpired=0" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-3.pem",
	    .server_password = NULL,
	    .server_verify = { "" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-3.pem",
	    .client_password = NULL,
	    .client_verify = { "" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-1.pem",
	    .server_password = NULL,
	    .server_verify = { "Check.Unexpired=0" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "Check.Valid=0" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-3.pem",
	    .server_password = NULL,
	    .server_verify = { "" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-3.pem",
	    .client_password = NULL,
	    .client_verify = { "" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-1.pem",
	    .server_password = NULL,
	    .server_verify = { "Check.Valid=0" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = false,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "I.CN=FDB LibTLS Plugin Test Intermediate CA 1",
	                       "I.CN=FDB LibTLS Plugin Test Intermediate CA 2,Check.Unexpired=0" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-3.pem",
	    .server_password = NULL,
	    .server_verify = { "" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "I.CN=FDB LibTLS Plugin Test Intermediate CA 1,Check.Unexpired=0",
	                       "I.CN=FDB LibTLS Plugin Test Intermediate CA 2" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-3.pem",
	    .server_password = NULL,
	    .server_verify = { "" },
	},

	// Match on specific subject and/or issuer.
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "C=US" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-1.pem",
	    .server_password = NULL,
	    .server_verify = { "" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = false,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "C=US" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-2.pem",
	    .server_password = NULL,
	    .server_verify = { "" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "C=AU" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-2.pem",
	    .server_password = NULL,
	    .server_verify = { "" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "C=US", "C=AU" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-2.pem",
	    .server_password = NULL,
	    .server_verify = { "" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = false,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "C=US", "C=JP" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-2.pem",
	    .server_password = NULL,
	    .server_verify = { "" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "CN=FDB LibTLS Plugin Test Server 2\\, \\80 \\<\\01\\+\\02=\\03\\>" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-2.pem",
	    .server_password = NULL,
	    .server_verify = { "" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = false,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "CN=FDB LibTLS Plugin Test Server 2\\, \\80 \\<\\01\\+\\02=\\04\\>" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-2.pem",
	    .server_password = NULL,
	    .server_verify = { "" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = false,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "CN=FDB LibTLS Plugin Test Server 2\\, \\81 \\<\\01\\+\\02=\\04\\>" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-2.pem",
	    .server_password = NULL,
	    .server_verify = { "" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = false,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "CN=FDB LibTLS Plugin Test Server 2\\, \\80 \\<\\01\\+\\02=\\04" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-2.pem",
	    .server_password = NULL,
	    .server_verify = { "" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "CN=FDB LibTLS Plugin Test Server 2\\, \\80 \\<\\01\\+\\02=\\03\\>" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-2.pem",
	    .server_password = NULL,
	    .server_verify = { "CN=FDB LibTLS Plugin Test Client 1" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-1.pem",
	    .server_password = NULL,
	    .server_verify = { "CN=FDB LibTLS Plugin Test Client 1" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-2.pem",
	    .client_password = NULL,
	    .client_verify = { "" },
	    .servername = NULL,
	    .server_success = false,
	    .server_path = "test-server-1.pem",
	    .server_password = NULL,
	    .server_verify = { "O=Apple Pty Limited,OU=FDC Team" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-2.pem",
	    .client_password = NULL,
	    .client_verify = { "O=Apple Inc.,OU=FDB Team" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-1.pem",
	    .server_password = NULL,
	    .server_verify = { "O=Apple Pty Limited,OU=FDB Team" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = false,
	    .client_path = "test-client-2.pem",
	    .client_password = NULL,
	    .client_verify = { "O=Apple Inc.,OU=FDC Team" },
	    .servername = NULL,
	    .server_success = false,
	    .server_path = "test-server-1.pem",
	    .server_password = NULL,
	    .server_verify = { "O=Apple Pty Limited,OU=FDC Team" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "I.C=US,I.ST=California,I.L=Cupertino,I.O=Apple Inc.,I.OU=FDB Team" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-1.pem",
	    .server_password = NULL,
	    .server_verify = { "I.C=US,I.ST=California,I.L=Cupertino,I.O=Apple Inc.,I.OU=FDB Team" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = false,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "I.C=US,I.ST=California,I.L=Cupertino,I.O=Apple Inc.,I.OU=FDC Team" },
	    .servername = NULL,
	    .server_success = false,
	    .server_path = "test-server-1.pem",
	    .server_password = NULL,
	    .server_verify = { "I.C=US,I.ST=California,I.L=Cupertino,I.O=Apple Inc.,I.OU=FDC Team" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "I.CN=FDB LibTLS Plugin Test Intermediate CA 1" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-1.pem",
	    .server_password = NULL,
	    .server_verify = { "I.CN=FDB LibTLS Plugin Test Intermediate CA 1" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = false,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "I.CN=FDB LibTLS Plugin Test Intermediate CA 2" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-1.pem",
	    .server_password = NULL,
	    .server_verify = { "I.CN=FDB LibTLS Plugin Test Intermediate CA 1" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "I.CN=FDB LibTLS Plugin Test Intermediate CA 2" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-2.pem",
	    .server_password = NULL,
	    .server_verify = { "I.CN=FDB LibTLS Plugin Test Intermediate CA 1" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "CN=FDB LibTLS Plugin Test Server 2\\, \\80 \\<\\01\\+\\02=\\03\\>,I.CN=FDB LibTLS Plugin "
	                       "Test Intermediate CA 2" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-2.pem",
	    .server_password = NULL,
	    .server_verify = { "I.CN=FDB LibTLS Plugin Test Intermediate CA 1,O=Apple Inc.,I.C=US,S.C=US" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = false,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "CN=FDB LibTLS Plugin Test Server 2\\, \\80 \\<\\01\\+\\02=\\03\\>,I.CN=FDB LibTLS Plugin "
	                       "Test Intermediate CA 1" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-2.pem",
	    .server_password = NULL,
	    .server_verify = { "I.CN=FDB LibTLS Plugin Test Intermediate CA 1,O=Apple Inc.,I.C=US,S.C=US" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "R.CN=FDB LibTLS Plugin Test Root CA 1" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-2.pem",
	    .server_password = NULL,
	    .server_verify = { "R.CN=FDB LibTLS Plugin Test Root CA 1" },
	},
	{
	    .ca_path = "test-ca-all.pem",
	    .client_success = false,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "R.CN=FDB LibTLS Plugin Test Root CA 1" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-4.pem",
	    .server_password = "fdb123",
	    .server_verify = { "R.CN=FDB LibTLS Plugin Test Root CA 1" },
	},
	{
	    .ca_path = "test-ca-all.pem",
	    .client_success = true,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "R.CN=FDB LibTLS Plugin Test Root CA 1", "R.CN=FDB LibTLS Plugin Test Root CA 2" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-4.pem",
	    .server_password = "fdb123",
	    .server_verify = { "R.CN=FDB LibTLS Plugin Test Root CA 1" },
	},
	{
	    .ca_path = "test-ca-all.pem",
	    .client_success = true,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "R.CN=FDB LibTLS Plugin Test Root CA 2" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-4.pem",
	    .server_password = "fdb123",
	    .server_verify = { "R.CN=FDB LibTLS Plugin Test Root CA 1" },
	},
	{
	    .ca_path = "test-ca-all.pem",
	    .client_success = true,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = { "R.OU=FDB Team" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-4.pem",
	    .server_password = "fdb123",
	    .server_verify = { "R.OU=FDB Team" },
	},

	// Client performing name validation via servername.
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = {},
	    .servername = "test.foundationdb.org",
	    .server_success = true,
	    .server_path = "test-server-1.pem",
	    .server_password = NULL,
	    .server_verify = { "" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = false,
	    .client_path = "test-client-1.pem",
	    .client_password = NULL,
	    .client_verify = {},
	    .servername = "www.foundationdb.org",
	    .server_success = true,
	    .server_path = "test-server-1.pem",
	    .server_password = NULL,
	    .server_verify = { "" },
	},

	// Prefix and Suffix Matching
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-2.pem",
	    .client_password = NULL,
	    .client_verify = { "O>=Apple Inc.,OU>=FDB" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-1.pem",
	    .server_password = NULL,
	    .server_verify = { "O<=Limited,OU<=Team" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = false,
	    .client_path = "test-client-2.pem",
	    .client_password = NULL,
	    .client_verify = { "O<=Apple Inc.,OU<=FDB" },
	    .servername = NULL,
	    .server_success = false,
	    .server_path = "test-server-1.pem",
	    .server_password = NULL,
	    .server_verify = { "O>=Limited,OU>=Team" },
	},

	// Subject Alternative Name
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-2.pem",
	    .client_password = NULL,
	    .client_verify = { "S.subjectAltName=DNS:test.foundationdb.org" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-1.pem",
	    .server_password = NULL,
	    .server_verify = { "Check.Valid=0" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-2.pem",
	    .client_password = NULL,
	    .client_verify = { "S.subjectAltName>=DNS:test." },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-1.pem",
	    .server_password = NULL,
	    .server_verify = { "Check.Valid=0" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = true,
	    .client_path = "test-client-2.pem",
	    .client_password = NULL,
	    .client_verify = { "S.subjectAltName<=DNS:.org" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-1.pem",
	    .server_password = NULL,
	    .server_verify = { "Check.Valid=0" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = false,
	    .client_path = "test-client-2.pem",
	    .client_password = NULL,
	    .client_verify = { "S.subjectAltName<=DNS:.com" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-1.pem",
	    .server_password = NULL,
	    .server_verify = { "Check.Valid=0" },
	},
	{
	    .ca_path = "test-ca-1.pem",
	    .client_success = false,
	    .client_path = "test-client-2.pem",
	    .client_password = NULL,
	    .client_verify = { "S.subjectAltName<=EMAIL:.com" },
	    .servername = NULL,
	    .server_success = true,
	    .server_path = "test-server-1.pem",
	    .server_password = NULL,
	    .server_verify = { "Check.Valid=0" },
	},
};

int main(int argc, char** argv) {
	void* pluginSO = NULL;
	void* (*getPlugin)(const char*);
	int failed = 0;

	if (argc != 2) {
		std::cerr << "usage: " << argv[0] << " <plugin_path>\n";
		exit(1);
	}

	pluginSO = dlopen(argv[1], RTLD_LAZY | RTLD_LOCAL);
	if (pluginSO == NULL) {
		std::cerr << "failed to load plugin '" << argv[1] << "': " << dlerror() << "\n";
		exit(1);
	}

	getPlugin = (void* (*)(const char*))dlsym(pluginSO, "get_plugin");
	if (getPlugin == NULL) {
		std::cerr << "plugin '" << argv[1] << "' does not provide get_plugin()\n";
		exit(1);
	}

	Reference<ITLSPlugin> plugin =
	    Reference<ITLSPlugin>((ITLSPlugin*)getPlugin(ITLSPlugin::get_plugin_type_name_and_version()));

	FDBLibTLSPluginTest* pt = new FDBLibTLSPluginTest(plugin);

	int test_num = 1;
	for (auto& cst : client_server_tests) {
		std::cerr << "== Test " << test_num++ << " ==\n";
		failed |= pt->client_server_test(&cst);
	}

	delete pt;

	return (failed);
}
