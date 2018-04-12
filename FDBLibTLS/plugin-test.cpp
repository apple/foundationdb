#include <exception>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <stdarg.h>
#include <dlfcn.h>

#include <boost/circular_buffer.hpp>

#include "ITLSPlugin.h"
#include "ReferenceCounted.h"

#include "FDBLibTLSPlugin.h"

#define TESTDATA "./testdata/"

static std::string load_file(std::string path)
{
	std::ifstream fs(path);
	std::stringstream ss;

	ss << fs.rdbuf();
	fs.close();

	return ss.str();
}

struct FDBLibTLSClientServerTest {
	FDBLibTLSClientServerTest(bool client_success, bool server_success, std::string client_path, std::string server_path, std::string client_verify, std::string server_verify):
		client_success(client_success), server_success(server_success), client_verify(client_verify), server_verify(server_verify) {
		client_data = load_file(TESTDATA + client_path);
		server_data = load_file(TESTDATA + server_path);
	}
	~FDBLibTLSClientServerTest() {}

	bool client_success;
	bool server_success;

	std::string client_data;
	std::string client_verify;
	std::string server_data;
	std::string server_verify;
};

struct FDBLibTLSPluginTest {
        FDBLibTLSPluginTest(Reference<ITLSPlugin> plugin, ITLSLogFunc logf);
        ~FDBLibTLSPluginTest();

        Reference<ITLSPlugin> plugin;
        ITLSLogFunc logf;

	boost::circular_buffer<uint8_t> client_buffer;
	boost::circular_buffer<uint8_t> server_buffer;

	int circular_read(boost::circular_buffer<uint8_t> *cb, uint8_t* buf, int len);
	int circular_write(boost::circular_buffer<uint8_t> *cb, const uint8_t* buf, int len);
	int client_read(uint8_t* buf, int len);
	int client_write(const uint8_t* buf, int len);
	int server_read(uint8_t* buf, int len);
	int server_write(const uint8_t* buf, int len);

	Reference<ITLSPolicy> create_policy(void);
	Reference<ITLSSession> create_client_session(Reference<ITLSPolicy> policy);
	Reference<ITLSSession> create_server_session(Reference<ITLSPolicy> policy);

	void circular_reset(void);
	void circular_self_test(void);

	int client_server_test(FDBLibTLSClientServerTest const& cst);
	int set_cert_data_test(void);
};

FDBLibTLSPluginTest::FDBLibTLSPluginTest(Reference<ITLSPlugin> plugin, ITLSLogFunc logf) :
	plugin(plugin), logf(logf)
{
	circular_reset();
	circular_self_test();
}

FDBLibTLSPluginTest::~FDBLibTLSPluginTest()
{
}

int FDBLibTLSPluginTest::circular_read(boost::circular_buffer<uint8_t> *cb, uint8_t* buf, int len)
{
	int n = 0;

	for (n = 0; n < len; n++) {
		if (cb->empty())
			break;
		buf[n] = (*cb)[0];
		cb->pop_front();
	}

	return n;
}

int FDBLibTLSPluginTest::circular_write(boost::circular_buffer<uint8_t> *cb, const uint8_t* buf, int len)
{
	int n = 0;

	for (n = 0; n < len; n++) {
		if (cb->full())
			break;
		cb->push_back(buf[n]);
	}

	return n;
}

int FDBLibTLSPluginTest::client_read(uint8_t* buf, int len)
{
	// Read bytes from the server from the client's buffer.
	return circular_read(&client_buffer, buf, len);
}

int FDBLibTLSPluginTest::client_write(const uint8_t* buf, int len)
{
	// Write bytes from the client into the server's buffer.
	return circular_write(&server_buffer, buf, len);
}

int FDBLibTLSPluginTest::server_read(uint8_t* buf, int len)
{
	// Read bytes from the client from the server's buffer.
	return circular_read(&server_buffer, buf, len);
}

int FDBLibTLSPluginTest::server_write(const uint8_t* buf, int len)
{
	// Write bytes from the server into the client's buffer.
	return circular_write(&client_buffer, buf, len);
}

void FDBLibTLSPluginTest::circular_reset()
{
	client_buffer = boost::circular_buffer<uint8_t>(1024);
	server_buffer = boost::circular_buffer<uint8_t>(1024);
}

void FDBLibTLSPluginTest::circular_self_test()
{
	uint8_t buf[1024] = {1, 2, 3};

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

Reference<ITLSPolicy> FDBLibTLSPluginTest::create_policy(void)
{
	return Reference<ITLSPolicy>(plugin->create_policy((ITLSLogFunc)logf));
}

static int client_send_func(void* ctx, const uint8_t* buf, int len) {
	FDBLibTLSPluginTest *pt = (FDBLibTLSPluginTest *)ctx;
	try {
		return pt->client_write(buf, len);
	} catch ( const std::runtime_error& e ) {
		return -1;
	}
}

static int client_recv_func(void* ctx, uint8_t* buf, int len) {
	FDBLibTLSPluginTest *pt = (FDBLibTLSPluginTest *)ctx;
	try {
		return pt->client_read(buf, len);
	} catch ( const std::runtime_error& e ) {
		return -1;
	}
}

Reference<ITLSSession> FDBLibTLSPluginTest::create_client_session(Reference<ITLSPolicy> policy)
{
	return Reference<ITLSSession>(policy->create_session(true, client_send_func, this, client_recv_func, this, NULL));
}

static int server_send_func(void* ctx, const uint8_t* buf, int len) {
	FDBLibTLSPluginTest *pt = (FDBLibTLSPluginTest *)ctx;
	try {
		return pt->server_write(buf, len);
	} catch ( const std::runtime_error& e ) {
		return -1;
	}
}

static int server_recv_func(void* ctx, uint8_t* buf, int len) {
	FDBLibTLSPluginTest *pt = (FDBLibTLSPluginTest *)ctx;
	try {
		return pt->server_read(buf, len);
	} catch ( const std::runtime_error& e ) {
		return -1;
	}
}

Reference<ITLSSession> FDBLibTLSPluginTest::create_server_session(Reference<ITLSPolicy> policy)
{
	return Reference<ITLSSession>(policy->create_session(false, server_send_func, this, server_recv_func, this, NULL));
}

int FDBLibTLSPluginTest::client_server_test(FDBLibTLSClientServerTest const& cst)
{
	circular_reset();

	Reference<ITLSPolicy> client_policy = create_policy();
	if (!client_policy->set_cert_data((const uint8_t*)&cst.client_data[0], cst.client_data.size())) {
		std::cerr << "FAIL: failed to set client cert data\n";
		return 1;
	}
	if (!client_policy->set_key_data((const uint8_t*)&cst.client_data[0], cst.client_data.size())) {
		std::cerr << "FAIL: failed to set client key data\n";
		return 1;
	}
	if (!client_policy->set_verify_peers((const uint8_t*)&cst.client_verify[0], cst.client_verify.size())) {
		std::cerr << "FAIL: failed to set client key data\n";
		return 1;
	}

	Reference<ITLSPolicy> server_policy = create_policy();
	if (!server_policy->set_cert_data((const uint8_t*)&cst.server_data[0], cst.server_data.size())) {
		std::cerr << "FAIL: failed to set server cert data\n";
		return 1;
	}
	if (!server_policy->set_key_data((const uint8_t*)&cst.server_data[0], cst.server_data.size())) {
		std::cerr << "FAIL: failed to set server key data\n";
		return 1;
	}
	if (!server_policy->set_verify_peers((const uint8_t*)&cst.server_verify[0], cst.server_verify.size())) {
		std::cerr << "FAIL: failed to set client key data\n";
		return 1;
	}

	Reference<ITLSSession> client_session = create_client_session(client_policy);
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
				if (cst.client_success) {
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
				if (cst.server_success) {
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

	if (!cst.client_success && !client_failed)
		std::cerr << "FAIL: client handshake succeeded when it should have failed\n";
	if (!cst.server_success && !server_failed)
		std::cerr << "FAIL: server handshake succeeded when it should have failed\n";
	if (!cst.client_success || !cst.server_success)
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
			rc = client_session->write((const uint8_t*)&client_msg[cn], client_msg.size()-cn);
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
			rc = server_session->write((const uint8_t*)&server_msg[cn], server_msg.size()-cn);
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

static void logf(const char* event, void* uid, int is_error, ...) {
	va_list args;

	std::string log_type ("INFO");
	if (is_error)
		log_type = "ERROR";

	std::cerr << log_type << ": " << event;

	va_start(args, is_error);

	const char *s = va_arg(args, const char *);
	while (s != NULL) {
		std::cerr << " " << s;
		s = va_arg(args, const char *);
	}

	std::cerr << "\n";

	va_end(args);
}

int main(int argc, char **argv)
{
	void *pluginSO = NULL;
	void *(*getPlugin)(const char*);
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

	getPlugin = (void*(*)(const char*))dlsym( pluginSO, "get_plugin" );
	if (getPlugin == NULL) {
		std::cerr << "plugin '" << argv[1] << "' does not provide get_plugin()\n";
		exit(1);
	}

	Reference<ITLSPlugin> plugin = Reference<ITLSPlugin>((ITLSPlugin *)getPlugin(ITLSPlugin::get_plugin_type_name_and_version()));

	std::vector<FDBLibTLSClientServerTest> tests = {
		// Valid - all use single root CA.
		FDBLibTLSClientServerTest(true, true, "test-1-client.pem", "test-1-server.pem", "", ""),
		FDBLibTLSClientServerTest(true, true, "test-1-client.pem", "test-2-server.pem", "", ""),
		FDBLibTLSClientServerTest(true, true, "test-2-client.pem", "test-2-server.pem", "", ""),
		FDBLibTLSClientServerTest(true, true, "test-2-client.pem", "test-1-server.pem", "", ""),

		// Certificates terminate at different intermediate CAs.
		FDBLibTLSClientServerTest(false, false, "test-4-client.pem", "test-5-server.pem", "", ""),
		FDBLibTLSClientServerTest(false, false, "test-5-client.pem", "test-4-server.pem", "", ""),
		FDBLibTLSClientServerTest(true, true, "test-4-client.pem", "test-5-server.pem",
			"Check.Valid=0", "Check.Valid=0"),
		FDBLibTLSClientServerTest(true, true, "test-5-client.pem", "test-4-server.pem",
			"Check.Valid=0", "Check.Valid=0"),

		// Expired certificates.
		FDBLibTLSClientServerTest(false, false, "test-1-client.pem", "test-3-server.pem", "", ""),
		FDBLibTLSClientServerTest(false, false, "test-3-client.pem", "test-1-server.pem", "", ""),
		FDBLibTLSClientServerTest(true, true, "test-1-client.pem", "test-3-server.pem", "Check.Unexpired=0", ""),
		FDBLibTLSClientServerTest(true, true, "test-3-client.pem", "test-1-server.pem", "", "Check.Unexpired=0"),
		FDBLibTLSClientServerTest(true, true, "test-1-client.pem", "test-3-server.pem", "Check.Valid=0", ""),
		FDBLibTLSClientServerTest(true, true, "test-3-client.pem", "test-1-server.pem", "", "Check.Valid=0"),

		// Match on specific subject and/or issuer.
		FDBLibTLSClientServerTest(true, true, "test-1-client.pem", "test-1-server.pem", "C=US", ""),
		FDBLibTLSClientServerTest(false, true, "test-1-client.pem", "test-2-server.pem", "C=US", ""),
		FDBLibTLSClientServerTest(true, true, "test-1-client.pem", "test-2-server.pem", "C=AU", ""),
		FDBLibTLSClientServerTest(true, true, "test-1-client.pem", "test-2-server.pem",
			"CN=FDB LibTLS Plugin Test Server 2\\, \\80 \\<\\01\\+\\02=\\03\\>", ""),
		FDBLibTLSClientServerTest(false, true, "test-1-client.pem", "test-2-server.pem",
			"CN=FDB LibTLS Plugin Test Server 2\\, \\80 \\<\\01\\+\\02=\\04\\>", ""),
		FDBLibTLSClientServerTest(false, true, "test-1-client.pem", "test-2-server.pem",
			"CN=FDB LibTLS Plugin Test Server 2\\, \\81 \\<\\01\\+\\02=\\04\\>", ""),
		FDBLibTLSClientServerTest(false, true, "test-1-client.pem", "test-2-server.pem",
			"CN=FDB LibTLS Plugin Test Server 2\\, \\80 \\<\\01\\+\\02=\\04", ""),
		FDBLibTLSClientServerTest(true, true, "test-1-client.pem", "test-2-server.pem",
			"CN=FDB LibTLS Plugin Test Server 2\\, \\80 \\<\\01\\+\\02=\\03\\>",
			"CN=FDB LibTLS Plugin Test Client 1"),
		FDBLibTLSClientServerTest(true, true, "test-1-client.pem", "test-1-server.pem",
			"", "CN=FDB LibTLS Plugin Test Client 1"),
		FDBLibTLSClientServerTest(true, false, "test-2-client.pem", "test-1-server.pem",
			"", "O=Apple Pty Limited,OU=FDC Team"),
		FDBLibTLSClientServerTest(true, true, "test-2-client.pem", "test-1-server.pem",
			"O=Apple Inc.,OU=FDB Team", "O=Apple Pty Limited,OU=FDB Team"),
		FDBLibTLSClientServerTest(false, false, "test-2-client.pem", "test-1-server.pem",
			"O=Apple Inc.,OU=FDC Team", "O=Apple Pty Limited,OU=FDC Team"),
		FDBLibTLSClientServerTest(true, true, "test-1-client.pem", "test-1-server.pem",
			"I.C=US,I.ST=California,I.L=Cupertino,I.O=Apple Inc.,I.OU=FDB Team",
			"I.C=US,I.ST=California,I.L=Cupertino,I.O=Apple Inc.,I.OU=FDB Team"),
		FDBLibTLSClientServerTest(false, false, "test-1-client.pem", "test-1-server.pem",
			"I.C=US,I.ST=California,I.L=Cupertino,I.O=Apple Inc.,I.OU=FDC Team",
			"I.C=US,I.ST=California,I.L=Cupertino,I.O=Apple Inc.,I.OU=FDC Team"),
		FDBLibTLSClientServerTest(true, true, "test-1-client.pem", "test-1-server.pem",
			"I.CN=FDB LibTLS Plugin Test Intermediate CA 1",
			"I.CN=FDB LibTLS Plugin Test Intermediate CA 1"),
		FDBLibTLSClientServerTest(false, true, "test-1-client.pem", "test-1-server.pem",
			"I.CN=FDB LibTLS Plugin Test Intermediate CA 2",
			"I.CN=FDB LibTLS Plugin Test Intermediate CA 1"),
		FDBLibTLSClientServerTest(true, true, "test-1-client.pem", "test-2-server.pem",
			"I.CN=FDB LibTLS Plugin Test Intermediate CA 2",
			"I.CN=FDB LibTLS Plugin Test Intermediate CA 1"),
		FDBLibTLSClientServerTest(true, true, "test-1-client.pem", "test-2-server.pem",
			"CN=FDB LibTLS Plugin Test Server 2\\, \\80 \\<\\01\\+\\02=\\03\\>,I.CN=FDB LibTLS Plugin Test Intermediate CA 2",
			"I.CN=FDB LibTLS Plugin Test Intermediate CA 1,O=Apple Inc.,I.C=US,S.C=US"),
		FDBLibTLSClientServerTest(false, true, "test-1-client.pem", "test-2-server.pem",
			"CN=FDB LibTLS Plugin Test Server 2\\, \\80 \\<\\01\\+\\02=\\03\\>,I.CN=FDB LibTLS Plugin Test Intermediate CA 1",
			"I.CN=FDB LibTLS Plugin Test Intermediate CA 1,O=Apple Inc.,I.C=US,S.C=US"),
	};

	FDBLibTLSPluginTest *pt = new FDBLibTLSPluginTest(plugin, (ITLSLogFunc)logf);

	int test_num = 1;
	for (auto &test: tests) {
		std::cerr << "== Test " << test_num++ << " ==\n";
		failed |= pt->client_server_test(test);
	}

	delete pt;

	return (failed);
}
