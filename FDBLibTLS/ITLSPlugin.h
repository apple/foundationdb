// Apple Proprietary and Confidential Information

#ifndef FDB_ITLSPLUGIN_H
#define FDB_ITLSPLUGIN_H

#pragma once

#include <stdint.h>

struct ITLSSession {
	enum { SUCCESS = 0, WANT_READ = -1, WANT_WRITE = -2, FAILED = -3 };

	virtual void addref() = 0;
	virtual void delref() = 0;

	// handshake should return SUCCESS if the handshake is complete,
	// FAILED on fatal error, or one of WANT_READ or WANT_WRITE if the
	// handshake should be reattempted after more data can be
	// read/written on the underlying connection.
	virtual int handshake() = 0;

	// read should return the (non-zero) number of bytes read,
	// WANT_READ or WANT_WRITE if the operation is blocked by the
	// underlying stream, or FAILED if there is an error (including a
	// closed connection).
	virtual int read(uint8_t* data, int length) = 0;

	// write should return the (non-zero) number of bytes written, or
	// WANT_READ or WANT_WRITE if the operation is blocked by the
	// underlying stream, or FAILED if there is an error.
	virtual int write(const uint8_t* data, int length) = 0;
};

// Returns the number of bytes sent (possibly 0), or -1 on error
// (including connection close)
typedef int (*TLSSendCallbackFunc)(void* ctx, const uint8_t* buf, int len);

// Returns the number of bytes read (possibly 0), or -1 on error
// (including connection close)
typedef int (*TLSRecvCallbackFunc)(void* ctx, uint8_t* buf, int len);

struct ITLSPolicy {
	virtual void addref() = 0;
	virtual void delref() = 0;

	// set_cert_data should import the provided certificate list and
	// associate it with this policy. cert_data will point to a PEM
	// encoded certificate list, ordered such that each certificate
	// certifies the one before it.
	//
	// cert_data may additionally contain key information, which must
	// be ignored.
	//
	// set_cert_data should return true if the operation succeeded,
	// and false otherwise. After the first call to create_session for
	// a given policy, set_cert_data should immediately return false
	// if called.
	virtual bool set_cert_data(const uint8_t* cert_data, int cert_len) = 0;

	// set_key_data should import the provided private key and
	// associate it with this policy. key_data will point to a PEM
	// encoded key.
	//
	// key_data may additionally contain certificate information,
	// which must be ignored.
	//
	// set_key_data should return true if the operation succeeded, and
	// false otherwise. After the first call to create_session for a
	// given policy, set_key_data should immediately return false if
	// called.
	virtual bool set_key_data(const uint8_t* key_data, int key_len) = 0;

	// set_verify_peers should modify the validation rules for
	// verifying a peer during connection handshake. The format of
	// verify_peers is implementation specific.
	//
	// set_verify_peers should return true if the operation succeed,
	// and false otherwise. After the first call to create_session for
	// a given policy, set_verify_peers should immediately return
	// false if called.
	virtual bool set_verify_peers(const uint8_t* verify_peers, int verify_peers_len) = 0;

	// create_session should return a new object that implements
	// ITLSSession, associated with this policy. After the first call
	// to create_session for a given policy, further calls to
	// ITLSPolicy::set_* will fail and return false.
	//
	// The newly created session should use send_func and recv_func to
	// send and receive data on the underlying transport, and must
	// provide send_ctx/recv_ctx to the callbacks.
	//
	// uid should only be provided when invoking an ITLSLogFunc, which
	// will use it to identify this session.
	virtual ITLSSession* create_session(bool is_client, TLSSendCallbackFunc send_func, void* send_ctx, TLSRecvCallbackFunc recv_func, void* recv_ctx, void* uid ) = 0;
};

// Logs a message/error to the appropriate trace log.
//
// event must be a valid XML attribute value. uid may be NULL or the
// uid provided to ITLSPolicy::create_session by the caller. is_error
// should be true for errors and false for informational messages. The
// remaining arguments must be pairs of (const char*); the first of
// each pair must be a valid XML attribute name, and the second a
// valid XML attribute value. The final parameter must be NULL.
typedef void (*ITLSLogFunc)(const char* event, void* uid, bool is_error, ...);

struct ITLSPlugin {
	virtual void addref() = 0;
	virtual void delref() = 0;

	// create_policy should return a new object that implements
	// ITLSPolicy.
	//
	// The newly created policy, and any session further created from
	// the policy, should use logf to log any messages or errors that
	// occur.
	virtual ITLSPolicy* create_policy( ITLSLogFunc logf ) = 0;

	static inline const char* get_plugin_type_name_and_version() { return "ITLSPlugin"; }
};

#endif /* FDB_ITLSPLUGIN_H */
