// Apple Proprietary and Confidential Information

#ifndef FDB_LIBTLS_SESSION_H
#define FDB_LIBTLS_SESSION_H

#pragma once

#include "ITLSPlugin.h"
#include "ReferenceCounted.h"

#include "FDBLibTLSPolicy.h"

#include <tls.h>

struct FDBLibTLSSession : ITLSSession, ReferenceCounted<FDBLibTLSSession> {
	FDBLibTLSSession(Reference<FDBLibTLSPolicy> policy, bool is_client, TLSSendCallbackFunc send_func, void* send_ctx, TLSRecvCallbackFunc recv_func, void* recv_ctx, void* uid);
	virtual ~FDBLibTLSSession();

	virtual void addref() { ReferenceCounted<FDBLibTLSSession>::addref(); }
	virtual void delref() { ReferenceCounted<FDBLibTLSSession>::delref(); }

	bool check_criteria();

	virtual int handshake();
	virtual int read(uint8_t* data, int length);
	virtual int write(const uint8_t* data, int length);

	Reference<FDBLibTLSPolicy> policy;

	struct tls *tls_ctx;
	struct tls *tls_sctx;

	TLSSendCallbackFunc send_func;
	void* send_ctx;
	TLSRecvCallbackFunc recv_func;
	void* recv_ctx;

	bool handshake_completed;

	void* uid;
};

#endif /* FDB_LIBTLS_SESSION_H */
