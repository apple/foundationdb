// Apple Proprietary and Confidential Information

#ifndef FDB_LIBTLS_PLUGIN_H
#define FDB_LIBTLS_PLUGIN_H

#pragma once

#include "ITLSPlugin.h"
#include "ReferenceCounted.h"

#include <tls.h>

struct FDBLibTLSPlugin : ITLSPlugin, ReferenceCounted<FDBLibTLSPlugin> {
	FDBLibTLSPlugin();
	virtual ~FDBLibTLSPlugin();

	virtual void addref() { ReferenceCounted<FDBLibTLSPlugin>::addref(); }
	virtual void delref() { ReferenceCounted<FDBLibTLSPlugin>::delref(); }

	virtual ITLSPolicy *create_policy(ITLSLogFunc logf);

	int rc;
};

#endif /* FDB_LIBTLS_PLUGIN_H */
