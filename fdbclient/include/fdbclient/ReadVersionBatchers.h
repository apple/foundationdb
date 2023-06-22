/**
 * ReadVersionBatchers.h
 */

#pragma once

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/PImpl.h"

class ReadVersionBatchers {
	PImpl<class ReadVersionBatchersImpl> impl;

public:
	ReadVersionBatchers(int capacity, double expirationTimeout, double cleaningInterval);
	~ReadVersionBatchers();
	Future<GetReadVersionReply> getReadVersion(Database,
	                                           TransactionPriority,
	                                           uint32_t flags,
	                                           Optional<TenantGroupName> const&,
	                                           SpanContext,
	                                           TagSet,
	                                           Optional<UID> debugID);
};
