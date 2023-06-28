/**
 * ReadVersionBatchers.h
 */

#pragma once

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/PImpl.h"

// This class is used to batch read version requests from multiple transactions
// and send them to GRV proxies as GetReadVersionRequests.
// Batches are created based on the following restrictions:
//
// - Time: Batches are sent at time intervals determined dynamically based on reply latencies.
// - Size: Batches cannot exceed CLIENT_KNOBS->MAX_BATCH_SIZE requests.
// - ThrottlingIds: All transactions in a batch share the same throttling ID, because
//     they will receive the same throttling from the proxy.
// - Flags: All transactions in a batch, must share the same flags (including priority)
//
// A ReadVersionBatchers object can only hold a fixed number of concurrent actors responsible for
// batching requests. This fixed number is determined by the capacity parameter to the constructor.
// In order to avoid leaking actors that are no longer being used, a background expiration process
// cancels batching actors that have been idle for too long (as determined by the expirationTimeout
// parameter to the constructor). The background expiration process runs at an interval determined
// by the cleaningInterval parameter to the constructor.
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
	                                           Optional<TransactionTag> const&,
	                                           Optional<UID> debugID);
};
