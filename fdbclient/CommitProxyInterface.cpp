#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/GetEncryptCipherKeys_impl.actor.h"

// Instantiate ClientDBInfo related templates
template class ReplyPromise<struct ClientDBInfo>;
template class ReplyPromise<CachedSerialization<ClientDBInfo>>;
template class GetEncryptCipherKeys<ClientDBInfo>;

// Instantiate OpenDatabaseCoordRequest related templates
template struct NetNotifiedQueue<OpenDatabaseCoordRequest, true>;

// Instantiate GetKeyServerLocationsReply related templates
template class ReplyPromise<GetKeyServerLocationsReply>;
template struct NetSAV<GetKeyServerLocationsReply>;