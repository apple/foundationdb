#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/CoordinationInterface.h"

// Instantiate ClientDBInfo related tempates
template class ReplyPromise<struct ClientDBInfo>;
template class ReplyPromise<CachedSerialization<ClientDBInfo>>;

// Instantiate OpenDatabaseCoordRequest related templates
template struct NetNotifiedQueue<OpenDatabaseCoordRequest, true>;

// Instantiate GetKeyServerLocationsReply related templates
template class ReplyPromise<GetKeyServerLocationsReply>;
template struct NetSAV<GetKeyServerLocationsReply>;