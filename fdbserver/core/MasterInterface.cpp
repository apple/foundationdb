#include "fdbserver/core/MasterInterface.h"

template class ReplyPromise<MasterInterface>;
template struct NetSAV<MasterInterface>;
