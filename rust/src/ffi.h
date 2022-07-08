#pragma once
#include "fdbserver/WorkerInterface.actor.h"
#include <rust/cxx.h>

using namespace rust::cxxbridge1;

std::unique_ptr<RegisterWorkerRequest> hello_world(Slice<const uint8_t> s);
