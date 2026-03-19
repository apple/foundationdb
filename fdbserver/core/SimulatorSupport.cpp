#include "fdbrpc/simulator.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "flow/network.h"

bool isSimulatorProcessUnreliable() {
	return g_network->isSimulated() && !g_simulator->getCurrentProcess()->isReliable();
}
