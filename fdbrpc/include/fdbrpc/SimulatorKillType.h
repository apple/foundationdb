#ifndef FDBRPC_SIMULATOR_KILLTYPE_H
#define FDBRPC_SIMULATOR_KILLTYPE_H

namespace simulator {

// Order matters!
enum KillType {
	KillInstantly,
	InjectFaults,
	FailDisk,
	RebootAndDelete,
	RebootProcessAndDelete,
	RebootProcessAndSwitch, // Reboot and switch cluster file
	Reboot,
	RebootProcess,
	None
};

} // namespace simulator

#endif // FDBRPC_SIMULATOR_KILLTYPE_H