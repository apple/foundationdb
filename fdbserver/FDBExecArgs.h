#pragma once
#ifndef FDBCLIENT_EXECCMDARGS_H
#define FDBCLIENT_EXECCMDARGS_H
#include <string>
#include <vector>
#include <map>
#include "flow/Arena.h"

// execute/snapshot command takes two arguments: <param1> <param2>
// param1 - represents the command type/name
// param2 - takes a binary path followed by a set of arguments in the following
// format <binary-path>:<key1=val1>,<key2=val2>...
// this class will abstract the format and give functions to get various pieces
// of information
class ExecCmdValueString {
public: // ctor & dtor
	ExecCmdValueString() {}
	explicit ExecCmdValueString(StringRef cmdValueString);

public: // interfaces
	StringRef getBinaryPath();
	VectorRef<StringRef> getBinaryArgs();
	StringRef getBinaryArgValue(StringRef key);
	void setCmdValueString(StringRef cmdValueString);
	StringRef getCmdValueString(void);

public: // helper functions
	void dbgPrint();

private: // functions
	void parseCmdValue();

private: // data
	Standalone<StringRef> cmdValueString;
	Standalone<VectorRef<StringRef>> binaryArgs;
	StringRef binaryPath;
	std::map<StringRef, StringRef> keyValueMap;
};
#endif
