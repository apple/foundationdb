#include "fdbserver/FDBExecArgs.h"
#include "flow/Trace.h"
#include "flow/flow.h"

ExecCmdValueString::ExecCmdValueString(StringRef pCmdValueString) {
	cmdValueString = pCmdValueString;
	parseCmdValue();
}

void ExecCmdValueString::setCmdValueString(StringRef pCmdValueString) {
	// reset everything
	binaryPath = StringRef();
	keyValueMap.clear();

	// set the new cmdValueString
	cmdValueString = pCmdValueString;

	// parse it out
	parseCmdValue();
}

StringRef ExecCmdValueString::getCmdValueString() {
	return cmdValueString.toString();
}

StringRef ExecCmdValueString::getBinaryPath() {
	return binaryPath;
}

VectorRef<StringRef> ExecCmdValueString::getBinaryArgs() {
	return binaryArgs;
}

StringRef ExecCmdValueString::getBinaryArgValue(StringRef key) {
	StringRef res;
	if (keyValueMap.find(key) != keyValueMap.end()) {
		res = keyValueMap[key];
	}
	return res;
}

void ExecCmdValueString::parseCmdValue() {
	StringRef param = this->cmdValueString;
	const uint8_t* ptr = param.begin();
	int p = 0;
	int pSemiColon = 0;
	{
		// get the binary path
		while (*(ptr + pSemiColon) != ':' && (ptr + pSemiColon) < param.end()) {
			pSemiColon++;
		}
		this->binaryPath = param.substr(p, pSemiColon - p);
	}

	// no arguments provided
	if ((ptr + pSemiColon) >= param.end()) {
		return;
	}

	p = pSemiColon + 1;

	{
		// extract the arguments
		for (; p <= param.size();) {
			int pComma = p;
			while (*(ptr + pComma) != ',' && (ptr + pComma) < param.end()) {
				pComma++;
			}
			StringRef token = param.substr(p, pComma - p);
			this->binaryArgs.push_back(this->binaryArgs.arena(), token);
			{
				// parse the token to get key,value
				int idx = 0;
				int pEqual = 0;
				const uint8_t* tokenPtr = token.begin();
				while (*(tokenPtr + pEqual) != '='
					   && (tokenPtr + pEqual) < token.end()) {
					pEqual++;
				}
				StringRef key = token.substr(idx, pEqual - idx);
				StringRef value;
				if (pEqual < token.size() - 1) {
					value = token.substr(pEqual + 1);
				}
				keyValueMap.insert(std::pair<StringRef, StringRef>(key, value));
			}
			p = pComma + 1;
		}
	}
	return;
}

void ExecCmdValueString::dbgPrint() {
	auto te = TraceEvent("ExecCmdValueString");

	te.detail("CmdValueString", cmdValueString.toString());
	te.detail("BinaryPath", binaryPath.toString());

	int i = 0;
	for (auto elem : binaryArgs) {
		te.detail(format("Arg", ++i).c_str(), elem.toString());
	}
	return;
}
