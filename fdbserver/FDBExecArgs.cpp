#include "fdbserver/FDBExecArgs.h"
#include <flow/Trace.h>
#include <flow/flow.h>

ExecCmdValueString::ExecCmdValueString(std::string const& pCmdValueString) {
	cmdValueString = pCmdValueString;
	parseCmdValue();
}

void ExecCmdValueString::setCmdValueString(std::string const& pCmdValueString) {
	// reset everything
	binaryPath = "";
	binaryArgs.clear();
	keyValueMap.clear();

	// set the new cmdValueString
	cmdValueString = pCmdValueString;

	// parse it out
	parseCmdValue();
}

std::string ExecCmdValueString::getBinaryPath() {
	return binaryPath;
}

std::vector<std::string> ExecCmdValueString::getBinaryArgs() {
	return binaryArgs;
}

std::string ExecCmdValueString::getBinaryArgValue(const std::string& key) {
	std::string res;
	if (keyValueMap.find(key) != keyValueMap.end()) {
		res = keyValueMap[key];
	}
	return res;
}

void ExecCmdValueString::parseCmdValue() {
	int p = 0;
	int pSemiColon = 0;
	std::string const& param = this->cmdValueString;
	{
		// get the binary path
		pSemiColon = param.find_first_of(':', p);
		if (pSemiColon == param.npos) {
			pSemiColon = param.size();
		}
		this->binaryPath = param.substr(p, pSemiColon - p);
	}

	// no arguments provided
	if (pSemiColon >= param.size() - 1) {
		return;
	}

	p = pSemiColon + 1;

	{
		// extract the arguments
		for (; p <= param.size();) {
			int pComma = param.find_first_of(',', p);
			if (pComma == param.npos) {
				pComma = param.size();
			}
			std::string token = param.substr(p, pComma - p);
			this->binaryArgs.push_back(token);
			{
				// parse the token to get key,value
				int idx = 0;
				int pEqual = token.find_first_of('=', idx);
				if (pEqual == token.npos) {
					pEqual = token.size();
				}
				std::string key = token.substr(idx, pEqual - idx);

				std::string value;
				if (pEqual < token.size() - 1) {
					value = token.substr(pEqual + 1);
				}
				keyValueMap.insert(std::pair<std::string, std::string>(key, value));
			}
			p = pComma + 1;
		}
	}
	return;
}

void ExecCmdValueString::dbgPrint() {
	auto te = TraceEvent("execCmdValueString");

	te.detail("cmdValueString", cmdValueString);
	te.detail("binaryPath", binaryPath);

	int i = 0;
	for (auto elem : binaryArgs) {
		te.detail(format("arg{}", ++i).c_str(), elem);
	}
	return;
}
