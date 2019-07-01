#ifndef SIMLOG_H
#define SIMLOG_H
#pragma once

#include "../workloads.h"
#include "fmt/format.h"

typedef FDBSeverity LogLevel;


#define STR(arg) fmt::format("{}", arg)

class Log {
	FDBWorkloadContext* context = nullptr;
    // DLL won't let me create a static singleton here.  Get 'variable <> has internal linkage but is not defined' error
    // static std::shared_ptr<Log> m_log;

public:
	Log(FDBWorkloadContext* c) { context = c; }
	void trace(const std::string& name) { trace(LogLevel::Info, name, {}); }
	void trace(const std::string& name, const std::vector<std::pair<std::string, std::string>>& details) {
		trace(LogLevel::Info, name, details);
	}
	void trace(LogLevel lvl, const std::string& name) { trace(lvl, name, {}); }
	void trace(LogLevel lvl, const std::string& name, const std::vector<std::pair<std::string, std::string>>& details) {
		context->trace(lvl, name, details);
	}
};

#endif
