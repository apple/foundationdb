#ifndef LOG_H
#define LOG_H
#pragma once

//#ifndef INGEST_ADAPTER_SIM_TEST

#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/spdlog.h"

enum LogLevel { Debug, Info, Warn, WarnAlways, Error };

#define STR(arg) fmt::format("{}", arg)

class Log {
	std::shared_ptr<spdlog::logger> logger;

public:
	Log() { logger = spdlog::get("pvTrace"); }
	void trace(const std::string& name) { trace(LogLevel::Info, name, {}); }
	void trace(const std::string& name, const std::vector<std::pair<std::string, std::string>>& details) {
		trace(LogLevel::Info, name, details);
	}
	void trace(LogLevel lvl, const std::string& name) { trace(lvl, name, {}); }
	void trace(LogLevel lvl, const std::string& name, const std::vector<std::pair<std::string, std::string>>& details) {
		std::string message = name;
		for (auto detail : details) {
			message += fmt::format("\", \"{}\": \"{}", detail.first,
			                       detail.second); // last quote added by pattern
		}
		switch (lvl) {
		case LogLevel::Debug:
			logger->debug(message);
			break;
		case LogLevel::Info:
			logger->info(message);
			break;
		case LogLevel::Warn:
		case LogLevel::WarnAlways:
			logger->warn(message);
			break;
		case LogLevel::Error:
			logger->error(message);
			break;
		}
	}
};

//#endif

#endif
