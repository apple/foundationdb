#ifndef LOG_H
#define LOG_H
#pragma once

#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/spdlog.h"

enum LogLevel { Debug, Info, Warn, WarnAlways, Error };

#define STR(arg) fmt::format("{}", arg)

class Log {
	std::shared_ptr<spdlog::logger> logger;

public:
    Log(std::string name) { logger = spdlog::get(name);
  }
	Log(std::string name, std::string path) {
		logger = spdlog::rotating_logger_mt(name, path, 1000000, 10);

		logger->flush_on(spdlog::level::info);
		logger->set_pattern("{\"Severity\": \"%l\", \"Time\": \"%E\", \"DateTime\": "
		                    "\"%Y-%m-%dT%T\", \"ThreadID\": \"%t\" \"Type\": \"%v\"} ");
	}
	static Log get(std::string name) { return Log(name); }
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

#endif
