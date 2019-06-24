#include "workloads.h"

enum LogLevel {
  Debug = FDBSeverity::Debug,
  Info = FDBSeverity::Info,
  Warn = FDBSeverity::Warn,
  WarnAlways = FDBSeverity::WarnAlways,
  Error = FDBSeverity::Error
};

#define STR(arg) fmt::format("{}", arg)

class Log {
  static FDBWorkloadContext *context = nullptr;
  void trace(const std::string &name) { trace(LogLevel::Info, name, {}); }
  void trace(const std::string &name,
             const std::vector<std::pair<std::string, std::string>> &details) {
    trace(LogLevel::Info, name, details);
  }
  void trace(LogLevel lvl, const std::string &name) { trace(lvl, name, {}); }
  void trace(LogLevel lvl, const std::string &name,
             const std::vector<std::pair<std::string, std::string>> &details) {
    context->trace(lvl, name, details);
  }
}
