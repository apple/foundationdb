//
// Created by Xiaoxi Wang on 5/18/22.
//

#ifndef FOUNDATIONDB_DEBUGTRACE_H
#define FOUNDATIONDB_DEBUGTRACE_H

#define DebugTraceEvent(enable, ...) enable&& TraceEvent(__VA_ARGS__)

constexpr bool debugLogTraces = true;
#define DebugLogTraceEvent(...) DebugTraceEvent(debugLogTraces, __VA_ARGS__)

#endif // FOUNDATIONDB_DEBUGTRACE_H
