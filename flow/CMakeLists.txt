find_package(Threads REQUIRED)

set(FLOW_SRCS
  ActorCollection.actor.cpp
  ActorCollection.h
  Arena.h
  AsioReactor.h
  CompressedInt.actor.cpp
  CompressedInt.h
  Deque.cpp
  Deque.h
  DeterministicRandom.h
  Error.cpp
  Error.h
  EventTypes.actor.h
  FastAlloc.cpp
  FastAlloc.h
  FastRef.h
  FaultInjection.cpp
  FaultInjection.h
  FileTraceLogWriter.cpp
  FileTraceLogWriter.h
  Hash3.c
  Hash3.h
  IDispatched.h
  IRandom.h
  IThreadPool.cpp
  IThreadPool.h
  IndexedSet.actor.h
  IndexedSet.cpp
  IndexedSet.h
  JsonTraceLogFormatter.cpp
  JsonTraceLogFormatter.h
  Knobs.cpp
  Knobs.h
  MetricSample.h
  Net2.actor.cpp
  Net2Packet.cpp
  Net2Packet.h
  Platform.cpp
  Platform.h
  Profiler.actor.cpp
  Profiler.h
  SignalSafeUnwind.cpp
  SignalSafeUnwind.h
  SimpleOpt.h
  Stats.actor.cpp
  Stats.h
  SystemMonitor.cpp
  SystemMonitor.h
  TDMetric.actor.h
  TDMetric.cpp
  ThreadHelper.actor.h
  ThreadHelper.cpp
  ThreadPrimitives.cpp
  ThreadPrimitives.h
  ThreadSafeQueue.h
  Trace.cpp
  Trace.h
  UnitTest.cpp
  UnitTest.h
  XmlTraceLogFormatter.h
  XmlTraceLogFormatter.cpp
  actorcompiler.h
  boost.cpp
  error_definitions.h
  flow.cpp
  flow.h
  genericactors.actor.cpp
  genericactors.actor.h
  network.cpp
  network.h
  serialize.h
  stacktrace.amalgamation.cpp
  stacktrace.h
  version.cpp)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/hgVersion.h.cmake ${CMAKE_CURRENT_BINARY_DIR}/hgVersion.h)

actor_set(FLOW_BUILD "${FLOW_SRCS}")
add_library(flow STATIC ${FLOW_BUILD})
actor_compile(flow "${FLOW_SRCS}")
target_include_directories(flow SYSTEM PUBLIC ${CMAKE_THREAD_LIBS_INIT})
target_include_directories(flow PUBLIC ${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_SOURCE_DIR})
if (NOT APPLE)
  set (FLOW_LIBS ${FLOW_LIBS} rt)
endif()
target_link_libraries(flow PRIVATE ${FLOW_LIBS})
target_link_libraries(flow PUBLIC boost_target Threads::Threads ${CMAKE_DL_LIBS})
target_compile_definitions(flow PUBLIC TLS_DISABLED)

if(APPLE)
  find_library(IO_KIT IOKit)
  find_library(CORE_FOUNDATION CoreFoundation)
  target_link_libraries(flow PRIVATE ${IO_KIT} ${CORE_FOUNDATION})
endif()

