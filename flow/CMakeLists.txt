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
  DeterministicRandom.cpp
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
  TLSPolicy.h
  TLSPolicy.cpp
  UnitTest.cpp
  UnitTest.h
  XmlTraceLogFormatter.h
  XmlTraceLogFormatter.cpp
  actorcompiler.h
  error_definitions.h
  flat_buffers.h
  flat_buffers.cpp
  flow.cpp
  flow.h
  genericactors.actor.cpp
  genericactors.actor.h
  network.cpp
  network.h
  serialize.h
  serialize.cpp
  stacktrace.amalgamation.cpp
  stacktrace.h
  version.cpp)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/hgVersion.h.cmake ${CMAKE_CURRENT_BINARY_DIR}/hgVersion.h)

add_flow_target(STATIC_LIBRARY NAME flow SRCS ${FLOW_SRCS})
target_include_directories(flow SYSTEM PUBLIC ${CMAKE_THREAD_LIBS_INIT})
target_include_directories(flow PUBLIC ${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_SOURCE_DIR})
if (NOT APPLE AND NOT WIN32)
  set (FLOW_LIBS ${FLOW_LIBS} rt)
elseif(WIN32)
  target_link_libraries(flow PUBLIC winmm.lib)
  target_link_libraries(flow PUBLIC psapi.lib)
endif()
target_link_libraries(flow PRIVATE ${FLOW_LIBS})
if(NOT WITH_TLS)
  target_compile_definitions(flow PUBLIC TLS_DISABLED)
else()
  target_link_libraries(flow PUBLIC OpenSSL::SSL)
endif()
target_link_libraries(flow PUBLIC boost_target Threads::Threads ${CMAKE_DL_LIBS})
if(USE_VALGRIND)
  target_link_libraries(flow PUBLIC Valgrind)
endif()

if(APPLE)
  find_library(IO_KIT IOKit)
  find_library(CORE_FOUNDATION CoreFoundation)
  target_link_libraries(flow PRIVATE ${IO_KIT} ${CORE_FOUNDATION})
endif()
