find_package(Threads REQUIRED)

set(FLOW_SRCS
  ActorCollection.actor.cpp
  ActorCollection.h
  Arena.cpp
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
  Histogram.cpp
  Histogram.h
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
  SystemMonitor.cpp
  SystemMonitor.h
  TDMetric.actor.h
  TDMetric.cpp
  TLSConfig.actor.cpp
  TLSConfig.actor.h
  ThreadHelper.actor.h
  ThreadHelper.cpp
  ThreadPrimitives.cpp
  ThreadPrimitives.h
  ThreadSafeQueue.h
  Trace.cpp
  Trace.h
  TreeBenchmark.h
  UnitTest.cpp
  UnitTest.h
  XmlTraceLogFormatter.cpp
  XmlTraceLogFormatter.h
  actorcompiler.h
	crc32c.h
  crc32c.cpp
  error_definitions.h
  ${CMAKE_CURRENT_BINARY_DIR}/SourceVersion.h
  flat_buffers.cpp
  flat_buffers.h
  flow.cpp
  flow.h
  genericactors.actor.cpp
  genericactors.actor.h
  network.cpp
  network.h
  serialize.cpp
  serialize.h
  stacktrace.amalgamation.cpp
  stacktrace.h
  version.cpp
  xxhash.c
  xxhash.h)

if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
  list(APPEND FLOW_SRCS aarch64/memcmp.S aarch64/memcpy.S)
endif()

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/SourceVersion.h.cmake ${CMAKE_CURRENT_BINARY_DIR}/SourceVersion.h)

add_flow_target(STATIC_LIBRARY NAME flow SRCS ${FLOW_SRCS})
if (NOT APPLE AND NOT WIN32)
  set (FLOW_LIBS ${FLOW_LIBS} rt)
elseif(WIN32)
  target_link_libraries(flow PUBLIC winmm.lib)
  target_link_libraries(flow PUBLIC psapi.lib)
endif()
if(CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
  set (FLOW_LIBS ${FLOW_LIBS} execinfo devstat)
  find_library(EIO eio)
  if(EIO)
    target_link_libraries(flow PUBLIC ${EIO})
  endif()
endif()
target_link_libraries(flow PRIVATE ${FLOW_LIBS})
if(USE_VALGRIND)
  target_link_libraries(flow PUBLIC Valgrind)
endif()
# TODO(atn34) Re-enable TLS for OPEN_FOR_IDE build once #2201 is resolved
if(NOT WITH_TLS OR OPEN_FOR_IDE)
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
