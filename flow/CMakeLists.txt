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
  ITrace.h
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
  Platform.actor.cpp
  Platform.actor.h
  Platform.h
  Profiler.actor.cpp
  Profiler.h
  SendBufferIterator.h
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
  ThreadHelper.actor.cpp
  ThreadPrimitives.cpp
  ThreadPrimitives.h
  ThreadSafeQueue.h
  Trace.cpp
  Trace.h
  Tracing.h
  Tracing.actor.cpp
  TreeBenchmark.h
  UnitTest.cpp
  UnitTest.h
  WriteOnlySet.actor.cpp
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
  rte_memcpy.h
  serialize.cpp
  serialize.h
  stacktrace.h
  test_memcpy.cpp
  test_memcpy_perf.cpp
  version.cpp
  xxhash.c
  xxhash.h)

add_library(stacktrace stacktrace.amalgamation.cpp stacktrace.h)
if (USE_ASAN)
  target_compile_definitions(stacktrace PRIVATE ADDRESS_SANITIZER)
elseif(USE_MSAN)
  target_compile_definitions(stacktrace PRIVATE MEMORY_SANITIZER)
elseif(USE_UBSAN)
  target_compile_definitions(stacktrace PRIVATE UNDEFINED_BEHAVIOR_SANITIZER)
elseif(USE_TSAN)
  target_compile_definitions(stacktrace PRIVATE THREAD_SANITIZER DYNAMIC_ANNOTATIONS_EXTERNAL_IMPL=1)
endif()

if(UNIX AND NOT APPLE)
  list(APPEND FLOW_SRCS folly_memcpy.S)
endif()

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/SourceVersion.h.cmake ${CMAKE_CURRENT_BINARY_DIR}/SourceVersion.h)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/config.h.cmake ${CMAKE_CURRENT_BINARY_DIR}/config.h)

add_flow_target(STATIC_LIBRARY NAME flow SRCS ${FLOW_SRCS})
target_link_libraries(flow PRIVATE stacktrace)
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
target_link_libraries(flow PUBLIC Threads::Threads ${CMAKE_DL_LIBS})
if(USE_SANITIZER)
  target_link_libraries(flow PUBLIC boost_asan)
else()
  target_link_libraries(flow PUBLIC boost_target)
endif()
if(USE_VALGRIND)
  target_link_libraries(flow PUBLIC Valgrind)
endif()

if(APPLE)
  find_library(IO_KIT IOKit)
  find_library(CORE_FOUNDATION CoreFoundation)
  target_link_libraries(flow PRIVATE ${IO_KIT} ${CORE_FOUNDATION})
endif()
