cmake_minimum_required(VERSION 3.5)
project(polevault)

set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${PROJECT_SOURCE_DIR}/cmake")
message (STATUS "${PROJECT_SOURCE_DIR} ${PROJECT_BINARY_DIR}")

if("${PROJECT_SOURCE_DIR}" STREQUAL "${PROJECT_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are forbidden and unsupported")
endif()

#file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/generated)

set(CMAKE_BUILD_TYPE Debug)

add_compile_options(-msse4.2)
add_compile_options(-ggdb)
add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-std=c++17>)

#add_compile_options(-pthread)

set(USE_ASAN OFF CACHE BOOL "Compile with address sanitizer")
if(USE_ASAN)
  set(SANITIZE_FLAGS "-fno-omit-frame-pointer")
  add_compile_options(
    -fno-omit-frame-pointer
    -DUSE_ASAN)
  set(SANITIZE_FLAGS "${SANITIZE_FLAGS} -fsanitize=address")
  add_compile_options(-fsanitize=address)
endif()
      
#TODO : go to fedora 28 and cmake 3.11 to use target_link_options
#target_link_options(protocol_test -fsanitize=address)
    set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} ")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${SANITIZE_FLAGS}")
    set(CMAKE_EXE_LINKER_FLAGS    "${CMAKE_EXE_LINKER_FLAGS}    ${SANITIZE_FLAGS} ${CMAKE_THREAD_LIBS_INIT}")


include(CompileBoost)
include(CompileFlatBuffers)
#include(CompileFMT) #already included in SPDLOG
include(DownloadSPDLOG)
find_package(Threads REQUIRED)

add_custom_command(PRE_BUILD
  OUTPUT ConsumerAdapterProtocol_generated.h
  COMMAND ${FLATC} --cpp --gen-mutable ${CMAKE_CURRENT_SOURCE_DIR}/ConsumerAdapterProtocol.fbs
  DEPENDS  ${CMAKE_CURRENT_SOURCE_DIR}/ConsumerAdapterProtocol.fbs
  DEPENDS flatbuffers)
  #WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
add_custom_target(ConsumerAdapterGen DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/ConsumerAdapterProtocol_generated.h)



# (Finally) get files to find and include generated .h files
include_directories(${CMAKE_CURRENT_BINARY_DIR}) 
include_directories(${flatbuffers_PREFIX}/include)
include_directories(${SPDLOG_INCLUDE})

set(SRCS
  ConsumerAdapter.h
  ConsumerAdapter.cpp
  ConsumerAdapterUtils.h
  ConsumerAdapterUtils.cpp
  ConsumerClient.h
  ConsumerClient.cpp
  ConsumerClientFDB6.cpp
  Crc32.h
  Crc32.cpp
  EndpointLoadGenerator.h
  EndpointLoadGenerator.cpp
  Log.h
  ProducerFuzz.h
  ProducerFuzz.cpp
)

set(PROTOTEST_SRCS
  ${SRCS}
  protocol_test.cpp)
set(MAIN_SRCS
  ${SRCS}
  adapter_main.cpp)

if(NOT FDB_API_VERSION)
  message(STATUS "Setting FDB API version to 610")
  set(FDB_API_VERSION 610)
endif()

set(CMAKE_SKIP_BUILD_RPATH TRUE)

set(FDB_LIB_DIR ${FDB_DIR}/usr/lib64)
set(FDB_INCLUDE_DIR ${FDB_DIR}/usr/include)
set(PROJECT_LINK_LIBS fdb_c)

link_directories(${FDB_LIB_DIR})
include_directories(${FDB_INCLUDE_DIR})

message(STATUS "Link directories ${FDB_LIB_DIR}")
message(STATUS "include directories ${FDB_INCLUDE_DIR}")

add_executable(protocol_test ${PROTOTEST_SRCS})

message(STATUS "target link libs: ${PROJECT_LINK_LIBS}")
target_link_libraries(protocol_test PUBLIC Threads::Threads boost_system spdlog -pthread ${PROJECT_LINK_LIBS})
target_compile_options(protocol_test PUBLIC -g -fPIC -O2 -DFDB_API_VERSION=${FDB_API_VERSION})
add_dependencies(protocol_test ConsumerAdapterGen)

add_executable(consumer_adapter ${MAIN_SRCS})

target_link_libraries(consumer_adapter PUBLIC Threads::Threads boost_system spdlog -pthread ${PROJECT_LINK_LIBS})
target_compile_options(consumer_adapter PUBLIC -g -fPIC -O2 -DFDB_API_VERSION=${FDB_API_VERSION})
add_dependencies(consumer_adapter ConsumerAdapterGen)


