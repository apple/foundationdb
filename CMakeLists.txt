#
# CMakeLists.txt
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
cmake_minimum_required(VERSION 3.12)
project(foundationdb
  VERSION 6.2.30
  DESCRIPTION "FoundationDB is a scalable, fault-tolerant, ordered key-value store with full ACID transactions."
  HOMEPAGE_URL "http://www.foundationdb.org/"
  LANGUAGES C CXX ASM)

set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${PROJECT_SOURCE_DIR}/cmake")
message (STATUS "${PROJECT_SOURCE_DIR} ${PROJECT_BINARY_DIR}")
if("${PROJECT_SOURCE_DIR}" STREQUAL "${PROJECT_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are forbidden")
endif()

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to 'Release' as none was specified")
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build" FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
    "MinSizeRel" "RelWithDebInfo")
endif()

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)

set(OPEN_FOR_IDE OFF CACHE BOOL "Open this in an IDE (won't compile/link)")

################################################################################
# Packages used for bindings
################################################################################

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/")

################################################################################
# Compiler configuration
################################################################################

include(ConfigureCompiler)

################################################################################
# Compiler configuration
################################################################################

include(FDBComponents)

################################################################################
# Get repository information
################################################################################

add_custom_target(branch_file ALL DEPENDS ${CURR_BRANCH_FILE})
execute_process(
  COMMAND git rev-parse HEAD
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
  OUTPUT_VARIABLE CURRENT_GIT_VERSION_WNL)
string(STRIP "${CURRENT_GIT_VERSION_WNL}" CURRENT_GIT_VERSION)
message(STATUS "Current git version ${CURRENT_GIT_VERSION}")

################################################################################
# Version information
################################################################################

if(NOT WIN32)
  add_custom_target(version_file ALL DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/versions.target)
  execute_process(
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/build/get_version.sh ${CMAKE_CURRENT_SOURCE_DIR}/versions.target
    OUTPUT_VARIABLE FDB_VERSION_WNL)
  execute_process(
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/build/get_package_name.sh ${CMAKE_CURRENT_SOURCE_DIR}/versions.target
    OUTPUT_VARIABLE FDB_PACKAGE_NAME_WNL)
  string(STRIP "${FDB_VERSION_WNL}" FDB_VERSION_TARGET_FILE)
  string(STRIP "${FDB_PACKAGE_NAME_WNL}" FDB_PACKAGE_NAME_TARGET_FILE)
endif()

set(USE_VERSIONS_TARGET OFF CACHE BOOL "Use the deprecated versions.target file")
if(USE_VERSIONS_TARGET)
  if (WIN32)
    message(FATAL_ERROR "USE_VERSION_TARGET us not supported on Windows")
  endif()
  set(FDB_VERSION ${FDB_VERION_TARGET_FILE})
  set(FDB_PACKAGE_NAME ${FDB_PACKAGE_NAME_TARGET_FILE})
  set(FDB_VERSION_PLAIN ${FDB_VERSION})
else()
  set(FDB_PACKAGE_NAME "${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}")
  set(FDB_VERSION ${PROJECT_VERSION})
  set(FDB_VERSION_PLAIN ${FDB_VERSION})
  if(NOT WIN32)
    # we need to assert that the cmake version is in sync with the target version
    if(NOT (FDB_VERSION STREQUAL FDB_VERSION_TARGET_FILE))
      message(SEND_ERROR "The project version in cmake is set to ${FDB_VERSION},\
 but versions.target has it at ${FDB_VERSION_TARGET_FILE}")
    endif()
    if(NOT (FDB_PACKAGE_NAME STREQUAL FDB_PACKAGE_NAME_TARGET_FILE))
      message(SEND_ERROR "The package name in cmake is set to ${FDB_PACKAGE_NAME},\
 but versions.target has it set to ${FDB_PACKAGE_NAME_TARGET_FILE}")
    endif()
  endif()
endif()

message(STATUS "FDB version is ${FDB_VERSION}")
message(STATUS "FDB package name is ${FDB_PACKAGE_NAME}")
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/versions.h.cmake ${CMAKE_CURRENT_BINARY_DIR}/fdbclient/versions.h)


################################################################################
# Flow
################################################################################

# Flow and other tools are written in C# - so we need that dependency
include(EnableCsharp)

# First thing we need is the actor compiler - and to compile and run the
# actor compiler, we need mono
include(CompileActorCompiler)

include(CompileCoverageTool)

# with the actor compiler, we can now make the flow commands available
include(FlowCommands)

################################################################################
# Vexilographer
################################################################################

include(CompileVexillographer)

################################################################################
# Generate config file
################################################################################

string(RANDOM LENGTH 8 description1)
string(RANDOM LENGTH 8 description2)
set(CLUSTER_DESCRIPTION1 ${description1} CACHE STRING "Cluster description")
set(CLUSTER_DESCRIPTION2 ${description2} CACHE STRING "Cluster description")

configure_file(fdb.cluster.cmake ${CMAKE_CURRENT_BINARY_DIR}/fdb.cluster)


################################################################################
# testing
################################################################################
enable_testing()

################################################################################
# Directory structure
################################################################################

include(cmake/InstallLayout.cmake)

################################################################################
# Random seed
################################################################################

string(RANDOM LENGTH 8 ALPHABET "0123456789abcdef" SEED_)
set(SEED "0x${SEED_}" CACHE STRING "Random seed for testing")

################################################################################
# components
################################################################################

include(CompileBoost)
add_subdirectory(flow)
add_subdirectory(fdbrpc)
add_subdirectory(fdbclient)
add_subdirectory(fdbserver)
add_subdirectory(fdbcli)
if(NOT WIN32)
  add_subdirectory(fdbmonitor)
else()
  add_subdirectory(fdbservice)
endif()
if(WITH_PYTHON)
  add_subdirectory(bindings)
endif()
add_subdirectory(fdbbackup)
add_subdirectory(contrib)
add_subdirectory(tests)
if(WITH_DOCUMENTATION)
  add_subdirectory(documentation)
endif()
add_subdirectory(monitoring)

if(WIN32)
  add_subdirectory(packaging/msi)
else()
  include(CPack)
endif()

################################################################################
# process compile commands for IDE
################################################################################

if (CMAKE_EXPORT_COMPILE_COMMANDS AND WITH_PYTHON)
  add_custom_command(
	  OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/compile_commands.json
	  COMMAND $<TARGET_FILE:Python::Interpreter> ${CMAKE_CURRENT_SOURCE_DIR}/build/gen_compile_db.py
	  ARGS -b ${CMAKE_CURRENT_BINARY_DIR} -s ${CMAKE_CURRENT_SOURCE_DIR} -o ${CMAKE_CURRENT_SOURCE_DIR}/compile_commands.json ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json
	  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/build/gen_compile_db.py ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json
	  COMMENT "Build compile commands for IDE"
	)
  add_custom_target(processed_compile_commands ALL DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/compile_commands.json ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json)
endif()

################################################################################
# Inform user which components we are going to build
################################################################################

print_components()

message(STATUS "CPACK_COMPONENTS_ALL ${CPACK_COMPONENTS_ALL}")
