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
cmake_minimum_required(VERSION 3.24.2)

# silence deprecation warnings in newer versions of cmake
if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()

project(foundationdb
  VERSION 7.3.65
  DESCRIPTION "FoundationDB is a scalable, fault-tolerant, ordered key-value store with full ACID transactions."
  HOMEPAGE_URL "http://www.foundationdb.org/"
  LANGUAGES C CXX ASM)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

message (STATUS "${PROJECT_SOURCE_DIR} ${PROJECT_BINARY_DIR}")
if("${PROJECT_SOURCE_DIR}" STREQUAL "${PROJECT_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are forbidden")
endif()

set(OPEN_FOR_IDE OFF CACHE BOOL "Open this in an IDE (won't compile/link)")

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  if (OPEN_FOR_IDE)
    message(STATUS "Defaulting build type to 'Debug' for OPEN_FOR_IDE")
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Choose the type of build" FORCE)
  else()
    message(STATUS "Setting build type to 'Release' as none was specified")
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build" FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
      "MinSizeRel" "RelWithDebInfo")
  endif()
endif()

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)

################################################################################
# Packages used for bindings
################################################################################

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/")

################################################################################
# Compiler configuration
################################################################################

include(ConfigureCompiler)

################################################################################
# Components configuration
################################################################################

include(FDBComponents)

################################################################################
# Get repository information
################################################################################

add_custom_target(branch_file ALL DEPENDS ${CURR_BRANCH_FILE})
execute_process(
  COMMAND git rev-parse HEAD
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
  OUTPUT_VARIABLE CURRENT_GIT_VERSION_WNL
  OUTPUT_STRIP_TRAILING_WHITESPACE)
string(STRIP "${CURRENT_GIT_VERSION_WNL}" CURRENT_GIT_VERSION)
message(STATUS "Current git version ${CURRENT_GIT_VERSION}")

################################################################################
# Version information
################################################################################
if (FDB_RELEASE_CANDIDATE)
    set(FDB_RELEASE_CANDIDATE_VERSION 1 CACHE STRING "release candidate version")
    set(FDB_VERSION ${PROJECT_VERSION}-rc${FDB_RELEASE_CANDIDATE_VERSION})
else()
    set(FDB_VERSION ${PROJECT_VERSION})
endif()
if (NOT FDB_RELEASE)
    string(TIMESTAMP FDB_BUILD_TIMESTMAP %Y%m%d%H%M%S)
    # Adding support to pass custom fdb_build timestamp,
    # required to achieve uniform naming across different builds
    set(FDB_BUILDTIME "${FDB_BUILD_TIMESTMAP}" CACHE STRING "A timestamp for packages")
    set(FDB_BUILDTIME_STRING ".${FDB_BUILDTIME}")
    set(PRERELEASE_TAG "prerelease")
endif()
set(FDB_VERSION_PLAIN ${FDB_VERSION})
string(REPLACE "." ";" FDB_VERSION_LIST ${FDB_VERSION_PLAIN})
list(GET FDB_VERSION_LIST 0 FDB_MAJOR)
list(GET FDB_VERSION_LIST 1 FDB_MINOR)
list(GET FDB_VERSION_LIST 2 FDB_PATCH)
set(FDB_PACKAGE_NAME "${FDB_MAJOR}.${FDB_MINOR}")
configure_file(${CMAKE_SOURCE_DIR}/versions.target.cmake ${CMAKE_CURRENT_BINARY_DIR}/versions.target)
file(WRITE ${CMAKE_BINARY_DIR}/version.txt ${FDB_VERSION})

set(FDB_CURRENT_VERSION ${PROJECT_VERSION})
set(FDB_FUTURE_VERSION "7.4.0")
set(FDB_PREV_RELEASE_VERSION "7.1.33")
set(FDB_PREV2_RELEASE_VERSION "7.0.0")
set(FDB_PREV3_RELEASE_VERSION "6.3.25")

################################################################################
# Flow
################################################################################

include(utils)

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

if(CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
  include_directories(/usr/local/include)
endif()

include(CompileBoost)
include(GetMsgpack)
add_subdirectory(contrib)
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
add_subdirectory(fdbbackup)
add_subdirectory(metacluster)
add_subdirectory(tests)
add_subdirectory(flowbench EXCLUDE_FROM_ALL)
if(WITH_PYTHON AND WITH_C_BINDING)
  add_subdirectory(bindings)
endif()
if(WITH_DOCUMENTATION)
  add_subdirectory(documentation)
endif()

if(WIN32)
  add_subdirectory(packaging/msi)
else()
  include(CPack)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
  add_link_options(-lexecinfo)
endif()

################################################################################
# process compile commands for IDE
################################################################################
if (CMAKE_EXPORT_COMPILE_COMMANDS AND WITH_PYTHON)
  add_custom_command(
	  OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/compile_commands.json
	  COMMAND $<TARGET_FILE:Python3::Interpreter> ${CMAKE_CURRENT_SOURCE_DIR}/contrib/gen_compile_db.py
	  ARGS -b ${CMAKE_CURRENT_BINARY_DIR} -s ${CMAKE_CURRENT_SOURCE_DIR} -o ${CMAKE_CURRENT_SOURCE_DIR}/compile_commands.json ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json
	  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/contrib/gen_compile_db.py ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json
	  COMMENT "Build compile commands for IDE"
	)
  add_custom_target(processed_compile_commands ALL DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/compile_commands.json ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json)
endif()

################################################################################
# Inform user which components we are going to build
################################################################################

print_components()

message(STATUS "CPACK_COMPONENTS_ALL ${CPACK_COMPONENTS_ALL}")
