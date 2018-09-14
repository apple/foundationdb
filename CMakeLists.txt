#
# flow.h
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
cmake_minimum_required(VERSION 3.5)
project(fdb)

set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${PROJECT_SOURCE_DIR}/cmake")
message (STATUS "${PROJECT_SOURCE_DIR} ${PROJECT_BINARY_DIR}")
if("${PROJECT_SOURCE_DIR}" STREQUAL "${PROJECT_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are forbidden, unsupported, and stupid!!")
endif()

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to 'Release' as none was specified")
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build" FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
    "MinSizeRel" "RelWithDebInfo")
endif()

SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
SET(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)

################################################################################
# Packages used for bindings
################################################################################

include(UseJava)
find_package(JNI 1.8 REQUIRED)
find_package(Java 1.8 COMPONENTS Development REQUIRED)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/")

find_package(PythonInterp 3.4 REQUIRED)
set(Python_ADDITIONAL_VERSIONS 3.4 3.5 3.5)
find_package(PythonLibs 3.4 REQUIRED)


################################################################################
# Compiler configuration
################################################################################

set(USE_GPERFTOOLS OFF CACHE BOOL "Use gperfools for profiling")
set(PORTABLE_BINARY OFF CACHE BOOL "Create a binary that runs on older OS versions")
set(USE_VALGRIND OFF CACHE BOOL "Compile for valgrind usage")
set(USE_GOLD_LINKER OFF CACHE BOOL "Use gold linker")
set(ALLOC_INSTRUMENTATION OFF CACHE BOOL "Instrument alloc")
set(WITH_UNDODB OFF CACHE BOOL "Use rr or undodb")
set(OPEN_FOR_IDE OFF CACHE BOOL "Open this in an IDE (won't compile/link)")
set(FDB_RELEASE OFF CACHE BOOL "This is a building of a final release")

find_package(Threads REQUIRED)
if(ALLOC_INSTRUMENTATION)
  add_compile_options(-DALLOC_INSTRUMENTATION)
endif()
if(WITH_UNDODB)
  add_compile_options(-DWITH_UNDODB)
endif()
if(DEBUG_TASKS)
  add_compile_options(-DDEBUG_TASKS)
endif()

if(NDEBUG)
  add_compile_options(-DNDEBUG)
endif()

if(FDB_RELEASE)
  add_compile_options(-DFDB_RELEASE)
endif()

include_directories(${CMAKE_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})
if (NOT OPEN_FOR_IDE)
  add_definitions(-DNO_INTELLISENSE)
endif()
add_definitions(-DUSE_UCONTEXT)
enable_language(ASM)

include(CheckFunctionExists)
set(CMAKE_REQUIRED_INCLUDES stdlib.h malloc.h)
set(CMAKE_REQUIRED_LIBRARIES c)


if(WIN32)
  add_compile_options(/W3 /EHsc)
else()
  if(USE_GOLD_LINKER)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=gold -Wl,--disable-new-dtags")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=gold -Wl,--disable-new-dtags")
  endif()

  set(GCC NO)
  set(CLANG NO)
  if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
    set(CLANG YES)
  else()
    # This is not a very good test. However, as we do not really support many architectures
    # this is good enough for now
    set(GCC YES)
  endif()

  # we always compile with debug symbols. CPack will strip them out
  # and create a debuginfo rpm
  add_compile_options(-ggdb)
  set(USE_ASAN OFF CACHE BOOL "Compile with address sanitizer")
  if(USE_ASAN)
    add_compile_options(
      -fno-omit-frame-pointer -fsanitize=address
      -DUSE_ASAN)
    set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -fno-omit-frame-pointer -fsanitize=address")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fno-omit-frame-pointer -fsanitize=address")
    set(CMAKE_EXE_LINKER_FLAGS    "${CMAKE_EXE_LINKER_FLAGS}    -fno-omit-frame-pointer -fsanitize=address ${CMAKE_THREAD_LIBS_INIT}")
  endif()

  if(PORTABLE_BINARY)
    message(STATUS "Create a more portable binary")
    set(CMAKE_MODULE_LINKER_FLAGS "-static-libstdc++ -static-libgcc ${CMAKE_MODULE_LINKER_FLAGS}")
    set(CMAKE_SHARED_LINKER_FLAGS "-static-libstdc++ -static-libgcc ${CMAKE_SHARED_LINKER_FLAGS}")
    set(CMAKE_EXE_LINKER_FLAGS    "-static-libstdc++ -static-libgcc ${CMAKE_EXE_LINKER_FLAGS}")
  endif()
  # Instruction sets we require to be supported by the CPU
  add_compile_options(
    -maes
    -mmmx
    -mavx
    -msse4.2)
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-std=c++14>)
  if (USE_VALGRIND)
    add_compile_options(-DVALGRIND -DUSE_VALGRIND)
  endif()
  if (CLANG)
    if (APPLE)
      add_compile_options(-stdlib=libc++)
    endif()
    add_compile_options(
      -Wno-unknown-warning-option
      -Wno-dangling-else
      -Wno-sign-compare
      -Wno-comment
      -Wno-unknown-pragmas
      -Wno-delete-non-virtual-dtor
      -Wno-undefined-var-template
      -Wno-unused-value
      -Wno-tautological-pointer-compare
      -Wno-format)
  endif()
  if (CMAKE_GENERATOR STREQUAL Xcode)
  else()
    add_compile_options(-Werror)
  endif()
  add_compile_options($<$<BOOL:${GCC}>:-Wno-pragmas>)
  add_compile_options(-Wno-error=format
    -Wno-deprecated
    -fvisibility=hidden
    -Wreturn-type
    -fdiagnostics-color=always
    -fPIC)

  if(CMAKE_COMPILER_IS_GNUCXX)
    set(USE_LTO OFF CACHE BOOL "Do link time optimization")
    if (USE_LTO)
      add_compile_options($<$<CONFIG:Release>:-flto>)
      set(CMAKE_AR  "gcc-ar")
      set(CMAKE_C_ARCHIVE_CREATE "<CMAKE_AR> qcs <TARGET> <LINK_FLAGS> <OBJECTS>")
      set(CMAKE_C_ARCHIVE_FINISH   true)
      set(CMAKE_CXX_ARCHIVE_CREATE "<CMAKE_AR> qcs <TARGET> <LINK_FLAGS> <OBJECTS>")
      set(CMAKE_CXX_ARCHIVE_FINISH   true)
    endif()
  endif()
endif()

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

add_custom_target(version_file ALL DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/versions.target)
execute_process(
  COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/build/get_version.sh ${CMAKE_CURRENT_SOURCE_DIR}/versions.target
  OUTPUT_VARIABLE FDB_VERSION_WNL)
execute_process(
  COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/build/get_package_name.sh ${CMAKE_CURRENT_SOURCE_DIR}/versions.target
  OUTPUT_VARIABLE FDB_PACKAGE_NAME_WNL)
string(STRIP "${FDB_VERSION_WNL}" FDB_VERSION)
string(STRIP "${FDB_PACKAGE_NAME_WNL}" FDB_PACKAGE_NAME)
set(FDB_VERSION_PLAIN ${FDB_VERSION})
if(NOT FDB_RELEASE)
  set(FDB_VERSION "${FDB_VERSION}-PRERELEASE")
endif()
message(STATUS "FDB version is ${FDB_VERSION}")
message(STATUS "FDB package name is ${FDB_PACKAGE_NAME}")
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/versions.h.cmake ${CMAKE_CURRENT_BINARY_DIR}/fdbclient/versions.h)


################################################################################
# ACTOR COMPILER
################################################################################

if (NOT OPEN_FOR_IDE)
  # First thing we need is the actor compiler - and to compile and run the
  # actor compiler, we need mono
  find_program(MONO_EXECUTABLE mono)
  find_program(MCS_EXECUTABLE dmcs)

  if (NOT MCS_EXECUTABLE)
    find_program(MCS_EXECUTABLE mcs)
  endif()

  set(MONO_FOUND FALSE CACHE INTERNAL "")

  if (NOT MCS_EXECUTABLE)
    find_program(MCS_EXECUTABLE mcs)
  endif()

  if (MONO_EXECUTABLE AND MCS_EXECUTABLE)
    set(MONO_FOUND True CACHE INTERNAL "")
  endif()

  if (NOT MONO_FOUND)
    message(FATAL_ERROR "Could not find mono")
  endif()

  set(ACTORCOMPILER_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/ActorCompiler.cs
    ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/ActorParser.cs
    ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/ParseTree.cs
    ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/Program.cs
    ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/Properties/AssemblyInfo.cs)
  set(ACTOR_COMPILER_REFERENCES
    "-r:System,System.Core,System.Xml.Linq,System.Data.DataSetExtensions,Microsoft.CSharp,System.Data,System.Xml")

  add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/actorcompiler.exe
    COMMAND ${MCS_EXECUTABLE} ARGS ${ACTOR_COMPILER_REFERENCES} ${ACTORCOMPILER_SRCS} "-target:exe" "-out:actorcompiler.exe"
    DEPENDS ${ACTORCOMPILER_SRCS}
    COMMENT "Compile actor compiler" VERBATIM)
  add_custom_target(actorcompiler DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/actorcompiler.exe)
  set(actor_exe "${CMAKE_CURRENT_BINARY_DIR}/actorcompiler.exe")
endif()

################################################################################
# Vexilographer
################################################################################

set(VEXILLOGRAPHER_SRCS
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/c.cs
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/cpp.cs
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/java.cs
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/python.cs
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/ruby.cs
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/vexillographer.cs)

set(VEXILLOGRAPHER_REFERENCES "-r:System,System.Core,System.Data,System.Xml,System.Xml.Linq")
set(VEXILLOGRAPHER_EXE "${CMAKE_CURRENT_BINARY_DIR}/vexillographer.exe")
add_custom_command(OUTPUT ${VEXILLOGRAPHER_EXE}
  COMMAND ${MCS_EXECUTABLE} ARGS ${VEXILLOGRAPHER_REFERENCES} ${VEXILLOGRAPHER_SRCS} -target:exe -out:${VEXILLOGRAPHER_EXE}
  DEPENDS ${VEXILLOGRAPHER_SRCS}
  COMMENT "Compile Vexillographer")
add_custom_target(vexillographer DEPENDS ${VEXILLOGRAPHER_EXE})

set(ERROR_GEN_SRCS
  ${CMAKE_CURRENT_SOURCE_DIR}/flow/error_gen.cs)
set(ERROR_GEN_REFERENCES "-r:System,System.Core,System.Data,System.Xml,System.Xml.Linq")
set(ERROR_GEN_EXE "${CMAKE_CURRENT_BINARY_DIR}/error_gen.exe")
add_custom_command (OUTPUT ${ERROR_GEN_EXE}
  COMMAND ${MCS_EXECUTABLE} ARGS ${ERROR_GEN_REFERENCES} ${ERROR_GEN_SRCS} -target:exe -out:${ERROR_GEN_EXE}
  DEPENDS ${ERROR_GEN_SRCS}
  COMMENT "Compile error_gen")
add_custom_target(error_gen DEPENDS ${ERROR_GEN_EXE})

################################################################################
# Helper functions
################################################################################

if (OPEN_FOR_IDE)
  macro(actor_set varname srcs)
    set(${varname})
    foreach(src ${srcs})
	    set(tmp "${src}")
	    set(${varname} "${${varname}};${tmp}")
    endforeach()
  endmacro()

  macro(actor_compile target srcs)
  endmacro()
else()
  macro(actor_set varname srcs)
    set(${varname})
    foreach(src ${srcs})
	    set(tmp "${src}")
	    if(${src} MATCHES ".*\\.h")
	      continue()
	    elseif(${src} MATCHES ".*\\.actor\\.cpp")
	      string(REPLACE ".actor.cpp" ".actor.g.cpp" tmp ${src})
	      set(tmp "${CMAKE_CURRENT_BINARY_DIR}/${tmp}")
	    endif()
	    set(${varname} "${${varname}};${tmp}")
    endforeach()
  endmacro()

  set(ACTOR_TARGET_COUNTER "0")
  macro(actor_compile target srcs)
    set(options DISABLE_ACTOR_WITHOUT_WAIT)
    set(oneValueArg)
    set(multiValueArgs)
    cmake_parse_arguments(ACTOR_COMPILE "${options}" "${oneValueArgs}" "${multiValueArgs}" "${ARGN}")
    set(_tmp_out "")
    foreach(src ${srcs})
	    set(tmp "")
	    if(${src} MATCHES ".*\\.actor\\.h")
	      string(REPLACE ".actor.h" ".actor.g.h" tmp ${src})
	    elseif(${src} MATCHES ".*\\.actor\\.cpp")
	      string(REPLACE ".actor.cpp" ".actor.g.cpp" tmp ${src})
	    endif()
	    set(actor_compiler_flags "")
      if(ACTOR_COMPILE_DISABLE_ACTOR_WITHOUT_WAIT)
        set(actor_compiler_flags "--disable-actor-without-wait-error")
      endif()
	    if(tmp)
	      add_custom_command(OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/${tmp}"
		      COMMAND ${MONO_EXECUTABLE} ${actor_exe} "${CMAKE_CURRENT_SOURCE_DIR}/${src}" "${CMAKE_CURRENT_BINARY_DIR}/${tmp}" ${actor_compiler_flags} > /dev/null
		      DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/${src}" actorcompiler ${actor_exe}
		      COMMENT "Compile actor: ${src}")
	      set(_tmp_out "${_tmp_out};${CMAKE_CURRENT_BINARY_DIR}/${tmp}")
	    endif()
    endforeach()
    MATH(EXPR ACTOR_TARGET_COUNTER "${ACTOR_TARGET_COUNTER}+1")
    add_custom_target(${target}_actors_${ACTOR_TARGET_COUNTER} DEPENDS ${_tmp_out})
    add_dependencies(${target} ${target}_actors_${ACTOR_TARGET_COUNTER})
    target_include_directories(${target} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
    target_include_directories(${target} PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
  endmacro()
endif()

# This macro can be used to install symlinks, which turns out to be
# non-trivial due to CMake version differences and limitations on how
# files can be installed when building binary packages.
#
# The rule for binary packaging is that files (including symlinks) must
# be installed with the standard CMake install() macro.
#
# The rule for non-binary packaging is that CMake 2.6 cannot install()
# symlinks, but can create the symlink at install-time via scripting.
# Though, we assume that CMake 2.6 isn't going to be used to generate
# packages because versions later than 2.8.3 are superior for that purpose.
#
#   _filepath: the absolute path to the file to symlink
#   _sympath: absolute path of the installed symlink

macro(InstallSymlink _filepath _sympath)
    get_filename_component(_symname ${_sympath} NAME)
    get_filename_component(_installdir ${_sympath} PATH)

    if (BINARY_PACKAGING_MODE)
        execute_process(COMMAND "${CMAKE_COMMAND}" -E create_symlink
                        ${_filepath}
                        ${CMAKE_CURRENT_BINARY_DIR}/${_symname})
        install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${_symname}
          DESTINATION ${_installdir}
		  COMPONENT clients)
    else ()
        # scripting the symlink installation at install time should work
        # for CMake 2.6.x and 2.8.x
        install(CODE "
            if (\"\$ENV{DESTDIR}\" STREQUAL \"\")
                execute_process(COMMAND \"${CMAKE_COMMAND}\" -E create_symlink
                                ${_filepath}
                                ${_installdir}/${_symname})
            else ()
                execute_process(COMMAND \"${CMAKE_COMMAND}\" -E create_symlink
                                ${_filepath}
                                \$ENV{DESTDIR}/${_installdir}/${_symname})
            endif ()
        "
		  COMPONENT clients)
    endif ()
endmacro(InstallSymlink)

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
add_subdirectory(fdbmonitor)
add_subdirectory(bindings)
add_subdirectory(fdbbackup)

include(CPack)

################################################################################
# process compile commands for IDE
################################################################################

if (CMAKE_EXPORT_COMPILE_COMMANDS)
  add_custom_command(
	OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/compile_commands.json
	COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/build/gen_compile_db.py
	ARGS -b ${CMAKE_CURRENT_BINARY_DIR} -s ${CMAKE_CURRENT_SOURCE_DIR} -o ${CMAKE_CURRENT_SOURCE_DIR}/compile_commands.json ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json
	DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/build/gen_compile_db.py ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json
	COMMENT "Build compile commands for IDE"
	)
  add_custom_target(procossed_compile_commands ALL DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/compile_commands.json ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json)
endif()
