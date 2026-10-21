# Builds the pinned gRPC, with the abseil, protobuf, re2 and c-ares it bundles, as part of FDB's own build, after
# applying FDB's patches from cmake/patches/grpc. Deterministic simulation depends on those patches, so FDB never uses
# a system gRPC.
#
# Configure fetches and patches the source (once per version and patch set); the libraries and protoc are ordinary
# targets in the build graph, so only what FDB links is compiled, in parallel with FDB, through any compiler launcher.

set(GRPC_VERSION 1.83.0)
set(GRPC_SOURCE_DIR "" CACHE PATH
  "Unpatched gRPC v${GRPC_VERSION} source tree including its third_party dependencies; cloned from GitHub when empty")

set(GRPC_ROOT "${CMAKE_BINARY_DIR}/grpc")
set(GRPC_SRC "${GRPC_ROOT}/src")

file(GLOB GRPC_PATCHES "${CMAKE_SOURCE_DIR}/cmake/patches/grpc/*.patch")
list(SORT GRPC_PATCHES)

set(GRPC_STAMP "${GRPC_VERSION};${GRPC_SOURCE_DIR}")
foreach(patch IN LISTS GRPC_PATCHES)
  file(SHA256 "${patch}" patch_hash)
  string(APPEND GRPC_STAMP ";${patch_hash}")
endforeach()
string(SHA256 GRPC_STAMP "${GRPC_STAMP}")

set(GRPC_STAMP_FILE "${GRPC_ROOT}/fdb-source-stamp")
set(GRPC_PREPARED_STAMP "")
if(EXISTS "${GRPC_STAMP_FILE}")
  file(READ "${GRPC_STAMP_FILE}" GRPC_PREPARED_STAMP)
endif()

function(grpc_run step)
  execute_process(COMMAND ${ARGN} RESULT_VARIABLE result OUTPUT_FILE "${GRPC_ROOT}/${step}.log"
                  ERROR_FILE "${GRPC_ROOT}/${step}.log")
  if(NOT result EQUAL 0)
    message(FATAL_ERROR "gRPC ${step} failed (${result}); see ${GRPC_ROOT}/${step}.log")
  endif()
endfunction()

if(NOT GRPC_PREPARED_STAMP STREQUAL GRPC_STAMP)
  message(STATUS "Preparing gRPC ${GRPC_VERSION} source in ${GRPC_SRC}")
  file(REMOVE_RECURSE "${GRPC_ROOT}")
  file(MAKE_DIRECTORY "${GRPC_ROOT}")
  find_package(Git REQUIRED)
  if(GRPC_SOURCE_DIR)
    grpc_run(copy ${CMAKE_COMMAND} -E copy_directory "${GRPC_SOURCE_DIR}" "${GRPC_SRC}")
  else()
    grpc_run(clone ${GIT_EXECUTABLE} clone --quiet --depth 1 --branch v${GRPC_VERSION}
             https://github.com/grpc/grpc.git "${GRPC_SRC}")
    grpc_run(submodules ${GIT_EXECUTABLE} -C "${GRPC_SRC}" submodule update --init --depth 1
             third_party/abseil-cpp third_party/protobuf third_party/re2 third_party/cares/cares)
  endif()
  foreach(patch IN LISTS GRPC_PATCHES)
    get_filename_component(patch_name "${patch}" NAME)
    # The ceiling keeps git from treating a copied, repository-less tree as part of an enclosing repository.
    grpc_run("patch-${patch_name}" ${CMAKE_COMMAND} -E env "GIT_CEILING_DIRECTORIES=${GRPC_ROOT}"
             ${GIT_EXECUTABLE} -C "${GRPC_SRC}" apply --whitespace=nowarn "${patch}")
  endforeach()
  file(WRITE "${GRPC_STAMP_FILE}" "${GRPC_STAMP}")
endif()

# A function, so the settings below stay out of FDB's scope.
function(add_grpc)
  # The bundled projects predate option() and set(CACHE) leaving the variables below in force.
  set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)
  set(CMAKE_POLICY_DEFAULT_CMP0126 NEW)
  set(BUILD_SHARED_LIBS OFF)
  set(CMAKE_POSITION_INDEPENDENT_CODE ON)
  unset(CMAKE_CXX_CLANG_TIDY)
  set(gRPC_INSTALL OFF)
  set(gRPC_BUILD_TESTS OFF)
  set(gRPC_ABSL_PROVIDER module)
  set(gRPC_PROTOBUF_PROVIDER module)
  set(gRPC_RE2_PROVIDER module)
  set(gRPC_CARES_PROVIDER module)
  set(gRPC_ZLIB_PROVIDER package)
  set(gRPC_SSL_PROVIDER package)
  # gRPC puts OpenSSL's include directory ahead of the bundled abseil and protobuf; when that is a shared prefix such as
  # Homebrew's /opt/homebrew/include, another installed abseil or protobuf would shadow them. Use the directory that
  # really holds OpenSSL's headers.
  file(REAL_PATH "${OPENSSL_INCLUDE_DIR}/openssl/ssl.h" ssl_h)
  get_filename_component(OPENSSL_INCLUDE_DIR "${ssl_h}" DIRECTORY)
  get_filename_component(OPENSSL_INCLUDE_DIR "${OPENSSL_INCLUDE_DIR}" DIRECTORY)
  foreach(plugin CSHARP NODE OBJECTIVE_C PHP PYTHON RUBY)
    set(gRPC_BUILD_GRPC_${plugin}_PLUGIN OFF)
  endforeach()
  set(gRPC_BUILD_CSHARP_EXT OFF)
  set(ABSL_PROPAGATE_CXX_STD ON)
  set(ABSL_ENABLE_INSTALL OFF)
  set(protobuf_INSTALL OFF)
  set(protobuf_BUILD_TESTS OFF)
  set(utf8_range_ENABLE_INSTALL OFF)
  set(RE2_BUILD_TESTING OFF)
  set(CARES_INSTALL OFF)

  # FDB's warning flags, and -Werror in particular, are for FDB's code.
  get_directory_property(fdb_options COMPILE_OPTIONS)
  set(grpc_options ${fdb_options})
  list(FILTER grpc_options EXCLUDE REGEX "-W")
  list(APPEND grpc_options -w)
  set_directory_properties(PROPERTIES COMPILE_OPTIONS "${grpc_options}")
  add_subdirectory("${GRPC_SRC}" "${GRPC_ROOT}/build" EXCLUDE_FROM_ALL)
  set_directory_properties(PROPERTIES COMPILE_OPTIONS "${fdb_options}")
endfunction()

add_grpc()

# Warnings in gRPC's, abseil's and protobuf's headers are not FDB's to fix, so FDB code includes them as system headers,
# like a prebuilt package's. (add_subdirectory(SYSTEM) does this from CMake 3.25.)
function(mark_includes_system dir)
  get_property(targets DIRECTORY "${dir}" PROPERTY BUILDSYSTEM_TARGETS)
  foreach(target IN LISTS targets)
    get_target_property(includes ${target} INTERFACE_INCLUDE_DIRECTORIES)
    if(includes)
      set_target_properties(${target} PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${includes}")
    endif()
  endforeach()
  get_property(subdirs DIRECTORY "${dir}" PROPERTY SUBDIRECTORIES)
  foreach(subdir IN LISTS subdirs)
    mark_includes_system("${subdir}")
  endforeach()
endfunction()
mark_includes_system("${GRPC_SRC}")

# Ahead of every other system include directory, which may be a shared prefix (such as Homebrew's) holding another gRPC,
# abseil or protobuf.
include_directories(BEFORE SYSTEM
  "${GRPC_SRC}/include"
  "${GRPC_SRC}/third_party/abseil-cpp"
  "${GRPC_SRC}/third_party/protobuf/src"
  "${GRPC_SRC}/third_party/protobuf/third_party/utf8_range")

# The names FDB uses are those of an installed gRPC package.
add_library(gRPC::grpc++ ALIAS grpc++)
add_executable(gRPC::grpc_cpp_plugin ALIAS grpc_cpp_plugin)
include("${GRPC_SRC}/third_party/protobuf/cmake/protobuf-generate.cmake")
