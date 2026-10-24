# Builds the pinned gRPC, with the abseil, protobuf, re2 and c-ares it bundles, as part of FDB's own build, after
# applying FDB's patches from cmake/patches/grpc. Deterministic simulation depends on those patches, so FDB never uses
# a system gRPC.
#
# Configure fetches and patches the source; the libraries and protoc are ordinary targets in the build graph, so only
# what FDB links is compiled, in parallel with FDB, through any compiler launcher.

include(FetchContent)
find_package(Git REQUIRED)

set(GRPC_VERSION 1.83.0)
file(GLOB GRPC_PATCHES "${CMAKE_SOURCE_DIR}/cmake/patches/grpc/*.patch")
list(SORT GRPC_PATCHES)

# FetchContent re-runs the patch step in the existing tree when the patch list changes, so the step first restores
# the checkout. Hosts without GitHub access set FETCHCONTENT_SOURCE_DIR_GRPC to an already patched tree instead.
FetchContent_Declare(grpc
  GIT_REPOSITORY https://github.com/grpc/grpc.git
  GIT_TAG v${GRPC_VERSION}
  GIT_SHALLOW ON
  GIT_SUBMODULES third_party/abseil-cpp third_party/protobuf third_party/re2 third_party/cares/cares
  GIT_SUBMODULES_RECURSE OFF
  PATCH_COMMAND ${GIT_EXECUTABLE} reset --quiet --hard --recurse-submodules
  COMMAND ${GIT_EXECUTABLE} clean --quiet -fd
  COMMAND ${GIT_EXECUTABLE} submodule --quiet foreach ${GIT_EXECUTABLE} clean --quiet -fd
  COMMAND ${GIT_EXECUTABLE} apply --whitespace=nowarn ${GRPC_PATCHES}
  # A directory without a CMakeLists.txt, so FetchContent_MakeAvailable only populates; add_grpc() adds the project
  # with EXCLUDE_FROM_ALL, which FetchContent supports only from CMake 3.28.
  SOURCE_SUBDIR fdb-populate-only)
FetchContent_MakeAvailable(grpc)
set(GRPC_SRC "${grpc_SOURCE_DIR}")

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
  add_subdirectory("${GRPC_SRC}" "${grpc_BINARY_DIR}" EXCLUDE_FROM_ALL)
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
