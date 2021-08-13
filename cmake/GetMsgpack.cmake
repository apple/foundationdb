find_package(msgpack 3.3.0 EXACT QUIET CONFIG)

add_library(msgpack INTERFACE)

if(msgpack_FOUND)
  target_link_libraries(msgpack INTERFACE msgpackc-cxx)
else()
  include(ExternalProject)
  ExternalProject_add(msgpackProject
    URL "https://github.com/msgpack/msgpack-c/releases/download/cpp-3.3.0/msgpack-3.3.0.tar.gz"
    URL_HASH SHA256=6e114d12a5ddb8cb11f669f83f32246e484a8addd0ce93f274996f1941c1f07b
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
  )

  ExternalProject_Get_property(msgpackProject SOURCE_DIR)
  target_include_directories(msgpack SYSTEM INTERFACE "${SOURCE_DIR}/include")
  add_dependencies(msgpack msgpackProject)
endif()
