# See: https://github.com/XiangpengHao/cxx-cmake-example

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CARGO_CMD cargo build)
    set(RUST_TARGET_DIR "debug")
else ()
    set(CARGO_CMD cargo build --release)
    set(RUST_TARGET_DIR "release")
endif()

if(ENABLE_LTO)
#    set(RUST_FLAGS "-Clinker-plugin-lto" "-Clinker=clang-13" "-Clink-arg=-fuse-ld=lld-13")
endif()

set(CXX_DIR ${CMAKE_CURRENT_BINARY_DIR}/cxxbridge/fdb_rust/src)
set(FDB_RUST_LIB "${CMAKE_CURRENT_BINARY_DIR}/${RUST_TARGET_DIR}/libfdb_rust.a")

add_library(fdb_rust2 STATIC ${CXX_DIR}/flow.rs.cc ${CXX_DIR}/fdbclient.rs.cc ${CXX_DIR}/fdbserver.rs.cc)
add_custom_target(
    run_cargo
    COMMAND CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR} RUSTFLAGS="${RUST_FLAGS}" ${CARGO_CMD}
    COMMAND cp ${CXX_DIR}/flow.rs.h ${CMAKE_CURRENT_BINARY_DIR}/flow.rs.h
    COMMAND cp ${CXX_DIR}/fdbclient.rs.h ${CMAKE_CURRENT_BINARY_DIR}/fdbclient.rs.h
    COMMAND cp ${CXX_DIR}/fdbserver.rs.h ${CMAKE_CURRENT_BINARY_DIR}/fdbserver.rs.h
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    BYPRODUCTS ${FDB_RUST_LIB} ${CXX_DIR}/flow.rs.cc ${CMAKE_CURRENT_BINARY_DIR}/flow.rs.h ${CXX_DIR}/fdbclient.rs.cc ${CMAKE_CURRENT_BINARY_DIR}/fdbclient.rs.h ${CXX_DIR}/fdbserver.rs.cc ${CMAKE_CURRENT_BINARY_DIR}/fdbserver.rs.h
)

target_include_directories(fdb_rust2 PRIVATE ${CMAKE_SOURCE_DIR}/contrib/fmt-8.1.1/include)
target_include_directories(fdb_rust2 PRIVATE ${CMAKE_BINARY_DIR}/flow/include)
target_include_directories(fdb_rust2 PRIVATE ${CMAKE_SOURCE_DIR}/flow/include)
target_include_directories(fdb_rust2 PRIVATE ${CMAKE_BINARY_DIR}/fdbclient/include)
target_include_directories(fdb_rust2 PRIVATE ${CMAKE_SOURCE_DIR}/fdbclient/include)
target_include_directories(fdb_rust2 PRIVATE ${CMAKE_BINARY_DIR}/fdbserver/include)
target_include_directories(fdb_rust2 PRIVATE ${CMAKE_SOURCE_DIR}/fdbserver/include)
target_include_directories(fdb_rust2 PRIVATE ${CMAKE_BINARY_DIR}/boost_install/include)
target_link_libraries(fdb_rust2 PUBLIC pthread dl ${FDB_RUST_LIB})

add_test(NAME fdb_rust_test 
    COMMAND cargo test
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})