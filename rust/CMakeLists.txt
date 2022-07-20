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

set(FDB_RUST_LIB "${CMAKE_CURRENT_BINARY_DIR}/${RUST_TARGET_DIR}/libfdb_rust.a")
set(FDB_RUST_CXX "${CMAKE_CURRENT_BINARY_DIR}/fdb_rust.cpp")

add_library(fdb_rust2 STATIC ${FDB_RUST_CXX})
add_custom_command(
    OUTPUT ${FDB_RUST_CXX}
    COMMAND CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR} RUSTFLAGS="${RUST_FLAGS}" ${CARGO_CMD}
    COMMAND cp ${CMAKE_CURRENT_BINARY_DIR}/cxxbridge/fdb_rust/src/lib.rs.cc ${FDB_RUST_CXX}
    COMMAND cp ${CMAKE_CURRENT_BINARY_DIR}/cxxbridge/fdb_rust/src/lib.rs.h ${CMAKE_CURRENT_BINARY_DIR}/fdb_rust.h
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    BYPRODUCTS ${FDB_RUST_LIB}
)

target_include_directories(fdb_rust2 PRIVATE ${CMAKE_BINARY_DIR}/flow/include)
target_include_directories(fdb_rust2 PRIVATE ${CMAKE_SOURCE_DIR}/flow/include)
target_link_libraries(fdb_rust2 PUBLIC pthread dl ${FDB_RUST_LIB})

add_test(NAME fdb_rust_test 
    COMMAND cargo test
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})