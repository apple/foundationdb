add_executable(swifttestapp
        main.swift
        ../flow/future_support.swift
        ../flow/reply_support.swift
        collections_support.swift
        task_support.swift
        )

target_link_libraries(swifttestapp flow)
# target_link_libraries(swifttestapp fdbserver)

target_include_directories(swifttestapp PUBLIC
    "${CMAKE_SOURCE_DIR}/flow/include"
    "${CMAKE_BINARY_DIR}/flow/include"
    "${CMAKE_SOURCE_DIR}/fdbclient/include"
    "${CMAKE_BINARY_DIR}/fdbclient/include"
    "${CMAKE_SOURCE_DIR}/fdbserver/include"
    "${CMAKE_BINARY_DIR}/fdbserver/include"
    "${CMAKE_SOURCE_DIR}/fdbrpc/include"
    "${CMAKE_BINARY_DIR}/fdbrpc/include"
    "${CMAKE_SOURCE_DIR}/contrib/fmt-8.1.1/include"
    "${CMAKE_SOURCE_DIR}/contrib/sqlite"
    "${CMAKE_BINARY_DIR}/boost_install/include"
    "${CMAKE_BINARY_DIR}/msgpackProject-prefix/src/msgpackProject/include"
)

# TODO: We need a platform-agnostic way to find the C++ stdlib searchpaths
target_include_directories(swifttestapp PRIVATE
    "/opt/boost_1_78_0_clang"
)

target_link_directories(swifttestapp PRIVATE
  "/opt/rh/devtoolset-8/root/usr/lib/gcc/x86_64-redhat-linux/8"
)

# TODO: the TBD validation skip is because of swift_job_run_generic, though it seems weird why we need to do that?
target_compile_options(swifttestapp PRIVATE "$<$<COMPILE_LANGUAGE:Swift>:SHELL:-Xcc -std=c++17 -Xfrontend -validate-tbd-against-ir=none> -Xcc -DNO_INTELLISENSE -Xcc -DSWIFT_HACK")
