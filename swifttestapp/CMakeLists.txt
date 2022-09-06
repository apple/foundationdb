add_executable(swifttestapp
        main.swift
        future_compat.swift
        )

target_link_libraries(swifttestapp flow)

target_include_directories(swifttestapp PUBLIC
    "${CMAKE_SOURCE_DIR}/flow/include"
    "${CMAKE_BINARY_DIR}/flow/include"
    "${CMAKE_SOURCE_DIR}/contrib/fmt-8.1.1/include"
)

# TODO: We need a platform-agnostic way to find the C++ stdlib searchpaths
target_include_directories(swifttestapp PRIVATE
    "/opt/boost_1_78_0_clang"
    "/opt/rh/devtoolset-8/root/usr/include/c++/8/x86_64-redhat-linux/"
    "/opt/rh/devtoolset-8/root/usr/lib/gcc/x86_64-redhat-linux/8/../../../../include/c++/8"
)

target_link_directories(swifttestapp PRIVATE
  "/opt/rh/devtoolset-8/root/usr/lib/gcc/x86_64-redhat-linux/8"
)

# TODO: the TBD validation skip is because of swift_job_run_generic, though it seems weird why we need to do that?
target_compile_options(swifttestapp PRIVATE "$<$<COMPILE_LANGUAGE:Swift>:SHELL:-Xcc -std=c++17 -enable-experimental-cxx-interop -Xfrontend -validate-tbd-against-ir=none>")
