cmake_minimum_required(VERSION 3.24)

project(fdb-serialize-check C CXX)

find_package(Boost REQUIRED COMPONENTS json)

set(CMAKE_CXX_STANDARD 20)

# Configure clang CMake variables/modules
find_package(Clang REQUIRED CONFIG)
message(STATUS "Using clang: ${LLVM_VERSION_MAJOR}")
set(LLVM_CMAKE_MODULE_PATH "${CLANG_INCLUDE_DIRS}/../lib/cmake")
list(APPEND CMAKE_MODULE_PATH "${LLVM_CMAKE_MODULE_PATH}/clang"
     "${LLVM_CMAKE_MODULE_PATH}/llvm")
include(AddLLVM)
include(AddClang)

set(SOURCE_SCANNER_BINARY src/SourceScanner.cpp)
add_clang_executable(source_scanner ${SOURCE_SCANNER_BINARY})
target_include_directories(
  source_scanner PRIVATE ${CLANG_INCLUDE_DIRS} ${LLVM_INCLUDE_DIRS}
                         ${Boost_INCLUDE_DIRS})
target_link_libraries(
  source_scanner PRIVATE clangAST clangASTMatchers clangBasic clangFrontend
                         clangSerialization clangTooling Boost::json)
