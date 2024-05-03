set(FOUNDATIONDB_CROSS_COMPILING ON CACHE STRING "" FORCE)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON CACHE STRING "" FORCE)

# The target is x86_64 Linux.
set(CMAKE_SYSTEM_NAME               Linux CACHE STRING "" FORCE)
set(CMAKE_SYSTEM_PROCESSOR          X86_64 CACHE STRING "" FORCE)

# Setup the host tools.
if (NOT FOUNDATIONDB_SWIFT_TOOLCHAIN_ROOT)
  message(FATAL_ERROR "Cross compilation: Missing FoundationDB swift toolchain root (FOUNDATIONDB_SWIFT_TOOLCHAIN_ROOT=<toolchain>/usr).")
endif()
set(CMAKE_Swift_COMPILER ${FOUNDATIONDB_SWIFT_TOOLCHAIN_ROOT}/bin/swiftc CACHE STRING "" FORCE)
set(CMAKE_C_COMPILER ${FOUNDATIONDB_SWIFT_TOOLCHAIN_ROOT}/bin/clang CACHE STRING "" FORCE)
set(CMAKE_CXX_COMPILER ${FOUNDATIONDB_SWIFT_TOOLCHAIN_ROOT}/bin/clang++ CACHE STRING "" FORCE)

if (NOT FOUNDATIONDB_LLVM_TOOLCHAIN_ROOT)
  message(FATAL_ERROR "Cross compilation: Missing FoundationDB LLVM toolchain root (FOUNDATIONDB_LLVM_TOOLCHAIN_ROOT=<toolchain>/usr).")
endif()
set(CMAKE_LINKER ${FOUNDATIONDB_LLVM_TOOLCHAIN_ROOT}/bin/ld.lld CACHE STRING "" FORCE)
set(CMAKE_AR ${FOUNDATIONDB_LLVM_TOOLCHAIN_ROOT}/bin/llvm-ar CACHE STRING "" FORCE)
set(CMAKE_RANLIB ${FOUNDATIONDB_LLVM_TOOLCHAIN_ROOT}/bin/llvm-ranlib CACHE STRING "" FORCE)

set(MONO_EXECUTABLE /Library/Frameworks/Mono.framework/Versions/Current/bin/mono CACHE STRING "" FORCE)
if(NOT EXISTS ${MONO_EXECUTABLE})
  message(FATAL_ERROR "Cross compilation: Mono is not installed.")
endif()
set(MCS_EXECUTABLE /Library/Frameworks/Mono.framework/Versions/Current/bin/mcs CACHE STRING "" FORCE)

execute_process(COMMAND which python3
WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
OUTPUT_VARIABLE
  WhichPython3
)
string(STRIP ${WhichPython3} WhichPython3Trimmed)
set(Python3_EXECUTABLE ${WhichPython3Trimmed} CACHE STRING "" FORCE)

# Set up the SDK root and compiler flags.
if (NOT FOUNDATIONDB_LINUX_CONTAINER_ROOT)
  message(FATAL_ERROR "Cross compilation: Missing FoundationDB linux container path (FOUNDATIONDB_LINUX_CONTAINER_ROOT).")
endif()
set(CMAKE_SYSROOT "${FOUNDATIONDB_LINUX_CONTAINER_ROOT}" CACHE STRING "" FORCE)
# FIXME: Do not hardcode 11.
set(CMAKE_Swift_COMPILER_EXTERNAL_TOOLCHAIN "${CMAKE_SYSROOT}/opt/rh/devtoolset-11/root/usr" CACHE STRING "" FORCE)

string(TOLOWER ${CMAKE_SYSTEM_PROCESSOR} TripleArch)
set(CMAKE_C_FLAGS "-target ${TripleArch}-unknown-linux-gnu -fuse-ld=${CMAKE_LINKER} --gcc-toolchain=${CMAKE_Swift_COMPILER_EXTERNAL_TOOLCHAIN}" CACHE STRING "" FORCE)
set(CMAKE_CXX_FLAGS "-target ${TripleArch}-unknown-linux-gnu -fuse-ld=${CMAKE_LINKER} --gcc-toolchain=${CMAKE_Swift_COMPILER_EXTERNAL_TOOLCHAIN}" CACHE STRING "" FORCE)

set(COMPILE_BOOST_CXXFLAGS "-target;${TripleArch}-linux-gnu;-fuse-ld=${CMAKE_LINKER};--gcc-toolchain=${CMAKE_Swift_COMPILER_EXTERNAL_TOOLCHAIN};--sysroot;${CMAKE_SYSROOT}" CACHE STRING "" FORCE)

# CMake might think it's linking MachO files and pass this flag, so avoid it.
set(HAVE_FLAG_SEARCH_PATHS_FIRST OFF CACHE BOOL "" FORCE)
