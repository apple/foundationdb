include(CompilerChecks)

env_set(USE_GPERFTOOLS OFF BOOL "Use gperfools for profiling")
env_set(USE_DTRACE ON BOOL "Enable dtrace probes on supported platforms")
env_set(USE_VALGRIND OFF BOOL "Compile for valgrind usage")
env_set(USE_VALGRIND_FOR_CTEST ${USE_VALGRIND} BOOL "Use valgrind for ctest")
env_set(VALGRIND_ARENA OFF BOOL "Inform valgrind about arena-allocated memory. Makes valgrind slower but more precise.")
env_set(ALLOC_INSTRUMENTATION OFF BOOL "Instrument alloc")
env_set(WITH_UNDODB OFF BOOL "Use rr or undodb")
env_set(USE_ASAN OFF BOOL "Compile with address sanitizer")
env_set(USE_UBSAN OFF BOOL "Compile with undefined behavior sanitizer")
env_set(USE_TSAN OFF BOOL "Compile with thread sanitizer")
env_set(FDB_RELEASE OFF BOOL "This is a building of a final release")
env_set(USE_CCACHE OFF BOOL "Use ccache for compilation if available")
env_set(RELATIVE_DEBUG_PATHS OFF BOOL "Use relative file paths in debug info")
env_set(USE_WERROR OFF BOOL "Compile with -Werror. Recommended for local development and CI.")
default_linker(_use_ld)
env_set(USE_LD "${_use_ld}" STRING
  "The linker to use for building: can be LD (system default and same as DEFAULT), BFD, GOLD, or LLD - will be LLD for Clang if available, DEFAULT otherwise")
use_libcxx(_use_libcxx)
env_set(USE_LIBCXX "${_use_libcxx}" BOOL "Use libc++")
static_link_libcxx(_static_link_libcxx)
env_set(STATIC_LINK_LIBCXX "${_static_link_libcxx}" BOOL "Statically link libstdcpp/libc++")

if(USE_LIBCXX AND STATIC_LINK_LIBCXX AND NOT USE_LD STREQUAL "LLD")
  message(FATAL_ERROR "Unsupported configuration: STATIC_LINK_LIBCXX with libc++ only works if USE_LD=LLD")
endif()
if(STATIC_LINK_LIBCXX AND USE_TSAN)
  message(FATAL_ERROR "Unsupported configuration: STATIC_LINK_LIBCXX doesn't work with tsan")
endif()

set(rel_debug_paths OFF)
if(RELATIVE_DEBUG_PATHS)
  set(rel_debug_paths ON)
endif()

if(USE_GPERFTOOLS)
  find_package(Gperftools REQUIRED)
endif()

add_compile_options(-DCMAKE_BUILD)
add_compile_definitions(BOOST_ERROR_CODE_HEADER_ONLY BOOST_SYSTEM_NO_DEPRECATED)

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
  add_compile_options(-DFDB_CLEAN_BUILD)
endif()

include_directories(${CMAKE_SOURCE_DIR})
include_directories(${CMAKE_BINARY_DIR})
if (NOT OPEN_FOR_IDE)
  add_definitions(-DNO_INTELLISENSE)
endif()
if(WIN32)
  add_definitions(-DUSE_USEFIBERS)
else()
  add_definitions(-DUSE_UCONTEXT)
endif()

if (USE_CCACHE)
  FIND_PROGRAM(CCACHE_FOUND "ccache")
  if(CCACHE_FOUND)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
  else()
    message(SEND_ERROR "CCACHE is ON, but ccache was not found")
  endif()
endif()

include(CheckFunctionExists)
set(CMAKE_REQUIRED_INCLUDES stdlib.h malloc.h)
set(CMAKE_REQUIRED_LIBRARIES c)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

if(NOT WIN32)
  include(CheckIncludeFile)
  CHECK_INCLUDE_FILE("stdatomic.h" HAS_C11_ATOMICS)
  if (NOT HAS_C11_ATOMICS)
    message(FATAL_ERROR "C compiler does not support c11 atomics")
  endif()
endif()

if(WIN32)
  # see: https://docs.microsoft.com/en-us/windows/desktop/WinProg/using-the-windows-headers
  # this sets the windows target version to Windows Server 2003
  set(WINDOWS_TARGET 0x0502)
  if(CMAKE_CXX_FLAGS MATCHES "/W[0-4]")
    # TODO: This doesn't seem to be good style, but I couldn't find a better way so far
    string(REGEX REPLACE "/W[0-4]" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  endif()
  add_compile_options(/W0 /EHsc /bigobj $<$<CONFIG:Release>:/Zi> /MP /FC /Gm-)
  add_compile_definitions(_WIN32_WINNT=${WINDOWS_TARGET} WINVER=${WINDOWS_TARGET} NTDDI_VERSION=0x05020000 BOOST_ALL_NO_LIB)
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /MT")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd")
else()
  set(GCC NO)
  set(CLANG NO)
  set(ICC NO)
  if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
    set(CLANG YES)
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Intel")
    set(ICC YES)
  else()
    # This is not a very good test. However, as we do not really support many architectures
    # this is good enough for now
    set(GCC YES)
  endif()

  # check linker flags.
  if (USE_LD STREQUAL "DEFAULT")
    set(USE_LD "LD")
  else()
    if ((NOT (USE_LD STREQUAL "LD")) AND (NOT (USE_LD STREQUAL "GOLD")) AND (NOT (USE_LD STREQUAL "LLD")) AND (NOT (USE_LD STREQUAL "BFD")))
      message (FATAL_ERROR "USE_LD must be set to DEFAULT, LD, BFD, GOLD, or LLD!")
    endif()
  endif()

  # if USE_LD=LD, then we don't do anything, defaulting to whatever system
  # linker is available (e.g. binutils doesn't normally exist on macOS, so this
  # implies the default xcode linker, and other distros may choose others by
  # default).

  if(USE_LD STREQUAL "BFD")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=bfd -Wl,--disable-new-dtags")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=bfd -Wl,--disable-new-dtags")
  endif()

  if(USE_LD STREQUAL "GOLD")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=gold -Wl,--disable-new-dtags")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=gold -Wl,--disable-new-dtags")
  endif()

  if(USE_LD STREQUAL "LLD")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=lld -Wl,--disable-new-dtags")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=lld -Wl,--disable-new-dtags")
  endif()

  if(rel_debug_paths)
    add_compile_options("-fdebug-prefix-map=${CMAKE_SOURCE_DIR}=." "-fdebug-prefix-map=${CMAKE_BINARY_DIR}=.")
  endif()

  # we always compile with debug symbols. CPack will strip them out
  # and create a debuginfo rpm
  add_compile_options(-ggdb -fno-omit-frame-pointer)
  if(USE_ASAN)
    add_compile_options(
      -fsanitize=address
      -DUSE_SANITIZER)
    set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -fsanitize=address")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fsanitize=address")
    set(CMAKE_EXE_LINKER_FLAGS    "${CMAKE_EXE_LINKER_FLAGS}    -fsanitize=address ${CMAKE_THREAD_LIBS_INIT}")
  endif()

  if(USE_UBSAN)
    add_compile_options(
      -fsanitize=undefined
      # TODO(atn34) Re-enable -fsanitize=alignment once https://github.com/apple/foundationdb/issues/1434 is resolved
      -fno-sanitize=alignment
      -DUSE_SANITIZER)
    set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -fsanitize=undefined")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fsanitize=undefined")
    set(CMAKE_EXE_LINKER_FLAGS    "${CMAKE_EXE_LINKER_FLAGS}    -fsanitize=undefined ${CMAKE_THREAD_LIBS_INIT}")
  endif()

  if(USE_TSAN)
    add_compile_options(
      -fsanitize=thread
      -DUSE_SANITIZER)
    set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -fsanitize=thread")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fsanitize=thread")
    set(CMAKE_EXE_LINKER_FLAGS    "${CMAKE_EXE_LINKER_FLAGS}    -fsanitize=thread ${CMAKE_THREAD_LIBS_INIT}")
  endif()

  if(PORTABLE_BINARY)
    message(STATUS "Create a more portable binary")
    set(CMAKE_MODULE_LINKER_FLAGS "-static-libstdc++ -static-libgcc ${CMAKE_MODULE_LINKER_FLAGS}")
    set(CMAKE_SHARED_LINKER_FLAGS "-static-libstdc++ -static-libgcc ${CMAKE_SHARED_LINKER_FLAGS}")
    set(CMAKE_EXE_LINKER_FLAGS    "-static-libstdc++ -static-libgcc ${CMAKE_EXE_LINKER_FLAGS}")
  endif()
  if(STATIC_LINK_LIBCXX)
    if (NOT USE_LIBCXX AND NOT APPLE)
      add_link_options(-static-libstdc++ -static-libgcc)
    endif()
  endif()
  # # Instruction sets we require to be supported by the CPU
  # TODO(atn34) Re-enable once https://github.com/apple/foundationdb/issues/1434 is resolved
  # Some of the following instructions have alignment requirements, so it seems
  # prudent to disable them until we properly align memory.
  # add_compile_options(
  #   -maes
  #   -mmmx
  #   -mavx
  #   -msse4.2)

  # Tentatively re-enabling vector instructions
  set(USE_AVX512F OFF CACHE BOOL "Enable AVX 512F instructions")
  if (USE_AVX512F)
    if (CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "^x86")
      add_compile_options(-mavx512f)
    elseif(USE_VALGRIND)
      message(STATUS "USE_VALGRIND=ON make USE_AVX OFF to satisfy valgrind analysis requirement")
      set(USE_AVX512F OFF)
    else()
      message(STATUS "USE_AVX512F is supported on x86 or x86_64 only")
      set(USE_AVX512F OFF)
    endif()
  endif()
  set(USE_AVX ON CACHE BOOL "Enable AVX instructions")
  if (USE_AVX)
    if (CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "^x86")
      add_compile_options(-mavx)
    elseif(USE_VALGRIND)
      message(STATUS "USE_VALGRIND=ON make USE_AVX OFF to satisfy valgrind analysis requirement")
      set(USE_AVX OFF)
    else()
      message(STATUS "USE_AVX is supported on x86 or x86_64 only")
      set(USE_AVX OFF)
    endif()
  endif()

  # Intentionally using builtin memcpy.  G++ does a good job on small memcpy's when the size is known at runtime.
  # If the size is not known, then it falls back on the memcpy that's available at runtime (rte_memcpy, as of this
  # writing; see flow.cpp).
  #
  # The downside of the builtin memcpy is that it's slower at large copies, so if we spend a lot of time on large
  # copies of sizes that are known at compile time, this might not be a win.  See the output of performance/memcpy
  # for more information.
  #add_compile_options(-fno-builtin-memcpy)

  if (USE_VALGRIND)
    add_compile_options(-DVALGRIND=1 -DUSE_VALGRIND=1)
  endif()
  if (VALGRIND_ARENA)
    add_compile_options(-DVALGRIND_ARENA=1)
  endif()
  if (CLANG)
    add_compile_options()
    # Clang has link errors unless `atomic` is specifically requested.
    if(NOT APPLE)
      #add_link_options(-latomic)
    endif()
    if (APPLE OR USE_LIBCXX)
      add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-stdlib=libc++>)
      add_compile_definitions(WITH_LIBCXX)
      if (NOT APPLE)
        if (STATIC_LINK_LIBCXX)
          add_link_options(-static-libgcc -nostdlib++  -Wl,-Bstatic -lc++ -lc++abi -Wl,-Bdynamic)
        endif()
        add_link_options(-stdlib=libc++ -Wl,-build-id=sha1)
      endif()
    endif()
    if (OPEN_FOR_IDE)
      add_compile_options(
        -Wno-unknown-attributes)
    endif()
    add_compile_options(
      -Wall -Wextra
      # Here's the current set of warnings we need to explicitly disable to compile warning-free with clang 10
      -Wno-comment
      -Wno-dangling-else
      -Wno-delete-non-virtual-dtor
      -Wno-format
      -Wno-mismatched-tags
      -Wno-missing-field-initializers
      -Wno-overloaded-virtual
      -Wno-reorder
      -Wno-reorder-ctor
      -Wno-sign-compare
      -Wno-tautological-pointer-compare
      -Wno-undefined-var-template
      -Wno-tautological-pointer-compare
      -Wredundant-move
      -Wpessimizing-move
      -Woverloaded-virtual
      -Wno-unknown-pragmas
      -Wno-unknown-warning-option
      -Wno-unused-function
      -Wno-unused-local-typedef
      -Wno-unused-parameter
      -Wno-unused-value
      -Wno-self-assign
      )
    if (USE_CCACHE)
      add_compile_options(
        -Wno-register
        -Wno-unused-command-line-argument)
    endif()
  endif()
  if (USE_WERROR)
    add_compile_options(-Werror)
  endif()
  if (GCC)
    add_compile_options(-Wno-pragmas)
    # Otherwise `state [[maybe_unused]] int x;` will issue a warning.
    # https://stackoverflow.com/questions/50646334/maybe-unused-on-member-variable-gcc-warns-incorrectly-that-attribute-is
    add_compile_options(-Wno-attributes)
  elseif(ICC)
    add_compile_options(-wd1879 -wd1011)
    add_link_options(-static-intel)
  endif()
  add_compile_options(-Wno-error=format
    -Wunused-variable
    -Wno-deprecated
    -fvisibility=hidden
    -Wreturn-type
    -fPIC)
  if (CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "^x86")
    add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Wclass-memaccess>)
  endif()
  if (GPERFTOOLS_FOUND AND GCC)
    add_compile_options(
      -fno-builtin-malloc
      -fno-builtin-calloc
      -fno-builtin-realloc
      -fno-builtin-free)
  endif()

  if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
    # Graviton or later
    # https://github.com/aws/aws-graviton-gettting-started
    add_compile_options(-march=armv8-a+crc+simd)
  endif()

  # Check whether we can use dtrace probes
  include(CheckSymbolExists)
  check_symbol_exists(DTRACE_PROBE sys/sdt.h SUPPORT_DTRACE)
  check_symbol_exists(aligned_alloc stdlib.h HAS_ALIGNED_ALLOC)
  message(STATUS "Has aligned_alloc: ${HAS_ALIGNED_ALLOC}")
  if((SUPPORT_DTRACE) AND (USE_DTRACE))
    add_compile_definitions(DTRACE_PROBES)
  endif()
  if(HAS_ALIGNED_ALLOC)
    add_compile_definitions(HAS_ALIGNED_ALLOC)
  endif()

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

