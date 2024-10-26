include(CompilerChecks)

env_set(USE_GPERFTOOLS OFF BOOL "Use gperfools for profiling")
env_set(USE_DTRACE ON BOOL "Enable dtrace probes on supported platforms")
env_set(USE_VALGRIND OFF BOOL "Compile for valgrind usage")
env_set(USE_VALGRIND_FOR_CTEST ${USE_VALGRIND} BOOL "Use valgrind for ctest")
env_set(ALLOC_INSTRUMENTATION OFF BOOL "Instrument alloc")
env_set(USE_ASAN OFF BOOL "Compile with address sanitizer")
env_set(USE_GCOV OFF BOOL "Compile with gcov instrumentation")
env_set(USE_MSAN OFF BOOL "Compile with memory sanitizer. To avoid false positives you need to dynamically link to a msan-instrumented libc++ and libc++abi, which you must compile separately. See https://github.com/google/sanitizers/wiki/MemorySanitizerLibcxxHowTo#instrumented-libc.")
env_set(USE_TSAN OFF BOOL "Compile with thread sanitizer. It is recommended to dynamically link to a tsan-instrumented libc++ and libc++abi, which you can compile separately.")
env_set(USE_UBSAN OFF BOOL "Compile with undefined behavior sanitizer")
env_set(FDB_RELEASE_CANDIDATE OFF BOOL "This is a building of a release candidate")
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
env_set(TRACE_PC_GUARD_INSTRUMENTATION_LIB "" STRING "Path to a library containing an implementation for __sanitizer_cov_trace_pc_guard. See https://clang.llvm.org/docs/SanitizerCoverage.html for more info.")
env_set(PROFILE_INSTR_GENERATE OFF BOOL "If set, build FDB as an instrumentation build to generate profiles")
env_set(PROFILE_INSTR_USE "" STRING "If set, build FDB with profile")
env_set(FULL_DEBUG_SYMBOLS OFF BOOL "Generate full debug symbols")
env_set(ENABLE_LONG_RUNNING_TESTS OFF BOOL "Add a long running tests package")

set(USE_SANITIZER OFF)
if(USE_ASAN OR USE_VALGRIND OR USE_MSAN OR USE_TSAN OR USE_UBSAN)
  set(USE_SANITIZER ON)
endif()

set(jemalloc_default ON)
# We don't want to use jemalloc on Windows
# Nor on FreeBSD, where jemalloc is the default system allocator
if(USE_SANITIZER OR WIN32 OR (CMAKE_SYSTEM_NAME STREQUAL "FreeBSD") OR APPLE)
  set(jemalloc_default OFF)
endif()
env_set(USE_JEMALLOC ${jemalloc_default} BOOL "Link with jemalloc")
env_set(USE_CUSTOM_JEMALLOC OFF BOOL "Manually download and build jemalloc")

if(USE_LIBCXX AND STATIC_LINK_LIBCXX AND NOT USE_LD STREQUAL "LLD")
  message(FATAL_ERROR "Unsupported configuration: STATIC_LINK_LIBCXX with libc++ only works if USE_LD=LLD")
endif()
if(STATIC_LINK_LIBCXX AND USE_TSAN)
  message(FATAL_ERROR "Unsupported configuration: STATIC_LINK_LIBCXX doesn't work with tsan")
endif()
if(STATIC_LINK_LIBCXX AND USE_MSAN)
  message(FATAL_ERROR "Unsupported configuration: STATIC_LINK_LIBCXX doesn't work with msan")
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

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

if(WIN32)
  add_definitions(-DBOOST_USE_WINDOWS_H)
  add_definitions(-DWIN32_LEAN_AND_MEAN)
  add_definitions(-D_ITERATOR_DEBUG_LEVEL=0)
  add_definitions(-DNOGDI) # WinGDI.h defines macro ERROR
  add_definitions(-D_USE_MATH_DEFINES) # Math constants
endif()

if(APPLE)
# Remove this after boost 1.81 or above is used
add_definitions(-D_LIBCPP_ENABLE_CXX17_REMOVED_UNARY_BINARY_FUNCTION)
endif()

if (USE_CCACHE)
  find_program(CCACHE_PROGRAM "ccache" REQUIRED)
  set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
  set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
endif()

include(CheckFunctionExists)
set(CMAKE_REQUIRED_INCLUDES stdlib.h malloc.h)
set(CMAKE_REQUIRED_LIBRARIES c)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

if(NOT OPEN_FOR_IDE)
  add_compile_definitions(NO_INTELLISENSE)
endif()

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
  add_compile_definitions(NOMINMAX)
  set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
else()
  set(GCC NO)
  set(CLANG NO)
  set(ICX NO)
  # CMAKE_CXX_COMPILER_ID is set to Clang even if CMAKE_CXX_COMPILER points to ICPX, so we have to check the compiler too.
  if (${CMAKE_CXX_COMPILER} MATCHES ".*icpx")
    set(ICX YES)
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
    set(CLANG YES)
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

  set(SANITIZER_COMPILE_OPTIONS)
  set(SANITIZER_LINK_OPTIONS)

  add_compile_options(-fno-omit-frame-pointer)

  if(CLANG)
    # The default DWARF 5 format does not play nicely with GNU Binutils 2.39 and earlier, resulting
    # in tools like addr2line omitting line numbers. We can consider removing this once we are able 
    # to use a version that has a fix.
    add_compile_options(-gdwarf-4)
  endif()

  if(FDB_RELEASE OR FULL_DEBUG_SYMBOLS OR CMAKE_BUILD_TYPE STREQUAL "Debug")
    # Configure with FULL_DEBUG_SYMBOLS=ON to generate all symbols for debugging with gdb
    # Also generating full debug symbols in release builds. CPack will strip them out
    # and create a debuginfo rpm
    add_compile_options(-ggdb)
  else()
    # Generating minimal debug symbols by default. They are sufficient for testing purposes
    add_compile_options(-ggdb1)
  endif()

  if(NOT FDB_RELEASE)
    # Enable compression of the debug sections. This reduces the size of the binaries several times. 
    # We do not enable it release builds, because CPack fails to generate debuginfo packages when
    # compression is enabled
    add_compile_options(-gz)
    add_link_options(-gz)
  endif()

  if(TRACE_PC_GUARD_INSTRUMENTATION_LIB)
      add_compile_options(-fsanitize-coverage=trace-pc-guard)
      link_libraries(${TRACE_PC_GUARD_INSTRUMENTATION_LIB})
  endif()
  if(USE_ASAN)
    list(APPEND SANITIZER_COMPILE_OPTIONS
      -fsanitize=address
      -DADDRESS_SANITIZER
      -DBOOST_USE_ASAN
      -DBOOST_USE_UCONTEXT)
    list(APPEND SANITIZER_LINK_OPTIONS -fsanitize=address)
  endif()

  if(USE_MSAN)
    if(NOT CLANG)
      message(FATAL_ERROR "Unsupported configuration: USE_MSAN only works with Clang")
    endif()
    list(APPEND SANITIZER_COMPILE_OPTIONS
      -fsanitize=memory
      -fsanitize-memory-track-origins=2
      -DBOOST_USE_UCONTEXT)
    list(APPEND SANITIZER_LINK_OPTIONS -fsanitize=memory)
  endif()

  if(USE_GCOV)
    add_compile_options(--coverage)
    add_link_options(--coverage)
  endif()

  if(USE_UBSAN)
    list(APPEND SANITIZER_COMPILE_OPTIONS
      -fsanitize=undefined
      # TODO(atn34) Re-enable -fsanitize=alignment once https://github.com/apple/foundationdb/issues/1434 is resolved
      -fno-sanitize=alignment
      # https://github.com/apple/foundationdb/issues/7955
      -fno-sanitize=function
      -DBOOST_USE_UCONTEXT)
    list(APPEND SANITIZER_LINK_OPTIONS -fsanitize=undefined)
  endif()

  if(USE_TSAN)
    list(APPEND SANITIZER_COMPILE_OPTIONS -fsanitize=thread -DBOOST_USE_UCONTEXT)
    list(APPEND SANITIZER_LINK_OPTIONS -fsanitize=thread)
  endif()

  if(USE_VALGRIND)
    list(APPEND SANITIZER_COMPILE_OPTIONS -DBOOST_USE_VALGRIND)
  endif()

  if(SANITIZER_COMPILE_OPTIONS)
    add_compile_options(${SANITIZER_COMPILE_OPTIONS})
  endif()
  if(SANITIZER_LINK_OPTIONS)
    add_link_options(${SANITIZER_LINK_OPTIONS})
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
  set(USE_AVX OFF CACHE BOOL "Enable AVX instructions")
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

  if (USE_LIBCXX)
    # Make sure that libc++ can be found be the platform's loader, so that thing's like cmake's "try_run" work.
    find_library(LIBCXX_SO_PATH c++ /usr/local/lib)
    if (LIBCXX_SO_PATH)
      get_filename_component(LIBCXX_SO_DIR ${LIBCXX_SO_PATH} DIRECTORY)
      if (APPLE)
        set(ENV{DYLD_LIBRARY_PATH} "$ENV{DYLD_LIBRARY_PATH}:${LIBCXX_SO_DIR}")
      else()
        set(ENV{LD_LIBRARY_PATH} "$ENV{LD_LIBRARY_PATH}:${LIBCXX_SO_DIR}")
      endif()
    endif()
  endif()

  if (CLANG OR ICX)
    if (APPLE OR USE_LIBCXX)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
      if (NOT APPLE)
        if (STATIC_LINK_LIBCXX)
          set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -nostdlib++ -Wl,-Bstatic -lc++ -lc++abi -Wl,-Bdynamic")
          set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -static-libgcc -nostdlib++ -Wl,-Bstatic -lc++ -lc++abi -Wl,-Bdynamic")
        endif()
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -stdlib=libc++ -Wl,-build-id=sha1")
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -stdlib=libc++ -Wl,-build-id=sha1")
      endif()
    endif()
    if (NOT APPLE AND NOT USE_LIBCXX)
      message(STATUS "Linking libatomic")
      set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -latomic")
      set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -latomic")
    endif()
    if (OPEN_FOR_IDE)
      add_compile_options(
        -Wno-unknown-attributes)
    endif()
    add_compile_options(
      -Wall
      -Wextra
      -Wredundant-move
      -Wpessimizing-move
      -Woverloaded-virtual
      -Wshift-sign-overflow
      # Here's the current set of warnings we need to explicitly disable to compile warning-free with clang 11
      -Wno-sign-compare
      -Wno-undefined-var-template
      -Wno-unknown-warning-option
      -Wno-unused-parameter
      -Wno-constant-logical-operand
      # These need to be disabled for FDB's RocksDB storage server implementation
      -Wno-deprecated-copy
      -Wno-delete-non-abstract-non-virtual-dtor
      -Wno-range-loop-construct
      -Wno-reorder-ctor
      # Needed for clang 13 (todo: Update above logic so that it figures out when to pass in -static-libstdc++ and when it will be ignored)
      # When you remove this, you might need to move it back to the USE_CCACHE stanza.  It was (only) there before I moved it here.
      -Wno-unused-command-line-argument
      # Disable C++ 20 warning for ambiguous operator.
      -Wno-ambiguous-reversed-operator
      )
    if (USE_CCACHE)
      add_compile_options(
        -Wno-register
      )
    endif()
    if (PROFILE_INSTR_GENERATE)
      add_compile_options(-fprofile-instr-generate)
      add_link_options(-fprofile-instr-generate)
    endif()
    if (NOT (PROFILE_INSTR_USE STREQUAL ""))
      if (PROFILE_INSTR_GENERATE)
          message(FATAL_ERROR "Can't set both PROFILE_INSTR_GENERATE and PROFILE_INSTR_USE")
      endif()
      add_compile_options(-Wno-error=profile-instr-out-of-date -Wno-error=profile-instr-unprofiled)
      add_compile_options(-fprofile-instr-use=${PROFILE_INSTR_USE})
      add_link_options(-fprofile-instr-use=${PROFILE_INSTR_USE})
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
  endif()
  add_compile_options(-Wno-error=format
    -Wunused-variable
    -Wno-deprecated
    -fvisibility=hidden
    -Wreturn-type
    -fPIC)
  if (CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "^x86" AND NOT CLANG AND NOT ICX)
    add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Wclass-memaccess>)
  endif()
  if (GPERFTOOLS_FOUND AND GCC)
    add_compile_options(
      -fno-builtin-malloc
      -fno-builtin-calloc
      -fno-builtin-realloc
      -fno-builtin-free)
  endif()
  
  if (ICX)
    find_library(CXX_LIB NAMES c++ PATHS "/usr/local/lib" REQUIRED)
    find_library(CXX_ABI_LIB NAMES c++abi PATHS "/usr/local/lib" REQUIRED)
    add_compile_options($<$<COMPILE_LANGUAGE:C>:-ffp-contract=on>)
    add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-ffp-contract=on>)
    # No libc++ and libc++abi in Intel LIBRARY_PATH
    link_libraries(${CXX_LIB} ${CXX_ABI_LIB})
  endif()

  if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
    # Graviton2 or later
    # https://github.com/aws/aws-graviton-gettting-started
    add_compile_options(-march=armv8.2-a+crc+simd)
  endif()

  if (CMAKE_SYSTEM_PROCESSOR MATCHES "ppc64le")
    add_compile_options(-m64 -mcpu=power9 -mtune=power9 -DNO_WARN_X86_INTRINSICS)
  endif()
  # Check whether we can use dtrace probes
  include(CheckSymbolExists)
  check_symbol_exists(DTRACE_PROBE sys/sdt.h SUPPORT_DTRACE)
  check_symbol_exists(aligned_alloc stdlib.h HAS_ALIGNED_ALLOC)
  message(STATUS "Has aligned_alloc: ${HAS_ALIGNED_ALLOC}")
  if((SUPPORT_DTRACE) AND (USE_DTRACE))
    set(DTRACE_PROBES 1)
  endif()

  set(USE_LTO OFF CACHE BOOL "Do link time optimization")
  if (USE_LTO)
    if (CLANG)
      set(CLANG_LTO_STRATEGY "Thin" CACHE STRING "LLVM LTO strategy (Thin, or Full)")
      if (CLANG_LTO_STRATEGY STREQUAL "Full")
        add_compile_options($<$<CONFIG:Release>:-flto=full>)
      else()
        add_compile_options($<$<CONFIG:Release>:-flto=thin>)
      endif()
      set(CMAKE_RANLIB "llvm-ranlib")
      set(CMAKE_AR "llvm-ar")
    endif()
    if(CMAKE_COMPILER_IS_GNUCXX)
      add_compile_options($<$<CONFIG:Release>:-flto>)
      set(CMAKE_AR  "gcc-ar")
      set(CMAKE_C_ARCHIVE_CREATE "<CMAKE_AR> qcs <TARGET> <LINK_FLAGS> <OBJECTS>")
      set(CMAKE_C_ARCHIVE_FINISH   true)
      set(CMAKE_CXX_ARCHIVE_CREATE "<CMAKE_AR> qcs <TARGET> <LINK_FLAGS> <OBJECTS>")
      set(CMAKE_CXX_ARCHIVE_FINISH   true)
    endif()
  endif()
endif()
