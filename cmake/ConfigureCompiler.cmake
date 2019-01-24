set(USE_GPERFTOOLS OFF CACHE BOOL "Use gperfools for profiling")
set(PORTABLE_BINARY OFF CACHE BOOL "Create a binary that runs on older OS versions")
set(USE_VALGRIND OFF CACHE BOOL "Compile for valgrind usage")
set(USE_GOLD_LINKER OFF CACHE BOOL "Use gold linker")
set(ALLOC_INSTRUMENTATION OFF CACHE BOOL "Instrument alloc")
set(WITH_UNDODB OFF CACHE BOOL "Use rr or undodb")
set(OPEN_FOR_IDE OFF CACHE BOOL "Open this in an IDE (won't compile/link)")
set(FDB_RELEASE OFF CACHE BOOL "This is a building of a final release")

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
endif()

include_directories(${CMAKE_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})
if (NOT OPEN_FOR_IDE)
  add_definitions(-DNO_INTELLISENSE)
endif()
add_definitions(-DUSE_UCONTEXT)
enable_language(ASM)

include(CheckFunctionExists)
set(CMAKE_REQUIRED_INCLUDES stdlib.h malloc.h)
set(CMAKE_REQUIRED_LIBRARIES c)


if(WIN32)
  add_compile_options(/W3 /EHsc)
else()
  if(USE_GOLD_LINKER)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=gold -Wl,--disable-new-dtags")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=gold -Wl,--disable-new-dtags")
  endif()

  set(GCC NO)
  set(CLANG NO)
  if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
    set(CLANG YES)
  else()
    # This is not a very good test. However, as we do not really support many architectures
    # this is good enough for now
    set(GCC YES)
  endif()

  # we always compile with debug symbols. CPack will strip them out
  # and create a debuginfo rpm
  add_compile_options(-ggdb)
  set(USE_ASAN OFF CACHE BOOL "Compile with address sanitizer")
  if(USE_ASAN)
    add_compile_options(
      -fno-omit-frame-pointer -fsanitize=address
      -DUSE_ASAN)
    set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -fno-omit-frame-pointer -fsanitize=address")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fno-omit-frame-pointer -fsanitize=address")
    set(CMAKE_EXE_LINKER_FLAGS    "${CMAKE_EXE_LINKER_FLAGS}    -fno-omit-frame-pointer -fsanitize=address ${CMAKE_THREAD_LIBS_INIT}")
  endif()

  if(PORTABLE_BINARY)
    message(STATUS "Create a more portable binary")
    set(CMAKE_MODULE_LINKER_FLAGS "-static-libstdc++ -static-libgcc ${CMAKE_MODULE_LINKER_FLAGS}")
    set(CMAKE_SHARED_LINKER_FLAGS "-static-libstdc++ -static-libgcc ${CMAKE_SHARED_LINKER_FLAGS}")
    set(CMAKE_EXE_LINKER_FLAGS    "-static-libstdc++ -static-libgcc ${CMAKE_EXE_LINKER_FLAGS}")
  endif()
  # Instruction sets we require to be supported by the CPU
  add_compile_options(
    -maes
    -mmmx
    -mavx
    -msse4.2)
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-std=c++11>)
  if (USE_VALGRIND)
    add_compile_options(-DVALGRIND -DUSE_VALGRIND)
  endif()
  if (CLANG)
    if (APPLE)
      add_compile_options(-stdlib=libc++)
    endif()
    add_compile_options(
      -Wno-unknown-warning-option
      -Wno-dangling-else
      -Wno-sign-compare
      -Wno-comment
      -Wno-unknown-pragmas
      -Wno-delete-non-virtual-dtor
      -Wno-undefined-var-template
      -Wno-unused-value
      -Wno-tautological-pointer-compare
      -Wno-format)
  endif()
  if (CMAKE_GENERATOR STREQUAL Xcode)
  else()
    add_compile_options(-Werror)
  endif()
  add_compile_options($<$<BOOL:${GCC}>:-Wno-pragmas>)
  add_compile_options(-Wno-error=format
    -Wno-deprecated
    -fvisibility=hidden
    -Wreturn-type
    -fdiagnostics-color=always
    -fPIC)

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
