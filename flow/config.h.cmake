#cmakedefine ALLOC_INSTRUMENTATION
#cmakedefine NDEBUG
#cmakedefine FDB_RELEASE
#ifdef FDB_RELEASE
# define FDB_CLEAN_BUILD
#endif // FDB_RELEASE
#cmakedefine OPEN_FOR_IDE
#define FDB_SOURCE_DIR "${CMAKE_SOURCE_DIR}"
#define FDB_BINARY_DIR "${CMAKE_BINARY_DIR}"
#cmakedefine USE_ASAN
#cmakedefine USE_MSAN
#cmakedefine USE_UBSAN
#cmakedefine USE_TSAN
#if defined(USE_ASAN) || \
    defined(USE_MSAN) || \
    defined(USE_UBSAN) || \
    defined(USE_TSAN)
# define USE_SANITIZER
#endif
#cmakedefine USE_GCOV
#cmakedefine USE_VALGRIND
#ifdef USE_VALGRIND
# define VALGRIND 1
#endif
#cmakedefine DTRACE_PROBES
#cmakedefine HAS_ALIGNED_ALLOC
#cmakedefine USE_JEMALLOC
