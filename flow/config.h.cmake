#cmakedefine ALLOC_INSTRUMENTATION
#cmakedefine NDEBUG
#cmakedefine FDB_RELEASE
#ifdef FDB_RELEASE
# define FDB_CLEAN_BUILD
#endif // FDB_RELEASE
#cmakedefine OPEN_FOR_IDE
#ifdef WIN32
# define _WIN32_WINNT ${WINDOWS_TARGET}
# define WINVER ${WINDOWS_TARGET}
# define NTDDI_VERSION 0x05020000
# define BOOST_ALL_NO_LIB
#else
# cmakedefine USE_ASAN
# cmakedefine USE_MSAN
# cmakedefine USE_UBSAN
# cmakedefine USE_TSAN
# if defined(USE_ASAN) || \
     defined(USE_MSAN) || \
     defined(USE_UBSAN) || \
     defined(USE_TSAN)
#  define USE_SANITIZER
# endif
# cmakedefine USE_GCOV
# cmakedefine USE_VALGRIND
# ifdef USE_VALGRIND
#  define VALGRIND 1
# endif
# cmakedefine DTRACE_PROBES
# cmakedefine HAS_ALIGNED_ALLOC
# cmakedefine USE_JEMALLOC
#endif // WIN32
