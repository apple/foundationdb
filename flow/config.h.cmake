#cmakedefine ALLOC_INSTRUMENTATION
#cmakedefine NDEBUG
#cmakedefine FDB_RELEASE
#ifdef FDB_RELEASE
# define FDB_CLEAN_BUILD
#endif // FDB_RELEASE
#cmakedefine OPEN_FOR_IDE
#ifdef WIN32
# define USE_USEFIBERS
# define _WIN32_WINNT ${WINDOWS_TARGET}
# define WINVER ${WINDOWS_TARGET}
# define NTDDI_VERSION 0x05020000
# define BOOST_ALL_NO_LIB
#else
# cmakedefine USE_UCONTEXT
# cmakedefine USE_ASAN
# cmakedefine USE_MSAN
# cmakedefine USE_UBSAN
# cmakedefine USE_UCONTEXT
# if defined(USE_ASAN) || \
     defined(USE_MSAN) || \
     defined(USE_UBSAN) || \
     defined(USE_TSAN)
#  define DUSE_SANITIZER
# #endif
# #ifdef USE_ASAN
#  define ADDRESS_SANITIZER
# endif // USE_ASAN
# ifdef USE_MSAN
#  define MEMORY_SANITIZER
# endif
# ifdef USE_UBSAN
#  define UNDEFINED_BEHAVIOR_SANITIZER
# endif
# ifdef USE_TSAN
#  define THREAD_SANITIZER
#  define DYNAMIC_ANNOTATIONS_EXTERNAL_IMPL 1
# endif
# cmakedefine USE_GCOV
# cmakedefine USE_VALGRIND
# ifdef USE_VALGRIND
#  define VALGRIND 1
# endif
# cmakedefine WITH_LIBCXX
# if !defined(WITH_LIBCXX) && defined(__APPLE__)
#  define WITH_LIBCXX
# endif
# cmakedefine DTRACE_PROBES
# cmakedefine HAS_ALIGNED_ALLOC
#endif // WIN32
