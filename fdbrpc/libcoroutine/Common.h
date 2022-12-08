
// metadoc Common copyright Steve Dekorte 2002
// metadoc Common license BSD revised
/*metadoc Common description
This is a header that all other source files should include.
These defines are helpful for doing OS specific checks in the code.
 */

#ifndef IOCOMMON_DEFINED
#define IOCOMMON_DEFINED 1

/*#define LOW_MEMORY_SYSTEM 1*/
#include <stdlib.h>
#include <string.h>
#include <stddef.h>

#if defined(__SVR4) && defined(__sun)
#include <inttypes.h>
#elif defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__) || defined(__DragonFly__)
#include <inttypes.h>
#elif !defined(__SYMBIAN32__) && !defined(__NeXT__)
#include <stdint.h>
#else
typedef unsigned char uint8_t;
typedef signed char int8_t;
typedef unsigned short uint16_t;
typedef signed short int16_t;
typedef unsigned long uint32_t;
typedef signed long int32_t;
/*
 typedef unsigned long uint64_t;
 typedef signed long int64_t;
 */
typedef unsigned long long uint64_t;
typedef long long int64_t;
#endif

/* Windows stuff */

#if defined(WIN32) || defined(__WINS__) || defined(__MINGW32__) || defined(_MSC_VER)
#define inline __inline
// #define snprintf _snprintf
#define usleep(x) Sleep(((x) + 999) / 1000)

#define HAS_FIBERS 1

#define ON_WINDOWS 1

// this also includes windows.h
#include <winsock2.h>

// Enable fibers
#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0400
#endif

#if !defined(__MINGW32__) && 0
#if defined(BUILDING_BASEKIT_DLL) || defined(BUILDING_IOVMALL_DLL)
#define BASEKIT_API __declspec(dllexport)
#else
#define BASEKIT_API __declspec(dllimport)
#endif
#else
#define BASEKIT_API
#endif
/*
#ifndef _SYS_STDINT_H_
#include "PortableStdint.h"
#endif
 */

#if !defined(__MINGW32__)
/* disable compile warnings which are always treated
as errors in my dev settings */

#pragma warning(disable : 4244)
/* warning C4244: 'function' : conversion from 'double ' to 'int ', possible loss of data */

#pragma warning(disable : 4996)
/* warning C4996: 'function' : This function or variable may be unsafe. Consider using 'function_s' instead */

#pragma warning(disable : 4018)
/* warning C4018: 'operator' : signed/unsigned mismatch */

/*#pragma warning( disable : 4090 ) */
/* warning C4090: 'function' : different 'const' qualifiers  */

/*#pragma warning( disable : 4024 )*/
/* warning C4024: different types for formal and actual parameter  */

/*#pragma warning( disable : 4761 ) */
/* warning C4761: integral size mismatch in argument; conversion supplied  */

/*#pragma warning( disable : 4047 ) */
/* warning C4047: '=' : 'char *' differs in levels of indirection from 'int '  */
#define ARCHITECTURE_x86 1
#endif

/* io_malloc, io_realloc, io_free undefined */
#if !defined(__SYMBIAN32__)
#include <memory.h>

/* strlen undefined */
#include <string.h>
#include <malloc.h> /* for calloc */
#endif
#else

// Not on windows so define this away
#define BASEKIT_API

#endif

/*
 [DBCS Enabling]

 DBCS (Short for Double-Byte Character Set), a character set that uses two-byte (16-bit) characters. Some languages,
 such as Chinese, Japanese and Korean (CJK), have writing schemes with many different characters that cannot be
 represented with single-byte codes such as ASCII and EBCDIC.

 In CJK world, CES (Character Encoding Scheme) and CCS (Coded Character Set) are actually different concept(one CES may
 contain multiple CCS). For example, EUC-JP is a CES which includes CCS of ASCII and JIS X 0208 (optionally JIS X 0201
 Kana and JIS X 0212).

 In Japanese (because I am Japanese),
 While EUC-JP and UTF-8 Map ASCII unchanged, ShiftJIS not (However ShiftJIS is de facto standard in Japan). For example,
 {0x95, 0x5c} represents one character. in ASCII, second byte(0x5c) is back slash character.
 */

/*
 check whether double-byte character. supported only ShiftJIS.
 if you want to use ShiftJIS characters in string literal, set compiler option -DDBCS_ENABLED=1.
 */

#if DBCS_ENABLED
#define ismbchar(c) ISSJIS((unsigned char)c)
#define mbcharlen(c) 2
#define ISSJIS(c) ((c >= 0x81 && c <= 0x9f) || (c >= 0xe0 && c <= 0xfc))
#else
#define ismbchar(c) 0
#define mbcharlen(c) 1
#endif /* DBCS_ENABLED */

#ifdef __cplusplus
extern "C" {
#endif

// #define IO_CHECK_ALLOC ENABLED(NOT_IN_CLEAN)

#ifdef IO_CHECK_ALLOC
BASEKIT_API size_t io_memsize(void* ptr);

#define io_malloc(size) io_real_malloc(size, __FILE__, __LINE__)
BASEKIT_API void* io_real_malloc(size_t size, char* file, int line);

#define io_calloc(count, size) io_real_calloc(count, size, __FILE__, __LINE__)
BASEKIT_API void* io_real_calloc(size_t count, size_t size, char* file, int line);

#define io_realloc(ptr, size) io_real_realloc(ptr, size, __FILE__, __LINE__)
BASEKIT_API void* io_real_realloc(void* ptr, size_t newSize, char* file, int line);

BASEKIT_API void io_free(void* ptr);
BASEKIT_API void io_show_mem(char* s);
BASEKIT_API size_t io_maxAllocatedBytes(void);
BASEKIT_API void io_resetMaxAllocatedBytes(void);
BASEKIT_API size_t io_frees(void);
BASEKIT_API size_t io_allocs(void);
BASEKIT_API size_t io_allocatedBytes(void);

BASEKIT_API void io_showUnfreed(void);
#else
#define io_memsize
#define io_malloc malloc
#define io_calloc calloc
#define io_realloc io_freerealloc
#define io_free free
#define io_show_mem

#define io_maxAllocatedBytes() 0
#define io_frees() 0
#define io_allocs() 0
#define io_allocatedBytes() 0
#define io_resetMaxAllocatedBytes()
#endif

BASEKIT_API void* cpalloc(const void* p, size_t size);
BASEKIT_API void* io_freerealloc(void* p, size_t size);

int io_isBigEndian(void);
BASEKIT_API uint32_t io_uint32InBigEndian(uint32_t i);

#ifdef __cplusplus
}
#endif

#endif
