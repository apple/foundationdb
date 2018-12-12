// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#ifndef ABSL_DEBUGGING_INTERNAL_ADDRESS_IS_READABLE_H_
#define ABSL_DEBUGGING_INTERNAL_ADDRESS_IS_READABLE_H_

namespace absl {
namespace debug_internal {

// Return whether the byte at *addr is readable, without faulting.
// Save and restores errno.
bool AddressIsReadable(const void *addr);

}  // namespace debug_internal
}  // namespace absl

#endif  // ABSL_DEBUGGING_INTERNAL_ADDRESS_IS_READABLE_H_
/*
 * Copyright 2017 The Abseil Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Allow dynamic symbol lookup for in-memory Elf images.

#ifndef ABSL_DEBUGGING_INTERNAL_ELF_MEM_IMAGE_H_
#define ABSL_DEBUGGING_INTERNAL_ELF_MEM_IMAGE_H_

// Including this will define the __GLIBC__ macro if glibc is being
// used.
#include <climits>

// Maybe one day we can rewrite this file not to require the elf
// symbol extensions in glibc, but for right now we need them.
#ifdef ABSL_HAVE_ELF_MEM_IMAGE
#error ABSL_HAVE_ELF_MEM_IMAGE cannot be directly set
#endif

#if defined(__ELF__) && defined(__GLIBC__) && !defined(__native_client__) && \
    !defined(__asmjs__)
#define ABSL_HAVE_ELF_MEM_IMAGE 1
#endif

#if ABSL_HAVE_ELF_MEM_IMAGE

#include <link.h>  // for ElfW

namespace absl {
namespace debug_internal {

// An in-memory ELF image (may not exist on disk).
class ElfMemImage {
 public:
  // Sentinel: there could never be an elf image at this address.
  static const void *const kInvalidBase;

  // Information about a single vdso symbol.
  // All pointers are into .dynsym, .dynstr, or .text of the VDSO.
  // Do not free() them or modify through them.
  struct SymbolInfo {
    const char      *name;      // E.g. "__vdso_getcpu"
    const char      *version;   // E.g. "LINUX_2.6", could be ""
                                // for unversioned symbol.
    const void      *address;   // Relocated symbol address.
    const ElfW(Sym) *symbol;    // Symbol in the dynamic symbol table.
  };

  // Supports iteration over all dynamic symbols.
  class SymbolIterator {
   public:
    friend class ElfMemImage;
    const SymbolInfo *operator->() const;
    const SymbolInfo &operator*() const;
    SymbolIterator& operator++();
    bool operator!=(const SymbolIterator &rhs) const;
    bool operator==(const SymbolIterator &rhs) const;
   private:
    SymbolIterator(const void *const image, int index);
    void Update(int incr);
    SymbolInfo info_;
    int index_;
    const void *const image_;
  };


  explicit ElfMemImage(const void *base);
  void                 Init(const void *base);
  bool                 IsPresent() const { return ehdr_ != nullptr; }
  const ElfW(Phdr)*    GetPhdr(int index) const;
  const ElfW(Sym)*     GetDynsym(int index) const;
  const ElfW(Versym)*  GetVersym(int index) const;
  const ElfW(Verdef)*  GetVerdef(int index) const;
  const ElfW(Verdaux)* GetVerdefAux(const ElfW(Verdef) *verdef) const;
  const char*          GetDynstr(ElfW(Word) offset) const;
  const void*          GetSymAddr(const ElfW(Sym) *sym) const;
  const char*          GetVerstr(ElfW(Word) offset) const;
  int                  GetNumSymbols() const;

  SymbolIterator begin() const;
  SymbolIterator end() const;

  // Look up versioned dynamic symbol in the image.
  // Returns false if image is not present, or doesn't contain given
  // symbol/version/type combination.
  // If info_out is non-null, additional details are filled in.
  bool LookupSymbol(const char *name, const char *version,
                    int symbol_type, SymbolInfo *info_out) const;

  // Find info about symbol (if any) which overlaps given address.
  // Returns true if symbol was found; false if image isn't present
  // or doesn't have a symbol overlapping given address.
  // If info_out is non-null, additional details are filled in.
  bool LookupSymbolByAddress(const void *address, SymbolInfo *info_out) const;

 private:
  const ElfW(Ehdr) *ehdr_;
  const ElfW(Sym) *dynsym_;
  const ElfW(Versym) *versym_;
  const ElfW(Verdef) *verdef_;
  const ElfW(Word) *hash_;
  const char *dynstr_;
  size_t strsize_;
  size_t verdefnum_;
  ElfW(Addr) link_base_;     // Link-time base (p_vaddr of first PT_LOAD).
};

}  // namespace debug_internal
}  // namespace absl

#endif  // ABSL_HAVE_ELF_MEM_IMAGE

#endif  // ABSL_DEBUGGING_INTERNAL_ELF_MEM_IMAGE_H_
/*
 * Copyright 2017 The Abseil Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.

 * Defines ABSL_STACKTRACE_INL_HEADER to the *-inl.h containing
 * actual unwinder implementation.
 * This header is "private" to stacktrace.cc.
 * DO NOT include it into any other files.
*/
#ifndef ABSL_DEBUGGING_INTERNAL_STACKTRACE_CONFIG_H_
#define ABSL_DEBUGGING_INTERNAL_STACKTRACE_CONFIG_H_

// First, test platforms which only support a stub.
#if ABSL_STACKTRACE_INL_HEADER
#error ABSL_STACKTRACE_INL_HEADER cannot be directly set
#elif defined(__native_client__) || defined(__APPLE__) || \
    defined(__ANDROID__) || defined(__myriad2__) || defined(asmjs__) || \
    defined(__Fuchsia__)
#define ABSL_STACKTRACE_INL_HEADER \
    "stacktrace_internal/stacktrace_unimplemented-inl.inc"

// Next, test for Mips and Windows.
// TODO(marmstrong): Mips case, remove the check for ABSL_STACKTRACE_INL_HEADER
#elif defined(__mips__) && !defined(ABSL_STACKTRACE_INL_HEADER)
#define ABSL_STACKTRACE_INL_HEADER \
    "stacktrace_internal/stacktrace_unimplemented-inl.inc"
#elif defined(_WIN32)  // windows
#define ABSL_STACKTRACE_INL_HEADER \
    "stacktrace_internal/stacktrace_win32-inl.inc"

// Finally, test NO_FRAME_POINTER.
#elif !defined(NO_FRAME_POINTER)
# if defined(__i386__) || defined(__x86_64__)
#define ABSL_STACKTRACE_INL_HEADER \
    "stacktrace_internal/stacktrace_x86-inl.inc"
# elif defined(__ppc__) || defined(__PPC__)
#define ABSL_STACKTRACE_INL_HEADER \
    "stacktrace_internal/stacktrace_powerpc-inl.inc"
# elif defined(__aarch64__)
#define ABSL_STACKTRACE_INL_HEADER \
    "stacktrace_internal/stacktrace_aarch64-inl.inc"
# elif defined(__arm__)
#define ABSL_STACKTRACE_INL_HEADER \
    "stacktrace_internal/stacktrace_arm-inl.inc"
# endif
#else  // defined(NO_FRAME_POINTER)
# if defined(__i386__) || defined(__x86_64__) || defined(__aarch64__)
#define ABSL_STACKTRACE_INL_HEADER \
    "stacktrace_internal/stacktrace_unimplemented-inl.inc"
# elif defined(__ppc__) || defined(__PPC__)
//  Use glibc's backtrace.
#define ABSL_STACKTRACE_INL_HEADER \
    "stacktrace_internal/stacktrace_generic-inl.inc"
# elif defined(__arm__)
#   error stacktrace without frame pointer is not supported on ARM
# endif
#endif  // NO_FRAME_POINTER

#if !defined(ABSL_STACKTRACE_INL_HEADER)
#error Not supported yet
#endif

#endif  // ABSL_DEBUGGING_INTERNAL_STACKTRACE_CONFIG_H_
//
// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

// Allow dynamic symbol lookup in the kernel VDSO page.
//
// VDSO stands for "Virtual Dynamic Shared Object" -- a page of
// executable code, which looks like a shared library, but doesn't
// necessarily exist anywhere on disk, and which gets mmap()ed into
// every process by kernels which support VDSO, such as 2.6.x for 32-bit
// executables, and 2.6.24 and above for 64-bit executables.
//
// More details could be found here:
// http://www.trilithium.com/johan/2005/08/linux-gate/
//
// VDSOSupport -- a class representing kernel VDSO (if present).
//
// Example usage:
//  VDSOSupport vdso;
//  VDSOSupport::SymbolInfo info;
//  typedef (*FN)(unsigned *, void *, void *);
//  FN fn = nullptr;
//  if (vdso.LookupSymbol("__vdso_getcpu", "LINUX_2.6", STT_FUNC, &info)) {
//     fn = reinterpret_cast<FN>(info.address);
//  }

#ifndef ABSL_DEBUGGING_INTERNAL_VDSO_SUPPORT_H_
#define ABSL_DEBUGGING_INTERNAL_VDSO_SUPPORT_H_

#include <atomic>


#ifdef ABSL_HAVE_ELF_MEM_IMAGE

#ifdef ABSL_HAVE_VDSO_SUPPORT
#error ABSL_HAVE_VDSO_SUPPORT cannot be directly set
#else
#define ABSL_HAVE_VDSO_SUPPORT 1
#endif

namespace absl {
namespace debug_internal {

// NOTE: this class may be used from within tcmalloc, and can not
// use any memory allocation routines.
class VDSOSupport {
 public:
  VDSOSupport();

  typedef ElfMemImage::SymbolInfo SymbolInfo;
  typedef ElfMemImage::SymbolIterator SymbolIterator;

  // On PowerPC64 VDSO symbols can either be of type STT_FUNC or STT_NOTYPE
  // depending on how the kernel is built.  The kernel is normally built with
  // STT_NOTYPE type VDSO symbols.  Let's make things simpler first by using a
  // compile-time constant.
#ifdef __powerpc64__
  enum { kVDSOSymbolType = STT_NOTYPE };
#else
  enum { kVDSOSymbolType = STT_FUNC };
#endif

  // Answers whether we have a vdso at all.
  bool IsPresent() const { return image_.IsPresent(); }

  // Allow to iterate over all VDSO symbols.
  SymbolIterator begin() const { return image_.begin(); }
  SymbolIterator end() const { return image_.end(); }

  // Look up versioned dynamic symbol in the kernel VDSO.
  // Returns false if VDSO is not present, or doesn't contain given
  // symbol/version/type combination.
  // If info_out != nullptr, additional details are filled in.
  bool LookupSymbol(const char *name, const char *version,
                    int symbol_type, SymbolInfo *info_out) const;

  // Find info about symbol (if any) which overlaps given address.
  // Returns true if symbol was found; false if VDSO isn't present
  // or doesn't have a symbol overlapping given address.
  // If info_out != nullptr, additional details are filled in.
  bool LookupSymbolByAddress(const void *address, SymbolInfo *info_out) const;

  // Used only for testing. Replace real VDSO base with a mock.
  // Returns previous value of vdso_base_. After you are done testing,
  // you are expected to call SetBase() with previous value, in order to
  // reset state to the way it was.
  const void *SetBase(const void *s);

  // Computes vdso_base_ and returns it. Should be called as early as
  // possible; before any thread creation, chroot or setuid.
  static const void *Init();

 private:
  // image_ represents VDSO ELF image in memory.
  // image_.ehdr_ == nullptr implies there is no VDSO.
  ElfMemImage image_;

  // Cached value of auxv AT_SYSINFO_EHDR, computed once.
  // This is a tri-state:
  //   kInvalidBase   => value hasn't been determined yet.
  //              0   => there is no VDSO.
  //           else   => vma of VDSO Elf{32,64}_Ehdr.
  //
  // When testing with mock VDSO, low bit is set.
  // The low bit is always available because vdso_base_ is
  // page-aligned.
  static std::atomic<const void *> vdso_base_;

  // NOLINT on 'long' because these routines mimic kernel api.
  // The 'cache' parameter may be used by some versions of the kernel,
  // and should be nullptr or point to a static buffer containing at
  // least two 'long's.
  static long InitAndGetCPU(unsigned *cpu, void *cache,     // NOLINT 'long'.
                            void *unused);
  static long GetCPUViaSyscall(unsigned *cpu, void *cache,  // NOLINT 'long'.
                               void *unused);
  typedef long (*GetCpuFn)(unsigned *cpu, void *cache,      // NOLINT 'long'.
                           void *unused);

  // This function pointer may point to InitAndGetCPU,
  // GetCPUViaSyscall, or __vdso_getcpu at different stages of initialization.
  static std::atomic<GetCpuFn> getcpu_fn_;

  friend int GetCPU(void);  // Needs access to getcpu_fn_.

  VDSOSupport(const VDSOSupport&) = delete;
  VDSOSupport& operator=(const VDSOSupport&) = delete;
};

// Same as sched_getcpu() on later glibc versions.
// Return current CPU, using (fast) __vdso_getcpu@LINUX_2.6 if present,
// otherwise use syscall(SYS_getcpu,...).
// May return -1 with errno == ENOSYS if the kernel doesn't
// support SYS_getcpu.
int GetCPU();

}  // namespace debug_internal
}  // namespace absl

#endif  // ABSL_HAVE_ELF_MEM_IMAGE

#endif  // ABSL_DEBUGGING_INTERNAL_VDSO_SUPPORT_H_
// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This header file defines macros for declaring attributes for functions,
// types, and variables.
//
// These macros are used within Abseil and allow the compiler to optimize, where
// applicable, certain function calls.
//
// This file is used for both C and C++!
//
// Most macros here are exposing GCC or Clang features, and are stubbed out for
// other compilers.
//
// GCC attributes documentation:
//   https://gcc.gnu.org/onlinedocs/gcc-4.7.0/gcc/Function-Attributes.html
//   https://gcc.gnu.org/onlinedocs/gcc-4.7.0/gcc/Variable-Attributes.html
//   https://gcc.gnu.org/onlinedocs/gcc-4.7.0/gcc/Type-Attributes.html
//
// Most attributes in this file are already supported by GCC 4.7. However, some
// of them are not supported in older version of Clang. Thus, we check
// `__has_attribute()` first. If the check fails, we check if we are on GCC and
// assume the attribute exists on GCC (which is verified on GCC 4.7).
//
// -----------------------------------------------------------------------------
// Sanitizer Attributes
// -----------------------------------------------------------------------------
//
// Sanitizer-related attributes are not "defined" in this file (and indeed
// are not defined as such in any file). To utilize the following
// sanitizer-related attributes within your builds, define the following macros
// within your build using a `-D` flag, along with the given value for
// `-fsanitize`:
//
//   * `ADDRESS_SANITIZER` + `-fsanitize=address` (Clang, GCC 4.8)
//   * `MEMORY_SANITIZER` + `-fsanitize=memory` (Clang-only)
//   * `THREAD_SANITIZER + `-fsanitize=thread` (Clang, GCC 4.8+)
//   * `UNDEFINED_BEHAVIOR_SANITIZER` + `-fsanitize=undefined` (Clang, GCC 4.9+)
//   * `CONTROL_FLOW_INTEGRITY` + -fsanitize=cfi (Clang-only)
//
// Example:
//
//   // Enable branches in the Abseil code that are tagged for ASan:
//   $ bazel -D ADDRESS_SANITIZER -fsanitize=address *target*
//
// Since these macro names are only supported by GCC and Clang, we only check
// for `__GNUC__` (GCC or Clang) and the above macros.
#ifndef ABSL_BASE_ATTRIBUTES_H_
#define ABSL_BASE_ATTRIBUTES_H_

// ABSL_HAVE_ATTRIBUTE
//
// A function-like feature checking macro that is a wrapper around
// `__has_attribute`, which is defined by GCC 5+ and Clang and evaluates to a
// nonzero constant integer if the attribute is supported or 0 if not.
//
// It evaluates to zero if `__has_attribute` is not defined by the compiler.
//
// GCC: https://gcc.gnu.org/gcc-5/changes.html
// Clang: https://clang.llvm.org/docs/LanguageExtensions.html
#ifdef __has_attribute
#define ABSL_HAVE_ATTRIBUTE(x) __has_attribute(x)
#else
#define ABSL_HAVE_ATTRIBUTE(x) 0
#endif

// ABSL_HAVE_CPP_ATTRIBUTE
//
// A function-like feature checking macro that accepts C++11 style attributes.
// It's a wrapper around `__has_cpp_attribute`, defined by ISO C++ SD-6
// (http://en.cppreference.com/w/cpp/experimental/feature_test). If we don't
// find `__has_cpp_attribute`, will evaluate to 0.
#if defined(__cplusplus) && defined(__has_cpp_attribute)
// NOTE: requiring __cplusplus above should not be necessary, but
// works around https://bugs.llvm.org/show_bug.cgi?id=23435.
#define ABSL_HAVE_CPP_ATTRIBUTE(x) __has_cpp_attribute(x)
#else
#define ABSL_HAVE_CPP_ATTRIBUTE(x) 0
#endif

// -----------------------------------------------------------------------------
// Function Attributes
// -----------------------------------------------------------------------------
//
// GCC: https://gcc.gnu.org/onlinedocs/gcc/Function-Attributes.html
// Clang: https://clang.llvm.org/docs/AttributeReference.html

// ABSL_PRINTF_ATTRIBUTE
// ABSL_SCANF_ATTRIBUTE
//
// Tells the compiler to perform `printf` format std::string checking if the
// compiler supports it; see the 'format' attribute in
// <http://gcc.gnu.org/onlinedocs/gcc-4.7.0/gcc/Function-Attributes.html>.
//
// Note: As the GCC manual states, "[s]ince non-static C++ methods
// have an implicit 'this' argument, the arguments of such methods
// should be counted from two, not one."
#if ABSL_HAVE_ATTRIBUTE(format) || (defined(__GNUC__) && !defined(__clang__))
#define ABSL_PRINTF_ATTRIBUTE(string_index, first_to_check) \
  __attribute__((__format__(__printf__, string_index, first_to_check)))
#define ABSL_SCANF_ATTRIBUTE(string_index, first_to_check) \
  __attribute__((__format__(__scanf__, string_index, first_to_check)))
#else
#define ABSL_PRINTF_ATTRIBUTE(string_index, first_to_check)
#define ABSL_SCANF_ATTRIBUTE(string_index, first_to_check)
#endif

// ABSL_ATTRIBUTE_ALWAYS_INLINE
// ABSL_ATTRIBUTE_NOINLINE
//
// Forces functions to either inline or not inline. Introduced in gcc 3.1.
#if ABSL_HAVE_ATTRIBUTE(always_inline) || \
    (defined(__GNUC__) && !defined(__clang__))
#define ABSL_ATTRIBUTE_ALWAYS_INLINE __attribute__((always_inline))
#define ABSL_HAVE_ATTRIBUTE_ALWAYS_INLINE 1
#else
#define ABSL_ATTRIBUTE_ALWAYS_INLINE
#endif

#if ABSL_HAVE_ATTRIBUTE(noinline) || (defined(__GNUC__) && !defined(__clang__))
#define ABSL_ATTRIBUTE_NOINLINE __attribute__((noinline))
#define ABSL_HAVE_ATTRIBUTE_NOINLINE 1
#else
#define ABSL_ATTRIBUTE_NOINLINE
#endif

// ABSL_ATTRIBUTE_NO_TAIL_CALL
//
// Prevents the compiler from optimizing away stack frames for functions which
// end in a call to another function.
#if ABSL_HAVE_ATTRIBUTE(disable_tail_calls)
#define ABSL_HAVE_ATTRIBUTE_NO_TAIL_CALL 1
#define ABSL_ATTRIBUTE_NO_TAIL_CALL __attribute__((disable_tail_calls))
#elif defined(__GNUC__) && !defined(__clang__)
#define ABSL_HAVE_ATTRIBUTE_NO_TAIL_CALL 1
#define ABSL_ATTRIBUTE_NO_TAIL_CALL \
  __attribute__((optimize("no-optimize-sibling-calls")))
#else
#define ABSL_ATTRIBUTE_NO_TAIL_CALL
#define ABSL_HAVE_ATTRIBUTE_NO_TAIL_CALL 0
#endif

// ABSL_ATTRIBUTE_WEAK
//
// Tags a function as weak for the purposes of compilation and linking.
#if ABSL_HAVE_ATTRIBUTE(weak) || (defined(__GNUC__) && !defined(__clang__))
#undef ABSL_ATTRIBUTE_WEAK
#define ABSL_ATTRIBUTE_WEAK __attribute__((weak))
#define ABSL_HAVE_ATTRIBUTE_WEAK 1
#else
#define ABSL_ATTRIBUTE_WEAK
#define ABSL_HAVE_ATTRIBUTE_WEAK 0
#endif

// ABSL_ATTRIBUTE_NONNULL
//
// Tells the compiler either (a) that a particular function parameter
// should be a non-null pointer, or (b) that all pointer arguments should
// be non-null.
//
// Note: As the GCC manual states, "[s]ince non-static C++ methods
// have an implicit 'this' argument, the arguments of such methods
// should be counted from two, not one."
//
// Args are indexed starting at 1.
//
// For non-static class member functions, the implicit `this` argument
// is arg 1, and the first explicit argument is arg 2. For static class member
// functions, there is no implicit `this`, and the first explicit argument is
// arg 1.
//
// Example:
//
//   /* arg_a cannot be null, but arg_b can */
//   void Function(void* arg_a, void* arg_b) ABSL_ATTRIBUTE_NONNULL(1);
//
//   class C {
//     /* arg_a cannot be null, but arg_b can */
//     void Method(void* arg_a, void* arg_b) ABSL_ATTRIBUTE_NONNULL(2);
//
//     /* arg_a cannot be null, but arg_b can */
//     static void StaticMethod(void* arg_a, void* arg_b)
//     ABSL_ATTRIBUTE_NONNULL(1);
//   };
//
// If no arguments are provided, then all pointer arguments should be non-null.
//
//  /* No pointer arguments may be null. */
//  void Function(void* arg_a, void* arg_b, int arg_c) ABSL_ATTRIBUTE_NONNULL();
//
// NOTE: The GCC nonnull attribute actually accepts a list of arguments, but
// ABSL_ATTRIBUTE_NONNULL does not.
#if ABSL_HAVE_ATTRIBUTE(nonnull) || (defined(__GNUC__) && !defined(__clang__))
#define ABSL_ATTRIBUTE_NONNULL(arg_index) __attribute__((nonnull(arg_index)))
#else
#define ABSL_ATTRIBUTE_NONNULL(...)
#endif

// ABSL_ATTRIBUTE_NORETURN
//
// Tells the compiler that a given function never returns.
#if ABSL_HAVE_ATTRIBUTE(noreturn) || (defined(__GNUC__) && !defined(__clang__))
#define ABSL_ATTRIBUTE_NORETURN __attribute__((noreturn))
#elif defined(_MSC_VER)
#define ABSL_ATTRIBUTE_NORETURN __declspec(noreturn)
#else
#define ABSL_ATTRIBUTE_NORETURN
#endif

// ABSL_ATTRIBUTE_NO_SANITIZE_ADDRESS
//
// Tells the AddressSanitizer (or other memory testing tools) to ignore a given
// function. Useful for cases when a function reads random locations on stack,
// calls _exit from a cloned subprocess, deliberately accesses buffer
// out of bounds or does other scary things with memory.
// NOTE: GCC supports AddressSanitizer(asan) since 4.8.
// https://gcc.gnu.org/gcc-4.8/changes.html
#if defined(__GNUC__) && defined(ADDRESS_SANITIZER)
#define ABSL_ATTRIBUTE_NO_SANITIZE_ADDRESS __attribute__((no_sanitize_address))
#else
#define ABSL_ATTRIBUTE_NO_SANITIZE_ADDRESS
#endif

// ABSL_ATTRIBUTE_NO_SANITIZE_MEMORY
//
// Tells the  MemorySanitizer to relax the handling of a given function. All
// "Use of uninitialized value" warnings from such functions will be suppressed,
// and all values loaded from memory will be considered fully initialized.
// This attribute is similar to the ADDRESS_SANITIZER attribute above, but deals
// with initialized-ness rather than addressability issues.
// NOTE: MemorySanitizer(msan) is supported by Clang but not GCC.
#if defined(__GNUC__) && defined(MEMORY_SANITIZER)
#define ABSL_ATTRIBUTE_NO_SANITIZE_MEMORY __attribute__((no_sanitize_memory))
#else
#define ABSL_ATTRIBUTE_NO_SANITIZE_MEMORY
#endif

// ABSL_ATTRIBUTE_NO_SANITIZE_THREAD
//
// Tells the ThreadSanitizer to not instrument a given function.
// NOTE: GCC supports ThreadSanitizer(tsan) since 4.8.
// https://gcc.gnu.org/gcc-4.8/changes.html
#if defined(__GNUC__) && defined(THREAD_SANITIZER)
#define ABSL_ATTRIBUTE_NO_SANITIZE_THREAD __attribute__((no_sanitize_thread))
#else
#define ABSL_ATTRIBUTE_NO_SANITIZE_THREAD
#endif

// ABSL_ATTRIBUTE_NO_SANITIZE_UNDEFINED
//
// Tells the UndefinedSanitizer to ignore a given function. Useful for cases
// where certain behavior (eg. devision by zero) is being used intentionally.
// NOTE: GCC supports UndefinedBehaviorSanitizer(ubsan) since 4.9.
// https://gcc.gnu.org/gcc-4.9/changes.html
#if defined(__GNUC__) && \
    (defined(UNDEFINED_BEHAVIOR_SANITIZER) || defined(ADDRESS_SANITIZER))
#define ABSL_ATTRIBUTE_NO_SANITIZE_UNDEFINED \
  __attribute__((no_sanitize("undefined")))
#else
#define ABSL_ATTRIBUTE_NO_SANITIZE_UNDEFINED
#endif

// ABSL_ATTRIBUTE_NO_SANITIZE_CFI
//
// Tells the ControlFlowIntegrity sanitizer to not instrument a given function.
// See https://clang.llvm.org/docs/ControlFlowIntegrity.html for details.
#if defined(__GNUC__) && defined(CONTROL_FLOW_INTEGRITY)
#define ABSL_ATTRIBUTE_NO_SANITIZE_CFI __attribute__((no_sanitize("cfi")))
#else
#define ABSL_ATTRIBUTE_NO_SANITIZE_CFI
#endif

// ABSL_HAVE_ATTRIBUTE_SECTION
//
// Indicates whether labeled sections are supported. Labeled sections are not
// supported on Darwin/iOS.
#ifdef ABSL_HAVE_ATTRIBUTE_SECTION
#error ABSL_HAVE_ATTRIBUTE_SECTION cannot be directly set
#elif (ABSL_HAVE_ATTRIBUTE(section) ||                \
       (defined(__GNUC__) && !defined(__clang__))) && \
    !defined(__APPLE__)
#define ABSL_HAVE_ATTRIBUTE_SECTION 1

// ABSL_ATTRIBUTE_SECTION
//
// Tells the compiler/linker to put a given function into a section and define
// `__start_ ## name` and `__stop_ ## name` symbols to bracket the section.
// This functionality is supported by GNU linker.  Any function annotated with
// `ABSL_ATTRIBUTE_SECTION` must not be inlined, or it will be placed into
// whatever section its caller is placed into.
//
#ifndef ABSL_ATTRIBUTE_SECTION
#define ABSL_ATTRIBUTE_SECTION(name) \
  __attribute__((section(#name))) __attribute__((noinline))
#endif

// ABSL_ATTRIBUTE_SECTION_VARIABLE
//
// Tells the compiler/linker to put a given variable into a section and define
// `__start_ ## name` and `__stop_ ## name` symbols to bracket the section.
// This functionality is supported by GNU linker.
#ifndef ABSL_ATTRIBUTE_SECTION_VARIABLE
#define ABSL_ATTRIBUTE_SECTION_VARIABLE(name) __attribute__((section(#name)))
#endif

// ABSL_DECLARE_ATTRIBUTE_SECTION_VARS
//
// A weak section declaration to be used as a global declaration
// for ABSL_ATTRIBUTE_SECTION_START|STOP(name) to compile and link
// even without functions with ABSL_ATTRIBUTE_SECTION(name).
// ABSL_DEFINE_ATTRIBUTE_SECTION should be in the exactly one file; it's
// a no-op on ELF but not on Mach-O.
//
#ifndef ABSL_DECLARE_ATTRIBUTE_SECTION_VARS
#define ABSL_DECLARE_ATTRIBUTE_SECTION_VARS(name) \
  extern char __start_##name[] ABSL_ATTRIBUTE_WEAK;    \
  extern char __stop_##name[] ABSL_ATTRIBUTE_WEAK
#endif
#ifndef ABSL_DEFINE_ATTRIBUTE_SECTION_VARS
#define ABSL_INIT_ATTRIBUTE_SECTION_VARS(name)
#define ABSL_DEFINE_ATTRIBUTE_SECTION_VARS(name)
#endif

// ABSL_ATTRIBUTE_SECTION_START
//
// Returns `void*` pointers to start/end of a section of code with
// functions having ABSL_ATTRIBUTE_SECTION(name).
// Returns 0 if no such functions exist.
// One must ABSL_DECLARE_ATTRIBUTE_SECTION_VARS(name) for this to compile and
// link.
//
#define ABSL_ATTRIBUTE_SECTION_START(name) \
  (reinterpret_cast<void *>(__start_##name))
#define ABSL_ATTRIBUTE_SECTION_STOP(name) \
  (reinterpret_cast<void *>(__stop_##name))
#else  // !ABSL_HAVE_ATTRIBUTE_SECTION

#define ABSL_HAVE_ATTRIBUTE_SECTION 0

// provide dummy definitions
#define ABSL_ATTRIBUTE_SECTION(name)
#define ABSL_ATTRIBUTE_SECTION_VARIABLE(name)
#define ABSL_INIT_ATTRIBUTE_SECTION_VARS(name)
#define ABSL_DEFINE_ATTRIBUTE_SECTION_VARS(name)
#define ABSL_DECLARE_ATTRIBUTE_SECTION_VARS(name)
#define ABSL_ATTRIBUTE_SECTION_START(name) (reinterpret_cast<void *>(0))
#define ABSL_ATTRIBUTE_SECTION_STOP(name) (reinterpret_cast<void *>(0))
#endif  // ABSL_ATTRIBUTE_SECTION

// ABSL_ATTRIBUTE_STACK_ALIGN_FOR_OLD_LIBC
//
// Support for aligning the stack on 32-bit x86.
#if ABSL_HAVE_ATTRIBUTE(force_align_arg_pointer) || \
    (defined(__GNUC__) && !defined(__clang__))
#if defined(__i386__)
#define ABSL_ATTRIBUTE_STACK_ALIGN_FOR_OLD_LIBC \
  __attribute__((force_align_arg_pointer))
#define ABSL_REQUIRE_STACK_ALIGN_TRAMPOLINE (0)
#elif defined(__x86_64__)
#define ABSL_REQUIRE_STACK_ALIGN_TRAMPOLINE (1)
#define ABSL_ATTRIBUTE_STACK_ALIGN_FOR_OLD_LIBC
#else  // !__i386__ && !__x86_64
#define ABSL_REQUIRE_STACK_ALIGN_TRAMPOLINE (0)
#define ABSL_ATTRIBUTE_STACK_ALIGN_FOR_OLD_LIBC
#endif  // __i386__
#else
#define ABSL_ATTRIBUTE_STACK_ALIGN_FOR_OLD_LIBC
#define ABSL_REQUIRE_STACK_ALIGN_TRAMPOLINE (0)
#endif

// ABSL_MUST_USE_RESULT
//
// Tells the compiler to warn about unused return values for functions declared
// with this macro. The macro must appear as the very first part of a function
// declaration or definition:
//
// Example:
//
//   ABSL_MUST_USE_RESULT Sprocket* AllocateSprocket();
//
// This placement has the broadest compatibility with GCC, Clang, and MSVC, with
// both defs and decls, and with GCC-style attributes, MSVC declspec, C++11
// and C++17 attributes.
//
// ABSL_MUST_USE_RESULT allows using cast-to-void to suppress the unused result
// warning. For that, warn_unused_result is used only for clang but not for gcc.
// https://gcc.gnu.org/bugzilla/show_bug.cgi?id=66425
//
// Note: past advice was to place the macro after the argument list.
#if ABSL_HAVE_ATTRIBUTE(nodiscard)
#define ABSL_MUST_USE_RESULT [[nodiscard]]
#elif defined(__clang__) && ABSL_HAVE_ATTRIBUTE(warn_unused_result)
#define ABSL_MUST_USE_RESULT __attribute__((warn_unused_result))
#else
#define ABSL_MUST_USE_RESULT
#endif

// ABSL_ATTRIBUTE_HOT, ABSL_ATTRIBUTE_COLD
//
// Tells GCC that a function is hot or cold. GCC can use this information to
// improve static analysis, i.e. a conditional branch to a cold function
// is likely to be not-taken.
// This annotation is used for function declarations.
//
// Example:
//
//   int foo() ABSL_ATTRIBUTE_HOT;
#if ABSL_HAVE_ATTRIBUTE(hot) || (defined(__GNUC__) && !defined(__clang__))
#define ABSL_ATTRIBUTE_HOT __attribute__((hot))
#else
#define ABSL_ATTRIBUTE_HOT
#endif

#if ABSL_HAVE_ATTRIBUTE(cold) || (defined(__GNUC__) && !defined(__clang__))
#define ABSL_ATTRIBUTE_COLD __attribute__((cold))
#else
#define ABSL_ATTRIBUTE_COLD
#endif

// ABSL_XRAY_ALWAYS_INSTRUMENT, ABSL_XRAY_NEVER_INSTRUMENT, ABSL_XRAY_LOG_ARGS
//
// We define the ABSL_XRAY_ALWAYS_INSTRUMENT and ABSL_XRAY_NEVER_INSTRUMENT
// macro used as an attribute to mark functions that must always or never be
// instrumented by XRay. Currently, this is only supported in Clang/LLVM.
//
// For reference on the LLVM XRay instrumentation, see
// http://llvm.org/docs/XRay.html.
//
// A function with the XRAY_ALWAYS_INSTRUMENT macro attribute in its declaration
// will always get the XRay instrumentation sleds. These sleds may introduce
// some binary size and runtime overhead and must be used sparingly.
//
// These attributes only take effect when the following conditions are met:
//
//   * The file/target is built in at least C++11 mode, with a Clang compiler
//     that supports XRay attributes.
//   * The file/target is built with the -fxray-instrument flag set for the
//     Clang/LLVM compiler.
//   * The function is defined in the translation unit (the compiler honors the
//     attribute in either the definition or the declaration, and must match).
//
// There are cases when, even when building with XRay instrumentation, users
// might want to control specifically which functions are instrumented for a
// particular build using special-case lists provided to the compiler. These
// special case lists are provided to Clang via the
// -fxray-always-instrument=... and -fxray-never-instrument=... flags. The
// attributes in source take precedence over these special-case lists.
//
// To disable the XRay attributes at build-time, users may define
// ABSL_NO_XRAY_ATTRIBUTES. Do NOT define ABSL_NO_XRAY_ATTRIBUTES on specific
// packages/targets, as this may lead to conflicting definitions of functions at
// link-time.
//
#if ABSL_HAVE_CPP_ATTRIBUTE(clang::xray_always_instrument) && \
    !defined(ABSL_NO_XRAY_ATTRIBUTES)
#define ABSL_XRAY_ALWAYS_INSTRUMENT [[clang::xray_always_instrument]]
#define ABSL_XRAY_NEVER_INSTRUMENT [[clang::xray_never_instrument]]
#if ABSL_HAVE_CPP_ATTRIBUTE(clang::xray_log_args)
#define ABSL_XRAY_LOG_ARGS(N) \
    [[clang::xray_always_instrument, clang::xray_log_args(N)]]
#else
#define ABSL_XRAY_LOG_ARGS(N) [[clang::xray_always_instrument]]
#endif
#else
#define ABSL_XRAY_ALWAYS_INSTRUMENT
#define ABSL_XRAY_NEVER_INSTRUMENT
#define ABSL_XRAY_LOG_ARGS(N)
#endif

// -----------------------------------------------------------------------------
// Variable Attributes
// -----------------------------------------------------------------------------

// ABSL_ATTRIBUTE_UNUSED
//
// Prevents the compiler from complaining about or optimizing away variables
// that appear unused.
#if ABSL_HAVE_ATTRIBUTE(unused) || (defined(__GNUC__) && !defined(__clang__))
#undef ABSL_ATTRIBUTE_UNUSED
#define ABSL_ATTRIBUTE_UNUSED __attribute__((__unused__))
#else
#define ABSL_ATTRIBUTE_UNUSED
#endif

// ABSL_ATTRIBUTE_INITIAL_EXEC
//
// Tells the compiler to use "initial-exec" mode for a thread-local variable.
// See http://people.redhat.com/drepper/tls.pdf for the gory details.
#if ABSL_HAVE_ATTRIBUTE(tls_model) || (defined(__GNUC__) && !defined(__clang__))
#define ABSL_ATTRIBUTE_INITIAL_EXEC __attribute__((tls_model("initial-exec")))
#else
#define ABSL_ATTRIBUTE_INITIAL_EXEC
#endif

// ABSL_ATTRIBUTE_PACKED
//
// Prevents the compiler from padding a structure to natural alignment
#if ABSL_HAVE_ATTRIBUTE(packed) || (defined(__GNUC__) && !defined(__clang__))
#define ABSL_ATTRIBUTE_PACKED __attribute__((__packed__))
#else
#define ABSL_ATTRIBUTE_PACKED
#endif

// ABSL_CONST_INIT
//
// A variable declaration annotated with the `ABSL_CONST_INIT` attribute will
// not compile (on supported platforms) unless the variable has a constant
// initializer. This is useful for variables with static and thread storage
// duration, because it guarantees that they will not suffer from the so-called
// "static init order fiasco".
//
// Example:
//
//   ABSL_CONST_INIT static MyType my_var = MakeMyType(...);
//
// Note that this attribute is redundant if the variable is declared constexpr.
#if ABSL_HAVE_CPP_ATTRIBUTE(clang::require_constant_initialization)
// NOLINTNEXTLINE(whitespace/braces)
#define ABSL_CONST_INIT [[clang::require_constant_initialization]]
#else
#define ABSL_CONST_INIT
#endif  // ABSL_HAVE_CPP_ATTRIBUTE(clang::require_constant_initialization)

#endif  // ABSL_BASE_ATTRIBUTES_H_
//
// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// -----------------------------------------------------------------------------
// File: config.h
// -----------------------------------------------------------------------------
//
// This header file defines a set of macros for checking the presence of
// important compiler and platform features. Such macros can be used to
// produce portable code by parameterizing compilation based on the presence or
// lack of a given feature.
//
// We define a "feature" as some interface we wish to program to: for example,
// a library function or system call. A value of `1` indicates support for
// that feature; any other value indicates the feature support is undefined.
//
// Example:
//
// Suppose a programmer wants to write a program that uses the 'mmap()' system
// call. The Abseil macro for that feature (`ABSL_HAVE_MMAP`) allows you to
// selectively include the `mmap.h` header and bracket code using that feature
// in the macro:
//
//
//   #ifdef ABSL_HAVE_MMAP
//   #include "sys/mman.h"
//   #endif  //ABSL_HAVE_MMAP
//
//   ...
//   #ifdef ABSL_HAVE_MMAP
//   void *ptr = mmap(...);
//   ...
//   #endif  // ABSL_HAVE_MMAP

#ifndef ABSL_BASE_CONFIG_H_
#define ABSL_BASE_CONFIG_H_

// Included for the __GLIBC__ macro (or similar macros on other systems).
#include <limits.h>

#ifdef __cplusplus
// Included for __GLIBCXX__, _LIBCPP_VERSION
#include <cstddef>
#endif  // __cplusplus

#if defined(__APPLE__)
// Included for TARGET_OS_IPHONE, __IPHONE_OS_VERSION_MIN_REQUIRED,
// __IPHONE_8_0.
#include <Availability.h>
#include <TargetConditionals.h>
#endif


// -----------------------------------------------------------------------------
// Compiler Feature Checks
// -----------------------------------------------------------------------------

// ABSL_HAVE_BUILTIN()
//
// Checks whether the compiler supports a Clang Feature Checking Macro, and if
// so, checks whether it supports the provided builtin function "x" where x
// is one of the functions noted in
// https://clang.llvm.org/docs/LanguageExtensions.html
//
// Note: Use this macro to avoid an extra level of #ifdef __has_builtin check.
// http://releases.llvm.org/3.3/tools/clang/docs/LanguageExtensions.html
#ifdef __has_builtin
#define ABSL_HAVE_BUILTIN(x) __has_builtin(x)
#else
#define ABSL_HAVE_BUILTIN(x) 0
#endif

// ABSL_HAVE_TLS is defined to 1 when __thread should be supported.
// We assume __thread is supported on Linux when compiled with Clang or compiled
// against libstdc++ with _GLIBCXX_HAVE_TLS defined.
#ifdef ABSL_HAVE_TLS
#error ABSL_HAVE_TLS cannot be directly set
#elif defined(__linux__) && (defined(__clang__) || defined(_GLIBCXX_HAVE_TLS))
#define ABSL_HAVE_TLS 1
#endif

// There are platforms for which TLS should not be used even though the compiler
// makes it seem like it's supported (Android NDK < r12b for example).
// This is primarily because of linker problems and toolchain misconfiguration:
// Abseil does not intend to support this indefinitely. Currently, the newest
// toolchain that we intend to support that requires this behavior is the
// r11 NDK - allowing for a 5 year support window on that means this option
// is likely to be removed around June of 2021.
#if defined(__ANDROID__) && defined(__clang__)
#if __has_include(<android/ndk-version.h>)
#include <android/ndk-version.h>
#endif
// TLS isn't supported until NDK r12b per
// https://developer.android.com/ndk/downloads/revision_history.html
// Since NDK r16, `__NDK_MAJOR__` and `__NDK_MINOR__` are defined in
// <android/ndk-version.h>. For NDK < r16, users should define these macros,
// e.g. `-D__NDK_MAJOR__=11 -D__NKD_MINOR__=0` for NDK r11.
#if defined(__NDK_MAJOR__) && defined(__NDK_MINOR__) && \
    ((__NDK_MAJOR__ < 12) || ((__NDK_MAJOR__ == 12) && (__NDK_MINOR__ < 1)))
#undef ABSL_HAVE_TLS
#endif
#endif  // defined(__ANDROID__) && defined(__clang__)

// ABSL_HAVE_STD_IS_TRIVIALLY_DESTRUCTIBLE
//
// Checks whether `std::is_trivially_destructible<T>` is supported.
//
// Notes: All supported compilers using libc++ support this feature, as does
// gcc >= 4.8.1 using libstdc++, and Visual Studio.
#ifdef ABSL_HAVE_STD_IS_TRIVIALLY_DESTRUCTIBLE
#error ABSL_HAVE_STD_IS_TRIVIALLY_DESTRUCTIBLE cannot be directly set
#elif defined(_LIBCPP_VERSION) ||                                        \
    (!defined(__clang__) && defined(__GNUC__) && defined(__GLIBCXX__) && \
     (__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 8))) ||        \
    defined(_MSC_VER)
#define ABSL_HAVE_STD_IS_TRIVIALLY_DESTRUCTIBLE 1
#endif

// ABSL_HAVE_STD_IS_TRIVIALLY_CONSTRUCTIBLE
//
// Checks whether `std::is_trivially_default_constructible<T>` and
// `std::is_trivially_copy_constructible<T>` are supported.

// ABSL_HAVE_STD_IS_TRIVIALLY_ASSIGNABLE
//
// Checks whether `std::is_trivially_copy_assignable<T>` is supported.

// Notes: Clang with libc++ supports these features, as does gcc >= 5.1 with
// either libc++ or libstdc++, and Visual Studio.
#if defined(ABSL_HAVE_STD_IS_TRIVIALLY_CONSTRUCTIBLE)
#error ABSL_HAVE_STD_IS_TRIVIALLY_CONSTRUCTIBLE cannot be directly set
#elif defined(ABSL_HAVE_STD_IS_TRIVIALLY_ASSIGNABLE)
#error ABSL_HAVE_STD_IS_TRIVIALLY_ASSIGNABLE cannot directly set
#elif (defined(__clang__) && defined(_LIBCPP_VERSION)) ||        \
    (!defined(__clang__) && defined(__GNUC__) &&                 \
     (__GNUC__ > 5 || (__GNUC__ == 5 && __GNUC_MINOR__ >= 1)) && \
     (defined(_LIBCPP_VERSION) || defined(__GLIBCXX__))) ||      \
    defined(_MSC_VER)
#define ABSL_HAVE_STD_IS_TRIVIALLY_CONSTRUCTIBLE 1
#define ABSL_HAVE_STD_IS_TRIVIALLY_ASSIGNABLE 1
#endif

// ABSL_HAVE_THREAD_LOCAL
//
// Checks whether C++11's `thread_local` storage duration specifier is
// supported.
#ifdef ABSL_HAVE_THREAD_LOCAL
#error ABSL_HAVE_THREAD_LOCAL cannot be directly set
#elif !defined(__apple_build_version__) ||   \
    ((__apple_build_version__ >= 8000042) && \
     !(TARGET_OS_IPHONE && __IPHONE_OS_VERSION_MIN_REQUIRED < __IPHONE_9_0))
// Notes: Xcode's clang did not support `thread_local` until version
// 8, and even then not for all iOS < 9.0.
#define ABSL_HAVE_THREAD_LOCAL 1
#endif

// ABSL_HAVE_INTRINSIC_INT128
//
// Checks whether the __int128 compiler extension for a 128-bit integral type is
// supported.
//
// Notes: __SIZEOF_INT128__ is defined by Clang and GCC when __int128 is
// supported, except on ppc64 and aarch64 where __int128 exists but has exhibits
// a sporadic compiler crashing bug. Nvidia's nvcc also defines __GNUC__ and
// __SIZEOF_INT128__ but not all versions actually support __int128.
#ifdef ABSL_HAVE_INTRINSIC_INT128
#error ABSL_HAVE_INTRINSIC_INT128 cannot be directly set
#elif (defined(__clang__) && defined(__SIZEOF_INT128__) &&               \
       !defined(__ppc64__) && !defined(__aarch64__)) ||                  \
    (defined(__CUDACC__) && defined(__SIZEOF_INT128__) &&                \
     __CUDACC_VER__ >= 70000) ||                                         \
    (!defined(__clang__) && !defined(__CUDACC__) && defined(__GNUC__) && \
     defined(__SIZEOF_INT128__))
#define ABSL_HAVE_INTRINSIC_INT128 1
#endif

// ABSL_HAVE_EXCEPTIONS
//
// Checks whether the compiler both supports and enables exceptions. Many
// compilers support a "no exceptions" mode that disables exceptions.
//
// Generally, when ABSL_HAVE_EXCEPTIONS is not defined:
//
// * Code using `throw` and `try` may not compile.
// * The `noexcept` specifier will still compile and behave as normal.
// * The `noexcept` operator may still return `false`.
//
// For further details, consult the compiler's documentation.
#ifdef ABSL_HAVE_EXCEPTIONS
#error ABSL_HAVE_EXCEPTIONS cannot be directly set.

#elif defined(__clang__)
// TODO(calabrese)
// Switch to using __cpp_exceptions when we no longer support versions < 3.6.
// For details on this check, see:
//   http://releases.llvm.org/3.6.0/tools/clang/docs/ReleaseNotes.html#the-exceptions-macro
#if defined(__EXCEPTIONS) && __has_feature(cxx_exceptions)
#define ABSL_HAVE_EXCEPTIONS 1
#endif  // defined(__EXCEPTIONS) && __has_feature(cxx_exceptions)

// Handle remaining special cases and default to exceptions being supported.
#elif !(defined(__GNUC__) && (__GNUC__ < 5) && !defined(__EXCEPTIONS)) &&    \
    !(defined(__GNUC__) && (__GNUC__ >= 5) && !defined(__cpp_exceptions)) && \
    !(defined(_MSC_VER) && !defined(_CPPUNWIND))
#define ABSL_HAVE_EXCEPTIONS 1
#endif

// -----------------------------------------------------------------------------
// Platform Feature Checks
// -----------------------------------------------------------------------------

// Currently supported operating systems and associated preprocessor
// symbols:
//
//   Linux and Linux-derived           __linux__
//   Android                           __ANDROID__ (implies __linux__)
//   Linux (non-Android)               __linux__ && !__ANDROID__
//   Darwin (Mac OS X and iOS)         __APPLE__
//   Akaros (http://akaros.org)        __ros__
//   Windows                           _WIN32
//   NaCL                              __native_client__
//   AsmJS                             __asmjs__
//   Fuschia                           __Fuchsia__
//
// Note that since Android defines both __ANDROID__ and __linux__, one
// may probe for either Linux or Android by simply testing for __linux__.

// ABSL_HAVE_MMAP
//
// Checks whether the platform has an mmap(2) implementation as defined in
// POSIX.1-2001.
#ifdef ABSL_HAVE_MMAP
#error ABSL_HAVE_MMAP cannot be directly set
#elif defined(__linux__) || defined(__APPLE__) || defined(__ros__) || \
    defined(__native_client__) || defined(__asmjs__) || defined(__Fuchsia__)
#define ABSL_HAVE_MMAP 1
#endif

// ABSL_HAVE_PTHREAD_GETSCHEDPARAM
//
// Checks whether the platform implements the pthread_(get|set)schedparam(3)
// functions as defined in POSIX.1-2001.
#ifdef ABSL_HAVE_PTHREAD_GETSCHEDPARAM
#error ABSL_HAVE_PTHREAD_GETSCHEDPARAM cannot be directly set
#elif defined(__linux__) || defined(__APPLE__) || defined(__ros__)
#define ABSL_HAVE_PTHREAD_GETSCHEDPARAM 1
#endif

// ABSL_HAVE_SCHED_YIELD
//
// Checks whether the platform implements sched_yield(2) as defined in
// POSIX.1-2001.
#ifdef ABSL_HAVE_SCHED_YIELD
#error ABSL_HAVE_SCHED_YIELD cannot be directly set
#elif defined(__linux__) || defined(__ros__) || defined(__native_client__)
#define ABSL_HAVE_SCHED_YIELD 1
#endif

// ABSL_HAVE_SEMAPHORE_H
//
// Checks whether the platform supports the <semaphore.h> header and sem_open(3)
// family of functions as standardized in POSIX.1-2001.
//
// Note: While Apple provides <semaphore.h> for both iOS and macOS, it is
// explicitly deprecated and will cause build failures if enabled for those
// platforms.  We side-step the issue by not defining it here for Apple
// platforms.
#ifdef ABSL_HAVE_SEMAPHORE_H
#error ABSL_HAVE_SEMAPHORE_H cannot be directly set
#elif defined(__linux__) || defined(__ros__)
#define ABSL_HAVE_SEMAPHORE_H 1
#endif

// ABSL_HAVE_ALARM
//
// Checks whether the platform supports the <signal.h> header and alarm(2)
// function as standardized in POSIX.1-2001.
#ifdef ABSL_HAVE_ALARM
#error ABSL_HAVE_ALARM cannot be directly set
#elif defined(__GOOGLE_GRTE_VERSION__)
// feature tests for Google's GRTE
#define ABSL_HAVE_ALARM 1
#elif defined(__GLIBC__)
// feature test for glibc
#define ABSL_HAVE_ALARM 1
#elif defined(_MSC_VER)
// feature tests for Microsoft's library
#elif defined(__native_client__)
#else
// other standard libraries
#define ABSL_HAVE_ALARM 1
#endif

// ABSL_IS_LITTLE_ENDIAN
// ABSL_IS_BIG_ENDIAN
//
// Checks the endianness of the platform.
//
// Notes: uses the built in endian macros provided by GCC (since 4.6) and
// Clang (since 3.2); see
// https://gcc.gnu.org/onlinedocs/cpp/Common-Predefined-Macros.html.
// Otherwise, if _WIN32, assume little endian. Otherwise, bail with an error.
#if defined(ABSL_IS_BIG_ENDIAN)
#error "ABSL_IS_BIG_ENDIAN cannot be directly set."
#endif
#if defined(ABSL_IS_LITTLE_ENDIAN)
#error "ABSL_IS_LITTLE_ENDIAN cannot be directly set."
#endif

#if (defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__) && \
     __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
#define ABSL_IS_LITTLE_ENDIAN 1
#elif defined(__BYTE_ORDER__) && defined(__ORDER_BIG_ENDIAN__) && \
    __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#define ABSL_IS_BIG_ENDIAN 1
#elif defined(_WIN32)
#define ABSL_IS_LITTLE_ENDIAN 1
#else
#error "absl endian detection needs to be set up for your compiler"
#endif

// ABSL_HAVE_STD_ANY
//
// Checks whether C++17 std::any is available by checking whether <any> exists.
#ifdef ABSL_HAVE_STD_ANY
#error "ABSL_HAVE_STD_ANY cannot be directly set."
#endif

#ifdef __has_include
#if __has_include(<any>) && __cplusplus >= 201703L
#define ABSL_HAVE_STD_ANY 1
#endif
#endif

// ABSL_HAVE_STD_OPTIONAL
//
// Checks whether C++17 std::optional is available.
#ifdef ABSL_HAVE_STD_OPTIONAL
#error "ABSL_HAVE_STD_OPTIONAL cannot be directly set."
#endif

#ifdef __has_include
#if __has_include(<optional>) && __cplusplus >= 201703L
#define ABSL_HAVE_STD_OPTIONAL 1
#endif
#endif

// ABSL_HAVE_STD_STRING_VIEW
//
// Checks whether C++17 std::string_view is available.
#ifdef ABSL_HAVE_STD_STRING_VIEW
#error "ABSL_HAVE_STD_STRING_VIEW cannot be directly set."
#endif

#ifdef __has_include
#if __has_include(<string_view>) && __cplusplus >= 201703L
#define ABSL_HAVE_STD_STRING_VIEW 1
#endif
#endif

#endif  // ABSL_BASE_CONFIG_H_
//
// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// -----------------------------------------------------------------------------
// File: optimization.h
// -----------------------------------------------------------------------------
//
// This header file defines portable macros for performance optimization.

#ifndef ABSL_BASE_OPTIMIZATION_H_
#define ABSL_BASE_OPTIMIZATION_H_


// ABSL_BLOCK_TAIL_CALL_OPTIMIZATION
//
// Instructs the compiler to avoid optimizing tail-call recursion. Use of this
// macro is useful when you wish to preserve the existing function order within
// a stack trace for logging, debugging, or profiling purposes.
//
// Example:
//
//   int f() {
//     int result = g();
//     ABSL_BLOCK_TAIL_CALL_OPTIMIZATION();
//     return result;
//   }
#if defined(__pnacl__)
#define ABSL_BLOCK_TAIL_CALL_OPTIMIZATION() if (volatile int x = 0) { (void)x; }
#elif defined(__clang__)
// Clang will not tail call given inline volatile assembly.
#define ABSL_BLOCK_TAIL_CALL_OPTIMIZATION() __asm__ __volatile__("")
#elif defined(__GNUC__)
// GCC will not tail call given inline volatile assembly.
#define ABSL_BLOCK_TAIL_CALL_OPTIMIZATION() __asm__ __volatile__("")
#elif defined(_MSC_VER)
#include <intrin.h>
// The __nop() intrinsic blocks the optimisation.
#define ABSL_BLOCK_TAIL_CALL_OPTIMIZATION() __nop()
#else
#define ABSL_BLOCK_TAIL_CALL_OPTIMIZATION() if (volatile int x = 0) { (void)x; }
#endif

// ABSL_CACHELINE_SIZE
//
// Explicitly defines the size of the L1 cache for purposes of alignment.
// Setting the cacheline size allows you to specify that certain objects be
// aligned on a cacheline boundary with `ABSL_CACHELINE_ALIGNED` declarations.
// (See below.)
//
// NOTE: this macro should be replaced with the following C++17 features, when
// those are generally available:
//
//   * `std::hardware_constructive_interference_size`
//   * `std::hardware_destructive_interference_size`
//
// See http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2016/p0154r1.html
// for more information.
#if defined(__GNUC__)
// Cache line alignment
#if defined(__i386__) || defined(__x86_64__)
#define ABSL_CACHELINE_SIZE 64
#elif defined(__powerpc64__)
#define ABSL_CACHELINE_SIZE 128
#elif defined(__aarch64__)
// We would need to read special register ctr_el0 to find out L1 dcache size.
// This value is a good estimate based on a real aarch64 machine.
#define ABSL_CACHELINE_SIZE 64
#elif defined(__arm__)
// Cache line sizes for ARM: These values are not strictly correct since
// cache line sizes depend on implementations, not architectures.  There
// are even implementations with cache line sizes configurable at boot
// time.
#if defined(__ARM_ARCH_5T__)
#define ABSL_CACHELINE_SIZE 32
#elif defined(__ARM_ARCH_7A__)
#define ABSL_CACHELINE_SIZE 64
#endif
#endif

#ifndef ABSL_CACHELINE_SIZE
// A reasonable default guess.  Note that overestimates tend to waste more
// space, while underestimates tend to waste more time.
#define ABSL_CACHELINE_SIZE 64
#endif

// ABSL_CACHELINE_ALIGNED
//
// Indicates that the declared object be cache aligned using
// `ABSL_CACHELINE_SIZE` (see above). Cacheline aligning objects allows you to
// load a set of related objects in the L1 cache for performance improvements.
// Cacheline aligning objects properly allows constructive memory sharing and
// prevents destructive (or "false") memory sharing.
//
// NOTE: this macro should be replaced with usage of `alignas()` using
// `std::hardware_constructive_interference_size` and/or
// `std::hardware_destructive_interference_size` when available within C++17.
//
// See http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2016/p0154r1.html
// for more information.
//
// On some compilers, `ABSL_CACHELINE_ALIGNED` expands to
// `__attribute__((aligned(ABSL_CACHELINE_SIZE)))`. For compilers where this is
// not known to work, the macro expands to nothing.
//
// No further guarantees are made here. The result of applying the macro
// to variables and types is always implementation-defined.
//
// WARNING: It is easy to use this attribute incorrectly, even to the point
// of causing bugs that are difficult to diagnose, crash, etc. It does not
// of itself guarantee that objects are aligned to a cache line.
//
// Recommendations:
//
// 1) Consult compiler documentation; this comment is not kept in sync as
//    toolchains evolve.
// 2) Verify your use has the intended effect. This often requires inspecting
//    the generated machine code.
// 3) Prefer applying this attribute to individual variables. Avoid
//    applying it to types. This tends to localize the effect.
#define ABSL_CACHELINE_ALIGNED __attribute__((aligned(ABSL_CACHELINE_SIZE)))

#else  // not GCC
#define ABSL_CACHELINE_SIZE 64
#define ABSL_CACHELINE_ALIGNED
#endif

// ABSL_PREDICT_TRUE, ABSL_PREDICT_FALSE
//
// Enables the compiler to prioritize compilation using static analysis for
// likely paths within a boolean branch.
//
// Example:
//
//   if (ABSL_PREDICT_TRUE(expression)) {
//     return result;                        // Faster if more likely
//   } else {
//     return 0;
//   }
//
// Compilers can use the information that a certain branch is not likely to be
// taken (for instance, a CHECK failure) to optimize for the common case in
// the absence of better information (ie. compiling gcc with `-fprofile-arcs`).
#if ABSL_HAVE_BUILTIN(__builtin_expect) || \
    (defined(__GNUC__) && !defined(__clang__))
#define ABSL_PREDICT_FALSE(x) (__builtin_expect(x, 0))
#define ABSL_PREDICT_TRUE(x) (__builtin_expect(!!(x), 1))
#else
#define ABSL_PREDICT_FALSE(x) x
#define ABSL_PREDICT_TRUE(x) x
#endif

#endif  // ABSL_BASE_OPTIMIZATION_H_
//
// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// -----------------------------------------------------------------------------
// File: macros.h
// -----------------------------------------------------------------------------
//
// This header file defines the set of language macros used within Abseil code.
// For the set of macros used to determine supported compilers and platforms,
// see absl/base/config.h instead.
//
// This code is compiled directly on many platforms, including client
// platforms like Windows, Mac, and embedded systems.  Before making
// any changes here, make sure that you're not breaking any platforms.
//

#ifndef ABSL_BASE_MACROS_H_
#define ABSL_BASE_MACROS_H_

#include <cstddef>


// ABSL_ARRAYSIZE()
//
// Returns the # of elements in an array as a compile-time constant, which can
// be used in defining new arrays. If you use this macro on a pointer by
// mistake, you will get a compile-time error.
//
// Note: this template function declaration is used in defining arraysize.
// Note that the function doesn't need an implementation, as we only
// use its type.
namespace absl {
namespace macros_internal {
template <typename T, size_t N>
char (&ArraySizeHelper(T (&array)[N]))[N];
}  // namespace macros_internal
}  // namespace absl
#define ABSL_ARRAYSIZE(array) \
  (sizeof(::absl::macros_internal::ArraySizeHelper(array)))

// kLinkerInitialized
//
// An enum used only as a constructor argument to indicate that a variable has
// static storage duration, and that the constructor should do nothing to its
// state. Use of this macro indicates to the reader that it is legal to
// declare a static instance of the class, provided the constructor is given
// the absl::base_internal::kLinkerInitialized argument.
//
// Normally, it is unsafe to declare a static variable that has a constructor or
// a destructor because invocation order is undefined. However, if the type can
// be zero-initialized (which the loader does for static variables) into a valid
// state and the type's destructor does not affect storage, then a constructor
// for static initialization can be declared.
//
// Example:
//       // Declaration
//       explicit MyClass(absl::base_internal:LinkerInitialized x) {}
//
//       // Invocation
//       static MyClass my_global(absl::base_internal::kLinkerInitialized);
namespace absl {
namespace base_internal {
enum LinkerInitialized {
  kLinkerInitialized = 0,
};
}  // namespace base_internal
}  // namespace absl

// ABSL_FALLTHROUGH_INTENDED
//
// Annotates implicit fall-through between switch labels, allowing a case to
// indicate intentional fallthrough and turn off warnings about any lack of a
// `break` statement. The ABSL_FALLTHROUGH_INTENDED macro should be followed by
// a semicolon and can be used in most places where `break` can, provided that
// no statements exist between it and the next switch label.
//
// Example:
//
//  switch (x) {
//    case 40:
//    case 41:
//      if (truth_is_out_there) {
//        ++x;
//        ABSL_FALLTHROUGH_INTENDED;  // Use instead of/along with annotations
//                                    // in comments
//      } else {
//        return x;
//      }
//    case 42:
//      ...
//
// Notes: when compiled with clang in C++11 mode, the ABSL_FALLTHROUGH_INTENDED
// macro is expanded to the [[clang::fallthrough]] attribute, which is analysed
// when  performing switch labels fall-through diagnostic
// (`-Wimplicit-fallthrough`). See clang documentation on language extensions
// for details:
// http://clang.llvm.org/docs/AttributeReference.html#fallthrough-clang-fallthrough
//
// When used with unsupported compilers, the ABSL_FALLTHROUGH_INTENDED macro
// has no effect on diagnostics. In any case this macro has no effect on runtime
// behavior and performance of code.
#ifdef ABSL_FALLTHROUGH_INTENDED
#error "ABSL_FALLTHROUGH_INTENDED should not be defined."
#endif

// TODO(zhangxy): Use c++17 standard [[fallthrough]] macro, when supported.
#if defined(__clang__) && defined(__has_warning)
#if __has_feature(cxx_attributes) && __has_warning("-Wimplicit-fallthrough")
#define ABSL_FALLTHROUGH_INTENDED [[clang::fallthrough]]
#endif
#elif defined(__GNUC__) && __GNUC__ >= 7
#define ABSL_FALLTHROUGH_INTENDED [[gnu::fallthrough]]
#endif

#ifndef ABSL_FALLTHROUGH_INTENDED
#define ABSL_FALLTHROUGH_INTENDED \
  do {                            \
  } while (0)
#endif

// ABSL_DEPRECATED()
//
// Marks a deprecated class, struct, enum, function, method and variable
// declarations. The macro argument is used as a custom diagnostic message (e.g.
// suggestion of a better alternative).
//
// Example:
//
//   class ABSL_DEPRECATED("Use Bar instead") Foo {...};
//   ABSL_DEPRECATED("Use Baz instead") void Bar() {...}
//
// Every usage of a deprecated entity will trigger a warning when compiled with
// clang's `-Wdeprecated-declarations` option. This option is turned off by
// default, but the warnings will be reported by clang-tidy.
#if defined(__clang__) && __cplusplus >= 201103L && defined(__has_warning)
#define ABSL_DEPRECATED(message) __attribute__((deprecated(message)))
#endif

#ifndef ABSL_DEPRECATED
#define ABSL_DEPRECATED(message)
#endif

// ABSL_BAD_CALL_IF()
//
// Used on a function overload to trap bad calls: any call that matches the
// overload will cause a compile-time error. This macro uses a clang-specific
// "enable_if" attribute, as described at
// http://clang.llvm.org/docs/AttributeReference.html#enable-if
//
// Overloads which use this macro should be bracketed by
// `#ifdef ABSL_BAD_CALL_IF`.
//
// Example:
//
//   int isdigit(int c);
//   #ifdef ABSL_BAD_CALL_IF
//   int isdigit(int c)
//     ABSL_BAD_CALL_IF(c <= -1 || c > 255,
//                       "'c' must have the value of an unsigned char or EOF");
//   #endif // ABSL_BAD_CALL_IF

#if defined(__clang__)
# if __has_attribute(enable_if)
#  define ABSL_BAD_CALL_IF(expr, msg) \
    __attribute__((enable_if(expr, "Bad call trap"), unavailable(msg)))
# endif
#endif

// ABSL_ASSERT()
//
// In C++11, `assert` can't be used portably within constexpr functions.
// ABSL_ASSERT functions as a runtime assert but works in C++11 constexpr
// functions.  Example:
//
// constexpr double Divide(double a, double b) {
//   return ABSL_ASSERT(b != 0), a / b;
// }
//
// This macro is inspired by
// https://akrzemi1.wordpress.com/2017/05/18/asserts-in-constexpr-functions/
#if defined(NDEBUG)
#define ABSL_ASSERT(expr) (false ? (void)(expr) : (void)0)
#else
#define ABSL_ASSERT(expr) \
  (ABSL_PREDICT_TRUE((expr)) ? (void)0 : [] { assert(false && #expr); }())
#endif

#endif  // ABSL_BASE_MACROS_H_
/*
 *  Copyright 2017 The Abseil Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/* This file defines dynamic annotations for use with dynamic analysis
   tool such as valgrind, PIN, etc.

   Dynamic annotation is a source code annotation that affects
   the generated code (that is, the annotation is not a comment).
   Each such annotation is attached to a particular
   instruction and/or to a particular object (address) in the program.

   The annotations that should be used by users are macros in all upper-case
   (e.g., ANNOTATE_THREAD_NAME).

   Actual implementation of these macros may differ depending on the
   dynamic analysis tool being used.

   This file supports the following configurations:
   - Dynamic Annotations enabled (with static thread-safety warnings disabled).
     In this case, macros expand to functions implemented by Thread Sanitizer,
     when building with TSan. When not provided an external implementation,
     dynamic_annotations.cc provides no-op implementations.

   - Static Clang thread-safety warnings enabled.
     When building with a Clang compiler that supports thread-safety warnings,
     a subset of annotations can be statically-checked at compile-time. We
     expand these macros to static-inline functions that can be analyzed for
     thread-safety, but afterwards elided when building the final binary.

   - All annotations are disabled.
     If neither Dynamic Annotations nor Clang thread-safety warnings are
     enabled, then all annotation-macros expand to empty. */

#ifndef ABSL_BASE_DYNAMIC_ANNOTATIONS_H_
#define ABSL_BASE_DYNAMIC_ANNOTATIONS_H_

#ifndef DYNAMIC_ANNOTATIONS_ENABLED
# define DYNAMIC_ANNOTATIONS_ENABLED 0
#endif

#if defined(__native_client__)
  #include "nacl/dynamic_annotations.h"

  // Stub out the macros missing from the NaCl version.
  #ifndef ANNOTATE_CONTIGUOUS_CONTAINER
    #define ANNOTATE_CONTIGUOUS_CONTAINER(beg, end, old_mid, new_mid)
  #endif
  #ifndef ANNOTATE_RWLOCK_CREATE_STATIC
    #define ANNOTATE_RWLOCK_CREATE_STATIC(lock)
  #endif
  #ifndef ADDRESS_SANITIZER_REDZONE
    #define ADDRESS_SANITIZER_REDZONE(name)
  #endif
  #ifndef ANNOTATE_MEMORY_IS_UNINITIALIZED
    #define ANNOTATE_MEMORY_IS_UNINITIALIZED(address, size)
  #endif

#else /* !__native_client__ */

#if DYNAMIC_ANNOTATIONS_ENABLED != 0

  /* -------------------------------------------------------------
     Annotations that suppress errors.  It is usually better to express the
     program's synchronization using the other annotations, but these can
     be used when all else fails. */

  /* Report that we may have a benign race at "pointer", with size
     "sizeof(*(pointer))". "pointer" must be a non-void* pointer.  Insert at the
     point where "pointer" has been allocated, preferably close to the point
     where the race happens.  See also ANNOTATE_BENIGN_RACE_STATIC. */
  #define ANNOTATE_BENIGN_RACE(pointer, description) \
    AnnotateBenignRaceSized(__FILE__, __LINE__, pointer, \
                            sizeof(*(pointer)), description)

  /* Same as ANNOTATE_BENIGN_RACE(address, description), but applies to
     the memory range [address, address+size). */
  #define ANNOTATE_BENIGN_RACE_SIZED(address, size, description) \
    AnnotateBenignRaceSized(__FILE__, __LINE__, address, size, description)

  /* Enable (enable!=0) or disable (enable==0) race detection for all threads.
     This annotation could be useful if you want to skip expensive race analysis
     during some period of program execution, e.g. during initialization. */
  #define ANNOTATE_ENABLE_RACE_DETECTION(enable) \
    AnnotateEnableRaceDetection(__FILE__, __LINE__, enable)

  /* -------------------------------------------------------------
     Annotations useful for debugging. */

  /* Report the current thread name to a race detector. */
  #define ANNOTATE_THREAD_NAME(name) \
    AnnotateThreadName(__FILE__, __LINE__, name)

  /* -------------------------------------------------------------
     Annotations useful when implementing locks.  They are not
     normally needed by modules that merely use locks.
     The "lock" argument is a pointer to the lock object. */

  /* Report that a lock has been created at address "lock". */
  #define ANNOTATE_RWLOCK_CREATE(lock) \
    AnnotateRWLockCreate(__FILE__, __LINE__, lock)

  /* Report that a linker initialized lock has been created at address "lock".
   */
#ifdef THREAD_SANITIZER
  #define ANNOTATE_RWLOCK_CREATE_STATIC(lock) \
    AnnotateRWLockCreateStatic(__FILE__, __LINE__, lock)
#else
  #define ANNOTATE_RWLOCK_CREATE_STATIC(lock) ANNOTATE_RWLOCK_CREATE(lock)
#endif

  /* Report that the lock at address "lock" is about to be destroyed. */
  #define ANNOTATE_RWLOCK_DESTROY(lock) \
    AnnotateRWLockDestroy(__FILE__, __LINE__, lock)

  /* Report that the lock at address "lock" has been acquired.
     is_w=1 for writer lock, is_w=0 for reader lock. */
  #define ANNOTATE_RWLOCK_ACQUIRED(lock, is_w) \
    AnnotateRWLockAcquired(__FILE__, __LINE__, lock, is_w)

  /* Report that the lock at address "lock" is about to be released. */
  #define ANNOTATE_RWLOCK_RELEASED(lock, is_w) \
    AnnotateRWLockReleased(__FILE__, __LINE__, lock, is_w)

#else  /* DYNAMIC_ANNOTATIONS_ENABLED == 0 */

  #define ANNOTATE_RWLOCK_CREATE(lock) /* empty */
  #define ANNOTATE_RWLOCK_CREATE_STATIC(lock) /* empty */
  #define ANNOTATE_RWLOCK_DESTROY(lock) /* empty */
  #define ANNOTATE_RWLOCK_ACQUIRED(lock, is_w) /* empty */
  #define ANNOTATE_RWLOCK_RELEASED(lock, is_w) /* empty */
  #define ANNOTATE_BENIGN_RACE(address, description) /* empty */
  #define ANNOTATE_BENIGN_RACE_SIZED(address, size, description) /* empty */
  #define ANNOTATE_THREAD_NAME(name) /* empty */
  #define ANNOTATE_ENABLE_RACE_DETECTION(enable) /* empty */

#endif  /* DYNAMIC_ANNOTATIONS_ENABLED */

/* These annotations are also made available to LLVM's Memory Sanitizer */
#if DYNAMIC_ANNOTATIONS_ENABLED == 1 || defined(MEMORY_SANITIZER)
  #define ANNOTATE_MEMORY_IS_INITIALIZED(address, size) \
    AnnotateMemoryIsInitialized(__FILE__, __LINE__, address, size)

  #define ANNOTATE_MEMORY_IS_UNINITIALIZED(address, size) \
    AnnotateMemoryIsUninitialized(__FILE__, __LINE__, address, size)
#else
  #define ANNOTATE_MEMORY_IS_INITIALIZED(address, size) /* empty */
  #define ANNOTATE_MEMORY_IS_UNINITIALIZED(address, size) /* empty */
#endif  /* DYNAMIC_ANNOTATIONS_ENABLED || MEMORY_SANITIZER */
/* TODO(delesley) -- Replace __CLANG_SUPPORT_DYN_ANNOTATION__ with the
   appropriate feature ID. */
#if defined(__clang__) && (!defined(SWIG)) \
    && defined(__CLANG_SUPPORT_DYN_ANNOTATION__)

  #if DYNAMIC_ANNOTATIONS_ENABLED == 0
    #define ANNOTALYSIS_ENABLED
  #endif

  /* When running in opt-mode, GCC will issue a warning, if these attributes are
     compiled. Only include them when compiling using Clang. */
  #define ATTRIBUTE_IGNORE_READS_BEGIN \
      __attribute((exclusive_lock_function("*")))
  #define ATTRIBUTE_IGNORE_READS_END \
      __attribute((unlock_function("*")))
#else
  #define ATTRIBUTE_IGNORE_READS_BEGIN  /* empty */
  #define ATTRIBUTE_IGNORE_READS_END  /* empty */
#endif  /* defined(__clang__) && ... */

#if (DYNAMIC_ANNOTATIONS_ENABLED != 0) || defined(ANNOTALYSIS_ENABLED)
  #define ANNOTATIONS_ENABLED
#endif

#if (DYNAMIC_ANNOTATIONS_ENABLED != 0)

  /* Request the analysis tool to ignore all reads in the current thread
     until ANNOTATE_IGNORE_READS_END is called.
     Useful to ignore intentional racey reads, while still checking
     other reads and all writes.
     See also ANNOTATE_UNPROTECTED_READ. */
  #define ANNOTATE_IGNORE_READS_BEGIN() \
    AnnotateIgnoreReadsBegin(__FILE__, __LINE__)

  /* Stop ignoring reads. */
  #define ANNOTATE_IGNORE_READS_END() \
    AnnotateIgnoreReadsEnd(__FILE__, __LINE__)

  /* Similar to ANNOTATE_IGNORE_READS_BEGIN, but ignore writes instead. */
  #define ANNOTATE_IGNORE_WRITES_BEGIN() \
    AnnotateIgnoreWritesBegin(__FILE__, __LINE__)

  /* Stop ignoring writes. */
  #define ANNOTATE_IGNORE_WRITES_END() \
    AnnotateIgnoreWritesEnd(__FILE__, __LINE__)

/* Clang provides limited support for static thread-safety analysis
   through a feature called Annotalysis. We configure macro-definitions
   according to whether Annotalysis support is available. */
#elif defined(ANNOTALYSIS_ENABLED)

  #define ANNOTATE_IGNORE_READS_BEGIN() \
    StaticAnnotateIgnoreReadsBegin(__FILE__, __LINE__)

  #define ANNOTATE_IGNORE_READS_END() \
    StaticAnnotateIgnoreReadsEnd(__FILE__, __LINE__)

  #define ANNOTATE_IGNORE_WRITES_BEGIN() \
    StaticAnnotateIgnoreWritesBegin(__FILE__, __LINE__)

  #define ANNOTATE_IGNORE_WRITES_END() \
    StaticAnnotateIgnoreWritesEnd(__FILE__, __LINE__)

#else
  #define ANNOTATE_IGNORE_READS_BEGIN()  /* empty */
  #define ANNOTATE_IGNORE_READS_END()  /* empty */
  #define ANNOTATE_IGNORE_WRITES_BEGIN()  /* empty */
  #define ANNOTATE_IGNORE_WRITES_END()  /* empty */
#endif

/* Implement the ANNOTATE_IGNORE_READS_AND_WRITES_* annotations using the more
   primitive annotations defined above. */
#if defined(ANNOTATIONS_ENABLED)

  /* Start ignoring all memory accesses (both reads and writes). */
  #define ANNOTATE_IGNORE_READS_AND_WRITES_BEGIN() \
    do {                                           \
      ANNOTATE_IGNORE_READS_BEGIN();               \
      ANNOTATE_IGNORE_WRITES_BEGIN();              \
    }while (0)

  /* Stop ignoring both reads and writes. */
  #define ANNOTATE_IGNORE_READS_AND_WRITES_END()   \
    do {                                           \
      ANNOTATE_IGNORE_WRITES_END();                \
      ANNOTATE_IGNORE_READS_END();                 \
    }while (0)

#else
  #define ANNOTATE_IGNORE_READS_AND_WRITES_BEGIN()  /* empty */
  #define ANNOTATE_IGNORE_READS_AND_WRITES_END()  /* empty */
#endif

/* Use the macros above rather than using these functions directly. */
#include <stddef.h>
#ifdef __cplusplus
extern "C" {
#endif
void AnnotateRWLockCreate(const char *file, int line,
                          const volatile void *lock);
void AnnotateRWLockCreateStatic(const char *file, int line,
                          const volatile void *lock);
void AnnotateRWLockDestroy(const char *file, int line,
                           const volatile void *lock);
void AnnotateRWLockAcquired(const char *file, int line,
                            const volatile void *lock, long is_w);  /* NOLINT */
void AnnotateRWLockReleased(const char *file, int line,
                            const volatile void *lock, long is_w);  /* NOLINT */
void AnnotateBenignRace(const char *file, int line,
                        const volatile void *address,
                        const char *description);
void AnnotateBenignRaceSized(const char *file, int line,
                        const volatile void *address,
                        size_t size,
                        const char *description);
void AnnotateThreadName(const char *file, int line,
                        const char *name);
void AnnotateEnableRaceDetection(const char *file, int line, int enable);
void AnnotateMemoryIsInitialized(const char *file, int line,
                                 const volatile void *mem, size_t size);
void AnnotateMemoryIsUninitialized(const char *file, int line,
                                   const volatile void *mem, size_t size);

/* Annotations expand to these functions, when Dynamic Annotations are enabled.
   These functions are either implemented as no-op calls, if no Sanitizer is
   attached, or provided with externally-linked implementations by a library
   like ThreadSanitizer. */
void AnnotateIgnoreReadsBegin(const char *file, int line)
    ATTRIBUTE_IGNORE_READS_BEGIN;
void AnnotateIgnoreReadsEnd(const char *file, int line)
    ATTRIBUTE_IGNORE_READS_END;
void AnnotateIgnoreWritesBegin(const char *file, int line);
void AnnotateIgnoreWritesEnd(const char *file, int line);

#if defined(ANNOTALYSIS_ENABLED)
/* When Annotalysis is enabled without Dynamic Annotations, the use of
   static-inline functions allows the annotations to be read at compile-time,
   while still letting the compiler elide the functions from the final build.

   TODO(delesley) -- The exclusive lock here ignores writes as well, but
   allows IGNORE_READS_AND_WRITES to work properly. */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
static inline void StaticAnnotateIgnoreReadsBegin(const char *file, int line)
    ATTRIBUTE_IGNORE_READS_BEGIN { (void)file; (void)line; }
static inline void StaticAnnotateIgnoreReadsEnd(const char *file, int line)
    ATTRIBUTE_IGNORE_READS_END { (void)file; (void)line; }
static inline void StaticAnnotateIgnoreWritesBegin(
    const char *file, int line) { (void)file; (void)line; }
static inline void StaticAnnotateIgnoreWritesEnd(
    const char *file, int line) { (void)file; (void)line; }
#pragma GCC diagnostic pop
#endif

/* Return non-zero value if running under valgrind.

  If "valgrind.h" is included into dynamic_annotations.cc,
  the regular valgrind mechanism will be used.
  See http://valgrind.org/docs/manual/manual-core-adv.html about
  RUNNING_ON_VALGRIND and other valgrind "client requests".
  The file "valgrind.h" may be obtained by doing
     svn co svn://svn.valgrind.org/valgrind/trunk/include

  If for some reason you can't use "valgrind.h" or want to fake valgrind,
  there are two ways to make this function return non-zero:
    - Use environment variable: export RUNNING_ON_VALGRIND=1
    - Make your tool intercept the function RunningOnValgrind() and
      change its return value.
 */
int RunningOnValgrind(void);

/* ValgrindSlowdown returns:
    * 1.0, if (RunningOnValgrind() == 0)
    * 50.0, if (RunningOnValgrind() != 0 && getenv("VALGRIND_SLOWDOWN") == NULL)
    * atof(getenv("VALGRIND_SLOWDOWN")) otherwise
   This function can be used to scale timeout values:
   EXAMPLE:
   for (;;) {
     DoExpensiveBackgroundTask();
     SleepForSeconds(5 * ValgrindSlowdown());
   }
 */
double ValgrindSlowdown(void);

#ifdef __cplusplus
}
#endif

/* ANNOTATE_UNPROTECTED_READ is the preferred way to annotate racey reads.

     Instead of doing
        ANNOTATE_IGNORE_READS_BEGIN();
        ... = x;
        ANNOTATE_IGNORE_READS_END();
     one can use
        ... = ANNOTATE_UNPROTECTED_READ(x); */
#if defined(__cplusplus) && defined(ANNOTATIONS_ENABLED)
template <typename T>
inline T ANNOTATE_UNPROTECTED_READ(const volatile T &x) { /* NOLINT */
  ANNOTATE_IGNORE_READS_BEGIN();
  T res = x;
  ANNOTATE_IGNORE_READS_END();
  return res;
  }
#else
  #define ANNOTATE_UNPROTECTED_READ(x) (x)
#endif

#if DYNAMIC_ANNOTATIONS_ENABLED != 0 && defined(__cplusplus)
  /* Apply ANNOTATE_BENIGN_RACE_SIZED to a static variable. */
  #define ANNOTATE_BENIGN_RACE_STATIC(static_var, description)        \
    namespace {                                                       \
      class static_var ## _annotator {                                \
       public:                                                        \
        static_var ## _annotator() {                                  \
          ANNOTATE_BENIGN_RACE_SIZED(&static_var,                     \
                                      sizeof(static_var),             \
            # static_var ": " description);                           \
        }                                                             \
      };                                                              \
      static static_var ## _annotator the ## static_var ## _annotator;\
    }  // namespace
#else /* DYNAMIC_ANNOTATIONS_ENABLED == 0 */
  #define ANNOTATE_BENIGN_RACE_STATIC(static_var, description)  /* empty */
#endif /* DYNAMIC_ANNOTATIONS_ENABLED */

#ifdef ADDRESS_SANITIZER
/* Describe the current state of a contiguous container such as e.g.
 * std::vector or std::string. For more details see
 * sanitizer/common_interface_defs.h, which is provided by the compiler. */
#include <sanitizer/common_interface_defs.h>
#define ANNOTATE_CONTIGUOUS_CONTAINER(beg, end, old_mid, new_mid) \
  __sanitizer_annotate_contiguous_container(beg, end, old_mid, new_mid)
#define ADDRESS_SANITIZER_REDZONE(name)         \
  struct { char x[8] __attribute__ ((aligned (8))); } name
#else
#define ANNOTATE_CONTIGUOUS_CONTAINER(beg, end, old_mid, new_mid)
#define ADDRESS_SANITIZER_REDZONE(name)
#endif  // ADDRESS_SANITIZER

/* Undefine the macros intended only in this file. */
#undef ANNOTALYSIS_ENABLED
#undef ANNOTATIONS_ENABLED
#undef ATTRIBUTE_IGNORE_READS_BEGIN
#undef ATTRIBUTE_IGNORE_READS_END

#endif /* !__native_client__ */

#endif  /* ABSL_BASE_DYNAMIC_ANNOTATIONS_H_ */

#define ABSL_RAW_CHECK(cond, msg) assert((cond) && (msg))
#define ABSL_RAW_LOG(sev, ...) do {} while(0)

// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Produce stack trace.
//
// There are three different ways we can try to get the stack trace:
//
// 1) Our hand-coded stack-unwinder.  This depends on a certain stack
//    layout, which is used by gcc (and those systems using a
//    gcc-compatible ABI) on x86 systems, at least since gcc 2.95.
//    It uses the frame pointer to do its work.
//
// 2) The libunwind library.  This is still in development, and as a
//    separate library adds a new dependency, but doesn't need a frame
//    pointer.  It also doesn't call malloc.
//
// 3) The gdb unwinder -- also the one used by the c++ exception code.
//    It's obviously well-tested, but has a fatal flaw: it can call
//    malloc() from the unwinder.  This is a problem because we're
//    trying to use the unwinder to instrument malloc().
//
// Note: if you add a new implementation here, make sure it works
// correctly when absl::GetStackTrace() is called with max_depth == 0.
// Some code may do that.


#include <atomic>


#if defined(ABSL_STACKTRACE_INL_HEADER)
#include ABSL_STACKTRACE_INL_HEADER
#else
# error Cannot calculate stack trace: will need to write for your environment
# include "stacktrace_internal/stacktrace_aarch64-inl.inc"
# include "stacktrace_internal/stacktrace_arm-inl.inc"
# include "stacktrace_internal/stacktrace_generic-inl.inc"
# include "stacktrace_internal/stacktrace_powerpc-inl.inc"
# include "stacktrace_internal/stacktrace_unimplemented-inl.inc"
# include "stacktrace_internal/stacktrace_win32-inl.inc"
# include "stacktrace_internal/stacktrace_x86-inl.inc"
#endif

namespace absl {
namespace {

typedef int (*Unwinder)(void**, int*, int, int, const void*, int*);
std::atomic<Unwinder> custom;

template <bool IS_STACK_FRAMES, bool IS_WITH_CONTEXT>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline int Unwind(void** result, int* sizes,
                                               int max_depth, int skip_count,
                                               const void* uc,
                                               int* min_dropped_frames) {
  Unwinder f = &UnwindImpl<IS_STACK_FRAMES, IS_WITH_CONTEXT>;
  Unwinder g = custom.load(std::memory_order_acquire);
  if (g != nullptr) f = g;

  // Add 1 to skip count for the unwinder function itself
  int size = (*f)(result, sizes, max_depth, skip_count + 1, uc,
                  min_dropped_frames);
  // To disable tail call to (*f)(...)
  ABSL_BLOCK_TAIL_CALL_OPTIMIZATION();
  return size;
}

}  // anonymous namespace

int GetStackFrames(void** result, int* sizes, int max_depth, int skip_count) {
  return Unwind<true, false>(result, sizes, max_depth, skip_count, nullptr,
                             nullptr);
}

int GetStackFramesWithContext(void** result, int* sizes, int max_depth,
                              int skip_count, const void* uc,
                              int* min_dropped_frames) {
  return Unwind<true, true>(result, sizes, max_depth, skip_count, uc,
                            min_dropped_frames);
}

int GetStackTrace(void** result, int max_depth, int skip_count) {
  return Unwind<false, false>(result, nullptr, max_depth, skip_count, nullptr,
                              nullptr);
}

int GetStackTraceWithContext(void** result, int max_depth, int skip_count,
                             const void* uc, int* min_dropped_frames) {
  return Unwind<false, true>(result, nullptr, max_depth, skip_count, uc,
                             min_dropped_frames);
}

void SetStackUnwinder(Unwinder w) {
  custom.store(w, std::memory_order_release);
}

int DefaultStackUnwinder(void** pcs, int* sizes, int depth, int skip,
                         const void* uc, int* min_dropped_frames) {
  skip++;  // For this function
  Unwinder f = nullptr;
  if (sizes == nullptr) {
    if (uc == nullptr) {
      f = &UnwindImpl<false, false>;
    } else {
      f = &UnwindImpl<false, true>;
    }
  } else {
    if (uc == nullptr) {
      f = &UnwindImpl<true, false>;
    } else {
      f = &UnwindImpl<true, true>;
    }
  }
  volatile int x = 0;
  int n = (*f)(pcs, sizes, depth, skip, uc, min_dropped_frames);
  x = 1; (void) x;  // To disable tail call to (*f)(...)
  return n;
}

}  // namespace absl
// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// base::AddressIsReadable() probes an address to see whether it is readable,
// without faulting.


#if !defined(__linux__) || defined(__ANDROID__)

namespace absl {
namespace debug_internal {

// On platforms other than Linux, just return true.
bool AddressIsReadable(const void* /* addr */) { return true; }

}  // namespace debug_internal
}  // namespace absl

#else

#include <fcntl.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <atomic>
#include <cerrno>
#include <cstdint>


namespace absl {
namespace debug_internal {

// Pack a pid and two file descriptors into a 64-bit word,
// using 16, 24, and 24 bits for each respectively.
static uint64_t Pack(uint64_t pid, uint64_t read_fd, uint64_t write_fd) {
  ABSL_RAW_CHECK((read_fd >> 24) == 0 && (write_fd >> 24) == 0,
                 "fd out of range");
  return (pid << 48) | ((read_fd & 0xffffff) << 24) | (write_fd & 0xffffff);
}

// Unpack x into a pid and two file descriptors, where x was created with
// Pack().
static void Unpack(uint64_t x, int *pid, int *read_fd, int *write_fd) {
  *pid = x >> 48;
  *read_fd = (x >> 24) & 0xffffff;
  *write_fd = x & 0xffffff;
}

// Return whether the byte at *addr is readable, without faulting.
// Save and restores errno.   Returns true on systems where
// unimplemented.
// This is a namespace-scoped variable for correct zero-initialization.
static std::atomic<uint64_t> pid_and_fds;  // initially 0, an invalid pid.
bool AddressIsReadable(const void *addr) {
  int save_errno = errno;
  // We test whether a byte is readable by using write().  Normally, this would
  // be done via a cached file descriptor to /dev/null, but linux fails to
  // check whether the byte is readable when the destination is /dev/null, so
  // we use a cached pipe.  We store the pid of the process that created the
  // pipe to handle the case where a process forks, and the child closes all
  // the file descriptors and then calls this routine.  This is not perfect:
  // the child could use the routine, then close all file descriptors and then
  // use this routine again.  But the likely use of this routine is when
  // crashing, to test the validity of pages when dumping the stack.  Beware
  // that we may leak file descriptors, but we're unlikely to leak many.
  int bytes_written;
  int current_pid = getpid() & 0xffff;   // we use only the low order 16 bits
  do {  // until we do not get EBADF trying to use file descriptors
    int pid;
    int read_fd;
    int write_fd;
    uint64_t local_pid_and_fds = pid_and_fds.load(std::memory_order_relaxed);
    Unpack(local_pid_and_fds, &pid, &read_fd, &write_fd);
    while (current_pid != pid) {
      int p[2];
      // new pipe
      if (pipe(p) != 0) {
        ABSL_RAW_LOG(FATAL, "Failed to create pipe, errno=%d", errno);
      }
      fcntl(p[0], F_SETFD, FD_CLOEXEC);
      fcntl(p[1], F_SETFD, FD_CLOEXEC);
      uint64_t new_pid_and_fds = Pack(current_pid, p[0], p[1]);
      if (pid_and_fds.compare_exchange_strong(
              local_pid_and_fds, new_pid_and_fds, std::memory_order_relaxed,
              std::memory_order_relaxed)) {
        local_pid_and_fds = new_pid_and_fds;  // fds exposed to other threads
      } else {  // fds not exposed to other threads; we can close them.
        close(p[0]);
        close(p[1]);
        local_pid_and_fds = pid_and_fds.load(std::memory_order_relaxed);
      }
      Unpack(local_pid_and_fds, &pid, &read_fd, &write_fd);
    }
    errno = 0;
    // Use syscall(SYS_write, ...) instead of write() to prevent ASAN
    // and other checkers from complaining about accesses to arbitrary
    // memory.
    do {
      bytes_written = syscall(SYS_write, write_fd, addr, 1);
    } while (bytes_written == -1 && errno == EINTR);
    if (bytes_written == 1) {   // remove the byte from the pipe
      char c;
      while (read(read_fd, &c, 1) == -1 && errno == EINTR) {
      }
    }
    if (errno == EBADF) {  // Descriptors invalid.
      // If pid_and_fds contains the problematic file descriptors we just used,
      // this call will forget them, and the loop will try again.
      pid_and_fds.compare_exchange_strong(local_pid_and_fds, 0,
                                          std::memory_order_relaxed,
                                          std::memory_order_relaxed);
    }
  } while (errno == EBADF);
  errno = save_errno;
  return bytes_written == 1;
}

}  // namespace debug_internal
}  // namespace absl

#endif
// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Allow dynamic symbol lookup in an in-memory Elf image.
//


#ifdef ABSL_HAVE_ELF_MEM_IMAGE  // defined in elf_mem_image.h

#include <string.h>
#include <cassert>
#include <cstddef>

// From binutils/include/elf/common.h (this doesn't appear to be documented
// anywhere else).
//
//   /* This flag appears in a Versym structure.  It means that the symbol
//      is hidden, and is only visible with an explicit version number.
//      This is a GNU extension.  */
//   #define VERSYM_HIDDEN           0x8000
//
//   /* This is the mask for the rest of the Versym information.  */
//   #define VERSYM_VERSION          0x7fff

#define VERSYM_VERSION 0x7fff

namespace absl {
namespace debug_internal {

namespace {

#if __WORDSIZE == 32
const int kElfClass = ELFCLASS32;
int ElfBind(const ElfW(Sym) *symbol) { return ELF32_ST_BIND(symbol->st_info); }
int ElfType(const ElfW(Sym) *symbol) { return ELF32_ST_TYPE(symbol->st_info); }
#elif __WORDSIZE == 64
const int kElfClass = ELFCLASS64;
int ElfBind(const ElfW(Sym) *symbol) { return ELF64_ST_BIND(symbol->st_info); }
int ElfType(const ElfW(Sym) *symbol) { return ELF64_ST_TYPE(symbol->st_info); }
#else
const int kElfClass = -1;
int ElfBind(const ElfW(Sym) *) {
  ABSL_RAW_LOG(FATAL, "Unexpected word size");
  return 0;
}
int ElfType(const ElfW(Sym) *) {
  ABSL_RAW_LOG(FATAL, "Unexpected word size");
  return 0;
}
#endif

// Extract an element from one of the ELF tables, cast it to desired type.
// This is just a simple arithmetic and a glorified cast.
// Callers are responsible for bounds checking.
template <typename T>
const T *GetTableElement(const ElfW(Ehdr) * ehdr, ElfW(Off) table_offset,
                         ElfW(Word) element_size, size_t index) {
  return reinterpret_cast<const T*>(reinterpret_cast<const char *>(ehdr)
                                    + table_offset
                                    + index * element_size);
}

}  // namespace

const void *const ElfMemImage::kInvalidBase =
    reinterpret_cast<const void *>(~0L);

ElfMemImage::ElfMemImage(const void *base) {
  ABSL_RAW_CHECK(base != kInvalidBase, "bad pointer");
  Init(base);
}

int ElfMemImage::GetNumSymbols() const {
  if (!hash_) {
    return 0;
  }
  // See http://www.caldera.com/developers/gabi/latest/ch5.dynamic.html#hash
  return hash_[1];
}

const ElfW(Sym) *ElfMemImage::GetDynsym(int index) const {
  ABSL_RAW_CHECK(index < GetNumSymbols(), "index out of range");
  return dynsym_ + index;
}

const ElfW(Versym) *ElfMemImage::GetVersym(int index) const {
  ABSL_RAW_CHECK(index < GetNumSymbols(), "index out of range");
  return versym_ + index;
}

const ElfW(Phdr) *ElfMemImage::GetPhdr(int index) const {
  ABSL_RAW_CHECK(index < ehdr_->e_phnum, "index out of range");
  return GetTableElement<ElfW(Phdr)>(ehdr_,
                                     ehdr_->e_phoff,
                                     ehdr_->e_phentsize,
                                     index);
}

const char *ElfMemImage::GetDynstr(ElfW(Word) offset) const {
  ABSL_RAW_CHECK(offset < strsize_, "offset out of range");
  return dynstr_ + offset;
}

const void *ElfMemImage::GetSymAddr(const ElfW(Sym) *sym) const {
  if (sym->st_shndx == SHN_UNDEF || sym->st_shndx >= SHN_LORESERVE) {
    // Symbol corresponds to "special" (e.g. SHN_ABS) section.
    return reinterpret_cast<const void *>(sym->st_value);
  }
  ABSL_RAW_CHECK(link_base_ < sym->st_value, "symbol out of range");
  return GetTableElement<char>(ehdr_, 0, 1, sym->st_value) - link_base_;
}

const ElfW(Verdef) *ElfMemImage::GetVerdef(int index) const {
  ABSL_RAW_CHECK(0 <= index && static_cast<size_t>(index) <= verdefnum_,
                 "index out of range");
  const ElfW(Verdef) *version_definition = verdef_;
  while (version_definition->vd_ndx < index && version_definition->vd_next) {
    const char *const version_definition_as_char =
        reinterpret_cast<const char *>(version_definition);
    version_definition =
        reinterpret_cast<const ElfW(Verdef) *>(version_definition_as_char +
                                               version_definition->vd_next);
  }
  return version_definition->vd_ndx == index ? version_definition : nullptr;
}

const ElfW(Verdaux) *ElfMemImage::GetVerdefAux(
    const ElfW(Verdef) *verdef) const {
  return reinterpret_cast<const ElfW(Verdaux) *>(verdef+1);
}

const char *ElfMemImage::GetVerstr(ElfW(Word) offset) const {
  ABSL_RAW_CHECK(offset < strsize_, "offset out of range");
  return dynstr_ + offset;
}

void ElfMemImage::Init(const void *base) {
  ehdr_      = nullptr;
  dynsym_    = nullptr;
  dynstr_    = nullptr;
  versym_    = nullptr;
  verdef_    = nullptr;
  hash_      = nullptr;
  strsize_   = 0;
  verdefnum_ = 0;
  link_base_ = ~0L;  // Sentinel: PT_LOAD .p_vaddr can't possibly be this.
  if (!base) {
    return;
  }
  const intptr_t base_as_uintptr_t = reinterpret_cast<uintptr_t>(base);
  // Fake VDSO has low bit set.
  const bool fake_vdso = ((base_as_uintptr_t & 1) != 0);
  base = reinterpret_cast<const void *>(base_as_uintptr_t & ~1);
  const char *const base_as_char = reinterpret_cast<const char *>(base);
  if (base_as_char[EI_MAG0] != ELFMAG0 || base_as_char[EI_MAG1] != ELFMAG1 ||
      base_as_char[EI_MAG2] != ELFMAG2 || base_as_char[EI_MAG3] != ELFMAG3) {
    assert(false);
    return;
  }
  int elf_class = base_as_char[EI_CLASS];
  if (elf_class != kElfClass) {
    assert(false);
    return;
  }
  switch (base_as_char[EI_DATA]) {
    case ELFDATA2LSB: {
      if (__LITTLE_ENDIAN != __BYTE_ORDER) {
        assert(false);
        return;
      }
      break;
    }
    case ELFDATA2MSB: {
      if (__BIG_ENDIAN != __BYTE_ORDER) {
        assert(false);
        return;
      }
      break;
    }
    default: {
      assert(false);
      return;
    }
  }

  ehdr_ = reinterpret_cast<const ElfW(Ehdr) *>(base);
  const ElfW(Phdr) *dynamic_program_header = nullptr;
  for (int i = 0; i < ehdr_->e_phnum; ++i) {
    const ElfW(Phdr) *const program_header = GetPhdr(i);
    switch (program_header->p_type) {
      case PT_LOAD:
        if (!~link_base_) {
          link_base_ = program_header->p_vaddr;
        }
        break;
      case PT_DYNAMIC:
        dynamic_program_header = program_header;
        break;
    }
  }
  if (!~link_base_ || !dynamic_program_header) {
    assert(false);
    // Mark this image as not present. Can not recur infinitely.
    Init(nullptr);
    return;
  }
  ptrdiff_t relocation =
      base_as_char - reinterpret_cast<const char *>(link_base_);
  ElfW(Dyn) *dynamic_entry =
      reinterpret_cast<ElfW(Dyn) *>(dynamic_program_header->p_vaddr +
                                    relocation);
  for (; dynamic_entry->d_tag != DT_NULL; ++dynamic_entry) {
    ElfW(Xword) value = dynamic_entry->d_un.d_val;
    if (fake_vdso) {
      // A complication: in the real VDSO, dynamic entries are not relocated
      // (it wasn't loaded by a dynamic loader). But when testing with a
      // "fake" dlopen()ed vdso library, the loader relocates some (but
      // not all!) of them before we get here.
      if (dynamic_entry->d_tag == DT_VERDEF) {
        // The only dynamic entry (of the ones we care about) libc-2.3.6
        // loader doesn't relocate.
        value += relocation;
      }
    } else {
      // Real VDSO. Everything needs to be relocated.
      value += relocation;
    }
    switch (dynamic_entry->d_tag) {
      case DT_HASH:
        hash_ = reinterpret_cast<ElfW(Word) *>(value);
        break;
      case DT_SYMTAB:
        dynsym_ = reinterpret_cast<ElfW(Sym) *>(value);
        break;
      case DT_STRTAB:
        dynstr_ = reinterpret_cast<const char *>(value);
        break;
      case DT_VERSYM:
        versym_ = reinterpret_cast<ElfW(Versym) *>(value);
        break;
      case DT_VERDEF:
        verdef_ = reinterpret_cast<ElfW(Verdef) *>(value);
        break;
      case DT_VERDEFNUM:
        verdefnum_ = dynamic_entry->d_un.d_val;
        break;
      case DT_STRSZ:
        strsize_ = dynamic_entry->d_un.d_val;
        break;
      default:
        // Unrecognized entries explicitly ignored.
        break;
    }
  }
  if (!hash_ || !dynsym_ || !dynstr_ || !versym_ ||
      !verdef_ || !verdefnum_ || !strsize_) {
    assert(false);  // invalid VDSO
    // Mark this image as not present. Can not recur infinitely.
    Init(nullptr);
    return;
  }
}

bool ElfMemImage::LookupSymbol(const char *name,
                               const char *version,
                               int type,
                               SymbolInfo *info_out) const {
  for (const SymbolInfo& info : *this) {
    if (strcmp(info.name, name) == 0 && strcmp(info.version, version) == 0 &&
        ElfType(info.symbol) == type) {
      if (info_out) {
        *info_out = info;
      }
      return true;
    }
  }
  return false;
}

bool ElfMemImage::LookupSymbolByAddress(const void *address,
                                        SymbolInfo *info_out) const {
  for (const SymbolInfo& info : *this) {
    const char *const symbol_start =
        reinterpret_cast<const char *>(info.address);
    const char *const symbol_end = symbol_start + info.symbol->st_size;
    if (symbol_start <= address && address < symbol_end) {
      if (info_out) {
        // Client wants to know details for that symbol (the usual case).
        if (ElfBind(info.symbol) == STB_GLOBAL) {
          // Strong symbol; just return it.
          *info_out = info;
          return true;
        } else {
          // Weak or local. Record it, but keep looking for a strong one.
          *info_out = info;
        }
      } else {
        // Client only cares if there is an overlapping symbol.
        return true;
      }
    }
  }
  return false;
}

ElfMemImage::SymbolIterator::SymbolIterator(const void *const image, int index)
    : index_(index), image_(image) {
}

const ElfMemImage::SymbolInfo *ElfMemImage::SymbolIterator::operator->() const {
  return &info_;
}

const ElfMemImage::SymbolInfo& ElfMemImage::SymbolIterator::operator*() const {
  return info_;
}

bool ElfMemImage::SymbolIterator::operator==(const SymbolIterator &rhs) const {
  return this->image_ == rhs.image_ && this->index_ == rhs.index_;
}

bool ElfMemImage::SymbolIterator::operator!=(const SymbolIterator &rhs) const {
  return !(*this == rhs);
}

ElfMemImage::SymbolIterator &ElfMemImage::SymbolIterator::operator++() {
  this->Update(1);
  return *this;
}

ElfMemImage::SymbolIterator ElfMemImage::begin() const {
  SymbolIterator it(this, 0);
  it.Update(0);
  return it;
}

ElfMemImage::SymbolIterator ElfMemImage::end() const {
  return SymbolIterator(this, GetNumSymbols());
}

void ElfMemImage::SymbolIterator::Update(int increment) {
  const ElfMemImage *image = reinterpret_cast<const ElfMemImage *>(image_);
  ABSL_RAW_CHECK(image->IsPresent() || increment == 0, "");
  if (!image->IsPresent()) {
    return;
  }
  index_ += increment;
  if (index_ >= image->GetNumSymbols()) {
    index_ = image->GetNumSymbols();
    return;
  }
  const ElfW(Sym)    *symbol = image->GetDynsym(index_);
  const ElfW(Versym) *version_symbol = image->GetVersym(index_);
  ABSL_RAW_CHECK(symbol && version_symbol, "");
  const char *const symbol_name = image->GetDynstr(symbol->st_name);
  const ElfW(Versym) version_index = version_symbol[0] & VERSYM_VERSION;
  const ElfW(Verdef) *version_definition = nullptr;
  const char *version_name = "";
  if (symbol->st_shndx == SHN_UNDEF) {
    // Undefined symbols reference DT_VERNEED, not DT_VERDEF, and
    // version_index could well be greater than verdefnum_, so calling
    // GetVerdef(version_index) may trigger assertion.
  } else {
    version_definition = image->GetVerdef(version_index);
  }
  if (version_definition) {
    // I am expecting 1 or 2 auxiliary entries: 1 for the version itself,
    // optional 2nd if the version has a parent.
    ABSL_RAW_CHECK(
        version_definition->vd_cnt == 1 || version_definition->vd_cnt == 2,
        "wrong number of entries");
    const ElfW(Verdaux) *version_aux = image->GetVerdefAux(version_definition);
    version_name = image->GetVerstr(version_aux->vda_name);
  }
  info_.name    = symbol_name;
  info_.version = version_name;
  info_.address = image->GetSymAddr(symbol);
  info_.symbol  = symbol;
}

}  // namespace debug_internal
}  // namespace absl

#endif  // ABSL_HAVE_ELF_MEM_IMAGE
// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Allow dynamic symbol lookup in the kernel VDSO page.
//
// VDSOSupport -- a class representing kernel VDSO (if present).


#ifdef ABSL_HAVE_VDSO_SUPPORT     // defined in vdso_support.h

#include <fcntl.h>
#include <sys/syscall.h>
#include <unistd.h>


#ifndef AT_SYSINFO_EHDR
#define AT_SYSINFO_EHDR 33  // for crosstoolv10
#endif

namespace absl {
namespace debug_internal {

std::atomic<const void *> VDSOSupport::vdso_base_(
    debug_internal::ElfMemImage::kInvalidBase);
std::atomic<VDSOSupport::GetCpuFn> VDSOSupport::getcpu_fn_(&InitAndGetCPU);
VDSOSupport::VDSOSupport()
    // If vdso_base_ is still set to kInvalidBase, we got here
    // before VDSOSupport::Init has been called. Call it now.
    : image_(vdso_base_.load(std::memory_order_relaxed) ==
                     debug_internal::ElfMemImage::kInvalidBase
                 ? Init()
                 : vdso_base_.load(std::memory_order_relaxed)) {}

// NOTE: we can't use GoogleOnceInit() below, because we can be
// called by tcmalloc, and none of the *once* stuff may be functional yet.
//
// In addition, we hope that the VDSOSupportHelper constructor
// causes this code to run before there are any threads, and before
// InitGoogle() has executed any chroot or setuid calls.
//
// Finally, even if there is a race here, it is harmless, because
// the operation should be idempotent.
const void *VDSOSupport::Init() {
  if (vdso_base_.load(std::memory_order_relaxed) ==
      debug_internal::ElfMemImage::kInvalidBase) {
    {
      // Valgrind zaps AT_SYSINFO_EHDR and friends from the auxv[]
      // on stack, and so glibc works as if VDSO was not present.
      // But going directly to kernel via /proc/self/auxv below bypasses
      // Valgrind zapping. So we check for Valgrind separately.
      if (RunningOnValgrind()) {
        vdso_base_.store(nullptr, std::memory_order_relaxed);
        getcpu_fn_.store(&GetCPUViaSyscall, std::memory_order_relaxed);
        return nullptr;
      }
      int fd = open("/proc/self/auxv", O_RDONLY);
      if (fd == -1) {
        // Kernel too old to have a VDSO.
        vdso_base_.store(nullptr, std::memory_order_relaxed);
        getcpu_fn_.store(&GetCPUViaSyscall, std::memory_order_relaxed);
        return nullptr;
      }
      ElfW(auxv_t) aux;
      while (read(fd, &aux, sizeof(aux)) == sizeof(aux)) {
        if (aux.a_type == AT_SYSINFO_EHDR) {
          vdso_base_.store(reinterpret_cast<void *>(aux.a_un.a_val),
                           std::memory_order_relaxed);
          break;
        }
      }
      close(fd);
    }
    if (vdso_base_.load(std::memory_order_relaxed) ==
        debug_internal::ElfMemImage::kInvalidBase) {
      // Didn't find AT_SYSINFO_EHDR in auxv[].
      vdso_base_.store(nullptr, std::memory_order_relaxed);
    }
  }
  GetCpuFn fn = &GetCPUViaSyscall;  // default if VDSO not present.
  if (vdso_base_.load(std::memory_order_relaxed)) {
    VDSOSupport vdso;
    SymbolInfo info;
    if (vdso.LookupSymbol("__vdso_getcpu", "LINUX_2.6", STT_FUNC, &info)) {
      fn = reinterpret_cast<GetCpuFn>(const_cast<void *>(info.address));
    }
  }
  // Subtle: this code runs outside of any locks; prevent compiler
  // from assigning to getcpu_fn_ more than once.
  getcpu_fn_.store(fn, std::memory_order_relaxed);
  return vdso_base_.load(std::memory_order_relaxed);
}

const void *VDSOSupport::SetBase(const void *base) {
  ABSL_RAW_CHECK(base != debug_internal::ElfMemImage::kInvalidBase,
                 "internal error");
  const void *old_base = vdso_base_.load(std::memory_order_relaxed);
  vdso_base_.store(base, std::memory_order_relaxed);
  image_.Init(base);
  // Also reset getcpu_fn_, so GetCPU could be tested with simulated VDSO.
  getcpu_fn_.store(&InitAndGetCPU, std::memory_order_relaxed);
  return old_base;
}

bool VDSOSupport::LookupSymbol(const char *name,
                               const char *version,
                               int type,
                               SymbolInfo *info) const {
  return image_.LookupSymbol(name, version, type, info);
}

bool VDSOSupport::LookupSymbolByAddress(const void *address,
                                        SymbolInfo *info_out) const {
  return image_.LookupSymbolByAddress(address, info_out);
}

// NOLINT on 'long' because this routine mimics kernel api.
long VDSOSupport::GetCPUViaSyscall(unsigned *cpu,  // NOLINT(runtime/int)
                                   void *, void *) {
#ifdef SYS_getcpu
  return syscall(SYS_getcpu, cpu, nullptr, nullptr);
#else
  // x86_64 never implemented sys_getcpu(), except as a VDSO call.
  errno = ENOSYS;
  return -1;
#endif
}

// Use fast __vdso_getcpu if available.
long VDSOSupport::InitAndGetCPU(unsigned *cpu,  // NOLINT(runtime/int)
                                void *x, void *y) {
  Init();
  GetCpuFn fn = getcpu_fn_.load(std::memory_order_relaxed);
  ABSL_RAW_CHECK(fn != &InitAndGetCPU, "Init() did not set getcpu_fn_");
  return (*fn)(cpu, x, y);
}

// This function must be very fast, and may be called from very
// low level (e.g. tcmalloc). Hence I avoid things like
// GoogleOnceInit() and ::operator new.
ABSL_ATTRIBUTE_NO_SANITIZE_MEMORY
int GetCPU() {
  unsigned cpu;
  int ret_code = (*VDSOSupport::getcpu_fn_)(&cpu, nullptr, nullptr);
  return ret_code == 0 ? cpu : ret_code;
}

// We need to make sure VDSOSupport::Init() is called before
// InitGoogle() does any setuid or chroot calls.  If VDSOSupport
// is used in any global constructor, this will happen, since
// VDSOSupport's constructor calls Init.  But if not, we need to
// ensure it here, with a global constructor of our own.  This
// is an allowed exception to the normal rule against non-trivial
// global constructors.
static class VDSOInitHelper {
 public:
  VDSOInitHelper() { VDSOSupport::Init(); }
} vdso_init_helper;

}  // namespace debug_internal
}  // namespace absl

#endif  // ABSL_HAVE_VDSO_SUPPORT
// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stdlib.h>
#include <string.h>


#ifndef __has_feature
#define __has_feature(x) 0
#endif

/* Compiler-based ThreadSanitizer defines
   DYNAMIC_ANNOTATIONS_EXTERNAL_IMPL = 1
   and provides its own definitions of the functions. */

#ifndef DYNAMIC_ANNOTATIONS_EXTERNAL_IMPL
# define DYNAMIC_ANNOTATIONS_EXTERNAL_IMPL 0
#endif

/* Each function is empty and called (via a macro) only in debug mode.
   The arguments are captured by dynamic tools at runtime. */

#if DYNAMIC_ANNOTATIONS_EXTERNAL_IMPL == 0 && !defined(__native_client__)

#if __has_feature(memory_sanitizer)
#include <sanitizer/msan_interface.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

void AnnotateRWLockCreate(const char *, int,
                          const volatile void *){}
void AnnotateRWLockDestroy(const char *, int,
                           const volatile void *){}
void AnnotateRWLockAcquired(const char *, int,
                            const volatile void *, long){}
void AnnotateRWLockReleased(const char *, int,
                            const volatile void *, long){}
void AnnotateBenignRace(const char *, int,
                        const volatile void *,
                        const char *){}
void AnnotateBenignRaceSized(const char *, int,
                             const volatile void *,
                             size_t,
                             const char *) {}
void AnnotateThreadName(const char *, int,
                        const char *){}
void AnnotateIgnoreReadsBegin(const char *, int){}
void AnnotateIgnoreReadsEnd(const char *, int){}
void AnnotateIgnoreWritesBegin(const char *, int){}
void AnnotateIgnoreWritesEnd(const char *, int){}
void AnnotateEnableRaceDetection(const char *, int, int){}
void AnnotateMemoryIsInitialized(const char *, int,
                                 const volatile void *mem, size_t size) {
#if __has_feature(memory_sanitizer)
  __msan_unpoison(mem, size);
#else
  (void)mem;
  (void)size;
#endif
}

void AnnotateMemoryIsUninitialized(const char *, int,
                                   const volatile void *mem, size_t size) {
#if __has_feature(memory_sanitizer)
  __msan_allocated_memory(mem, size);
#else
  (void)mem;
  (void)size;
#endif
}

static int GetRunningOnValgrind(void) {
#ifdef RUNNING_ON_VALGRIND
  if (RUNNING_ON_VALGRIND) return 1;
#endif
  char *running_on_valgrind_str = getenv("RUNNING_ON_VALGRIND");
  if (running_on_valgrind_str) {
    return strcmp(running_on_valgrind_str, "0") != 0;
  }
  return 0;
}

/* See the comments in dynamic_annotations.h */
int RunningOnValgrind(void) {
  static volatile int running_on_valgrind = -1;
  int local_running_on_valgrind = running_on_valgrind;
  /* C doesn't have thread-safe initialization of statics, and we
     don't want to depend on pthread_once here, so hack it. */
  ANNOTATE_BENIGN_RACE(&running_on_valgrind, "safe hack");
  if (local_running_on_valgrind == -1)
    running_on_valgrind = local_running_on_valgrind = GetRunningOnValgrind();
  return local_running_on_valgrind;
}

/* See the comments in dynamic_annotations.h */
double ValgrindSlowdown(void) {
  /* Same initialization hack as in RunningOnValgrind(). */
  static volatile double slowdown = 0.0;
  double local_slowdown = slowdown;
  ANNOTATE_BENIGN_RACE(&slowdown, "safe hack");
  if (RunningOnValgrind() == 0) {
    return 1.0;
  }
  if (local_slowdown == 0.0) {
    char *env = getenv("VALGRIND_SLOWDOWN");
    slowdown = local_slowdown = env ? atof(env) : 50.0;
  }
  return local_slowdown;
}

#ifdef __cplusplus
}  // extern "C"
#endif
#endif  /* DYNAMIC_ANNOTATIONS_EXTERNAL_IMPL == 0 */
