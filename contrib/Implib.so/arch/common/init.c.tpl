/*
 * Copyright 2018-2020 Yury Gribov
 *
 * The MIT License (MIT)
 *
 * Use of this source code is governed by MIT license that can be
 * found in the LICENSE.txt file.
 */

#include <dlfcn.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

// Sanity check for ARM to avoid puzzling runtime crashes
#ifdef __arm__
# if defined __thumb__ && ! defined __THUMB_INTERWORK__
#   error "ARM trampolines need -mthumb-interwork to work in Thumb mode"
# endif
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define CHECK(cond, fmt, ...) do { \
    if(!(cond)) { \
      fprintf(stderr, "implib-gen: $load_name: " fmt "\n", ##__VA_ARGS__); \
      abort(); \
    } \
  } while(0)

#define CALL_USER_CALLBACK $has_dlopen_callback
#define NO_DLOPEN $no_dlopen
#define LAZY_LOAD $lazy_load

static void *lib_handle;
static int is_lib_loading;

static void *load_library() {
  if(lib_handle)
    return lib_handle;

  is_lib_loading = 1;

  // TODO: dlopen and users callback must be protected w/ critical section (to avoid dlopening lib twice)
#if NO_DLOPEN
  CHECK(0, "internal error"); // We shouldn't get here
#elif CALL_USER_CALLBACK
  extern void *$dlopen_callback(const char *lib_name);
  lib_handle = $dlopen_callback("$load_name");
  CHECK(lib_handle, "callback '$dlopen_callback' failed to load library");
#else
  lib_handle = dlopen("$load_name", RTLD_LAZY | RTLD_GLOBAL);
  CHECK(lib_handle, "failed to load library: %s", dlerror());
#endif

  is_lib_loading = 0;

  return lib_handle;
}

#if ! NO_DLOPEN && ! LAZY_LOAD
static void __attribute__((constructor)) load_lib() {
  load_library();
}
#endif

static void __attribute__((destructor)) unload_lib() {
  if(lib_handle)
    dlclose(lib_handle);
}

// TODO: convert to single 0-separated string
static const char *const sym_names[] = {
  $sym_names
  0
};

extern void *_${lib_suffix}_tramp_table[];

// Can be sped up by manually parsing library symtab...
void _${lib_suffix}_tramp_resolve(int i) {
  assert((unsigned)i + 1 < sizeof(sym_names) / sizeof(sym_names[0]));

  CHECK(!is_lib_loading, "library function '%s' called during library load", sym_names[i]);

  void *h = 0;
#if NO_DLOPEN
  // FIXME: instead of RTLD_NEXT we should search for loaded lib_handle
  // as in https://github.com/jethrogb/ssltrace/blob/bf17c150a7/ssltrace.cpp#L74-L112
  h = RTLD_NEXT;
#elif LAZY_LOAD
  h = load_library();
#else
  h = lib_handle;
  CHECK(h, "failed to resolve symbol '%s', library failed to load", sym_names[i]);
#endif

  // Dlsym is thread-safe so don't need to protect it.
  _${lib_suffix}_tramp_table[i] = dlsym(h, sym_names[i]);
  CHECK(_${lib_suffix}_tramp_table[i], "failed to resolve symbol '%s'", sym_names[i]);
}

// Helper for user to resolve all symbols
void _${lib_suffix}_tramp_resolve_all(void) {
  size_t i;
  for(i = 0; i + 1 < sizeof(sym_names) / sizeof(sym_names[0]); ++i)
    _${lib_suffix}_tramp_resolve(i);
}

#ifdef __cplusplus
}  // extern "C"
#endif
