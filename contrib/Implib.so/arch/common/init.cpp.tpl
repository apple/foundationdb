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
#include <mutex>

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

static void *lib_handle;

static void *load_library() {
  if(lib_handle)
    return lib_handle;

  // TODO: dlopen and users callback must be protected w/ critical section (to avoid dlopening lib twice)
#if CALL_USER_CALLBACK
  extern void *$dlopen_callback(const char *lib_name);
  lib_handle = $dlopen_callback("$load_name");
  CHECK(lib_handle, "callback '$dlopen_callback' failed to load library");
#else
  lib_handle = dlopen("$load_name", RTLD_LAZY | RTLD_GLOBAL);
  CHECK(lib_handle, "failed to load library: %s", dlerror());
#endif

  return lib_handle;
}

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

// Load library and resolve all symbols
static void load_and_resolve(void) {
  static std::mutex load_mutex;
  static int is_loaded = false;

  std::unique_lock<std::mutex> lock(load_mutex);
  if (is_loaded)
    return;

  void *h = 0;
  h = load_library();

  size_t i;
  for(i = 0; i + 1 < sizeof(sym_names) / sizeof(sym_names[0]); ++i)
    // Resolving some of the symbols may fail. We ignore it, because if we are loading 
    // a library of an older version it may lack certain functions
    _${lib_suffix}_tramp_table[i] = dlsym(h, sym_names[i]);

  is_loaded = true;
}

// The function is called if the table entry for the symbol is not set.
// In that case we load the library and try to resolve all symbols if that was not done yet.
// If the table entry is still missing, then the symbol is not available in the loaded library,
// which is a fatal error on which we immediately exit the process.
void _${lib_suffix}_tramp_resolve(int i) {
  assert((unsigned)i + 1 < sizeof(sym_names) / sizeof(sym_names[0]));
  load_and_resolve();
  CHECK(_${lib_suffix}_tramp_table[i], "failed to resolve symbol '%s'", sym_names[i]);
}

#ifdef __cplusplus
}  // extern "C"
#endif
