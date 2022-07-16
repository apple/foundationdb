#include <stdint.h>
#include <stdio.h>

namespace {
FILE* out = nullptr;
FILE* in = nullptr;
void loop_forever() {
	// Try to convince the optimizer not to optimize away this loop
	static volatile uint64_t x = 0;
	for (;;) {
		++x;
	}
}
} // namespace

// This callback is inserted by the compiler as a module constructor
// into every DSO. 'start' and 'stop' correspond to the
// beginning and end of the section with the guards for the entire
// binary (executable or DSO). The callback will be called at least
// once per DSO and may be called multiple times with the same parameters.
extern "C" void __sanitizer_cov_trace_pc_guard_init(uint32_t* start, uint32_t* stop) {
	in = fopen("in.bin", "r");
	out = fopen("out.bin", "w");
	static uint64_t N; // Counter for the guards.
	if (start == stop || *start)
		return; // Initialize only once.
	for (uint32_t* x = start; x < stop; x++) {
		*x = ++N; // Guards should start from 1.
	}
}

// This callback is inserted by the compiler on every edge in the
// control flow (some optimizations apply).
// Typically, the compiler will emit the code like this:
//    if(*guard)
//      __sanitizer_cov_trace_pc_guard(guard);
// But for large functions it will emit a simple call:
//    __sanitizer_cov_trace_pc_guard(guard);
extern "C" void __sanitizer_cov_trace_pc_guard(uint32_t* guard) {
	if (!guard) {
		return;
	}
	fwrite(guard, sizeof(*guard), 1, out);
	if (in) {
		uint32_t theirs;
		auto read = fread(&theirs, sizeof(theirs), 1, in);
		if (read != 1 || *guard != theirs) {
			printf("Non-determinism detected\n");
			loop_forever();
		}
	}
}