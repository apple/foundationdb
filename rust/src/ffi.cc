#include "ffi.h"

std::unique_ptr<RegisterWorkerRequest> hello_world(Slice<const uint8_t> s) {
    for (auto c : s) {
        printf(" %c\n", c);
    }
    return std::make_unique<RegisterWorkerRequest>();
}
