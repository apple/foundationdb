#include "FastAlloc.h"
#include <vector>
#include <mutex>
#include <thread>
#include <iostream>

int main() {
	constexpr auto Size = 16;
	for (int i = 0; i < 1000; ++i) {
		// Create a thread, allocate, free, and join
		auto thread = std::thread([]() {
			std::vector<void*> allocations;
			for (int i = 0; i < 1; ++i) {
				allocations.push_back(FastAllocator<Size>::allocate());
			}
			for (auto* p : allocations) {
				FastAllocator<Size>::release(p);
			}
		});
		thread.join();
	}
	std::cout << "Total Memory\t" << FastAllocator<Size>::getTotalMemory() << std::endl;
	std::cout << "High water\t" << get_high_water_mark() << std::endl;
}