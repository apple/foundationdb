#include "flow/UnitTest.h"
#include "flow/ArenaAllocator.h"
#include "flow/ArenaString.h"

TEST_CASE("/flow/ArenaString") {
	Arena arena;
	ArenaAllocator<char> alloc(arena);
	{
		ArenaString s("1", alloc);
		auto shortStrBuf = s.data();
		s.assign(100, '1');
		auto longStrBuf = s.data();
		ASSERT_NE(shortStrBuf, longStrBuf);
		ArenaString t = s;
		auto copiedStrBuf = t.data();
		ASSERT_NE(copiedStrBuf, longStrBuf);
	}
	{
		ArenaString s(alloc);
		s.assign(100, 'a');
		ArenaString t(100, 'a', alloc);
		ASSERT(s == t);
	}
	{
		// Default construction of string does not specify an allocator, and Arena by extension.
		// Any modification that requires allocation will throw bad_allocator() when assigning beyond
		// short-string-optimized length.
		ArenaString s;
		bool hit = false;
		try {
			s.assign(100, 'a');
		} catch (Error& e) {
			hit = true;
			ASSERT_EQ(e.code(), error_code_bad_allocator);
		}
		ASSERT(hit);
	}
	{
		// string_view may be used to bridge strings with different allocators
		ArenaString s(100, 'a', alloc);
		std::string_view sv(s);
		std::string s2(sv);
		std::string_view sv2(s2);
		ASSERT(sv == sv2);
	}
	return Void();
}

void forceLinkArenaStringTests() {}
