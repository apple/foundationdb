/*
    ============
    SHA-1 in C++
    ============

    100% Public Domain.

    Original C Code
        -- Steve Reid <steve@edmweb.com>
    Small changes to fit into bglibs
        -- Bruce Guenter <bruce@untroubled.org>
    Translation to simpler C++ Code
        -- Volker Grabsch <vog@notjusthosting.com>
*/

#pragma once

#include <iostream>
#include <string>
#include <stdint.h>

class SHA1 {
public:
	SHA1();
	void update(const std::string& s);
	void update(std::istream& is);
	std::string final();

	static std::string from_string(const std::string& string);

private:
	typedef uint32_t uint32; /* just needs to be at least 32bit */
	typedef uint64_t uint64; /* just needs to be at least 64bit */

	static const unsigned int DIGEST_INTS = 5; /* number of 32bit integers per SHA1 digest */
	static const unsigned int BLOCK_INTS = 16; /* number of 32bit integers per SHA1 block */
	static const unsigned int BLOCK_BYTES = BLOCK_INTS * 4;

	uint32 digest[DIGEST_INTS];
	std::string buffer;
	uint64 transforms;

	void reset();
	void transform(uint32 block[BLOCK_BYTES]);

	static void buffer_to_block(const std::string& buffer, uint32 block[BLOCK_BYTES]);
	static void read(std::istream& is, std::string& s, int max);
};
