/*
 * File:   Crc32.hpp
 * Author: vantonov
 * Owners: philippu/mpilman
 *
 * Copyright (c) 2013-2014 Snowflake Computing
 *
 * Created on March 5, 2013, 7:59 PM
 */

#include "Crc32.h"

# define SF_CRC32_DI(s, d) __builtin_ia32_crc32di((s), (d))
# define SF_CRC32_HI(s, d) __builtin_ia32_crc32hi((s), (d))
# define SF_CRC32_QI(s, d) __builtin_ia32_crc32qi((s), (d))
# define SF_CRC32_SI(s, d) __builtin_ia32_crc32si((s), (d))

/**
 * Compute checksum over a contiguous block
 *
 * @param data
 *   pointer to the contiguous data
 * @param len
 *   length of data, bytes
 */
void
Crc32::block(const void* data, uint32_t len)
{
    auto d = reinterpret_cast<const uint8_t*>(data);

    // NB: THIS CODE ASSUMES 64-BIT CPU
    uint32_t s = sum_;

    if (len & ~uint64_t(07)) {
        uint32_t l = len >> 3;
        uint64_t s8 = s;
        do {
            s8 = SF_CRC32_DI(s8, *reinterpret_cast<const uint64_t*>(d));
            d += 8;
        } while (--l);
        s = s8;
    }

    if (len & 04) {
        s = SF_CRC32_SI(s, *reinterpret_cast<const uint32_t*>(d));
        d += 4;
    }

    if (len & 02) {
        s = SF_CRC32_HI(s, *reinterpret_cast<const uint16_t*>(d));
        d += 2;
    }

    if (len & 01)
        s = SF_CRC32_QI(s, *d);
    sum_ = s;
}
