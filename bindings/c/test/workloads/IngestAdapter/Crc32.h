/*
 * File:   Crc32.hpp
 * Author: vantonov
 * Owners: avg philippu
 *
 * Copyright (c) 2013-2014 Snowflake Computing
 *
 * Created on March 5, 2013, 7:59 PM
 */

#pragma once
#ifndef CRC32_HPP
#define CRC32_HPP

#include <cstdint>
#include <type_traits>

/**
 * Class Crc32 implements the Intel hardware-assisted CRC-32C computation
 * (Using Castagnioli GF(2) generator polynomial 0x1EDC6F41).
 *
 * The class instance contains the CRC-32 state, which can be used to
 * compute CRC-32 over disjoint memory blocks.
 */
class Crc32 final
{
    // The default init vector
    static constexpr uint32_t INIT_VECTOR = 0xFFFFFFFF;

public:
    /**
     * Constructor
     */
    constexpr Crc32()
      : sum_(INIT_VECTOR)
    {
    }

    /**
     * Reset the state of the Crc32 digest
     *
     * @param init_vec
     *   Initializer vector, if non-standard value is needed
     */
    void reset(uint32_t init_vec = INIT_VECTOR) { sum_ = init_vec; }

    /**
     * Compute checksum over a contiguous block
     *
     * @param data
     *   pointer to the contiguous data
     * @param len
     *   length of data, bytes
     */
    void block(const void* data, uint32_t len);

    /**
     * Get the final checksum
     */
    uint32_t sum() const { return sum_; }

    /**
     * Simplified interface to compute checksum over a single
     * contiguous block with default init vector
     *
     * @param data
     *   pointer to the contiguous data
     * @param len
     *   length of data, bytes
     * @return
     *   CRC-32 checksum
     */
    static uint32_t sum(const void* data, uint32_t len)
    {
        Crc32 s;

        s.block(data, len);
        return s.sum();
    }

    template <class T>
    typename std::enable_if<std::is_integral<T>::value, Crc32&>::type operator&(
        T t)
    {
        block(&t, sizeof(t));
        return *this;
    }

private:
    uint32_t sum_; // Currently accumulated CRC-32 checksum
};

#endif /* CRC32_HPP */
