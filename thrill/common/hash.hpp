/*******************************************************************************
 * thrill/common/hash.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_HASH_HEADER
#define THRILL_COMMON_HASH_HEADER

#include <cassert>
#include <cstdint>
#include <type_traits>
#include <immintrin.h>

#include "config.hpp"

namespace thrill {
namespace common {

/*
 * A reinterpret_cast that doesn't violate the strict aliasing rule.  Zero
 * overhead with any reasonable compiler (g++ -O1 or higher, clang++ -O2 or
 * higher)
 */
template<typename T, typename F>
struct alias_cast_t {
    static_assert(sizeof(T) == sizeof(F),
                  "Cannot cast types of different sizes");
    union {
        F raw;
        T data;
    };
};

template<typename T, typename F>
T alias_cast(F raw_data) {
    alias_cast_t<T, F> ac;
    ac.raw = raw_data;
    return ac.data;
}


/**
 * A CRC32C hasher using SSE4.2 intrinsics
 */
template <typename ValueType>
struct hash_crc32_intel {
    // Hash data with Intel's CRC32C instructions
    // Copyright 2008,2009,2010 Massachusetts Institute of Technology.
    uint32_t hash_bytes(const void* data, size_t length, uint32_t crc = 0xffffffff) {
        const char* p_buf = (const char*) data;
        // The 64-bit crc32 instruction returns a 64-bit value (even though a
        // CRC32 hash has - well - 32 bits. Whatever.
        uint64_t crc_carry = crc;
        for (size_t i = 0; i < length / sizeof(uint64_t); i++) {
            crc_carry = _mm_crc32_u64(crc_carry, *(uint64_t*) p_buf);
            p_buf += sizeof(uint64_t);
        }
        crc = (uint32_t) crc_carry; // discard the rest
        length &= 7; // remaining length

        // ugly switch statement, faster than a loop-based version
        switch (length) {
        case 7:
            crc = _mm_crc32_u8(crc, *p_buf++);
        case 6:
            crc = _mm_crc32_u16(crc, *(uint16_t*) p_buf);
            p_buf += 2;
            // case 5 is below: 4 + 1
        case 4:
            crc = _mm_crc32_u32(crc, *(uint32_t*) p_buf);
            break;
        case 3:
            crc = _mm_crc32_u8(crc, *p_buf++);
        case 2:
            crc = _mm_crc32_u16(crc, *(uint16_t*) p_buf);
            break;
        case 5:
            crc = _mm_crc32_u32(crc, *(uint32_t*) p_buf);
            p_buf += 4;
        case 1:
            crc = _mm_crc32_u8(crc, *p_buf);
            break;
        case 0:
            break;
        default: // wat
            assert(false);
        }
        return crc;
    }

    // Hash large or oddly-sized types
    template <typename T = ValueType>
    uint32_t operator()(const T& val, uint32_t crc = 0xffffffff,
                        typename std::enable_if<
                        (sizeof(T) > 8) || (sizeof(T) > 4 && sizeof(T) < 8) || sizeof(T) == 3
                        >::type* = 0) {
        return hash_bytes((void*)&val, sizeof(T), crc);
    }

    // Specializations for {8,4,2,1}-byte types avoiding unnecessary branches
    template <typename T = ValueType>
    uint32_t operator()(const T& val, uint32_t crc = 0xffffffff,
                        typename std::enable_if<sizeof(T) == 8>::type* = 0) {
        // For Intel reasons, the 64-bit version returns a 64-bit int
        uint64_t res = _mm_crc32_u64(crc, *alias_cast<const uint64_t*>(&val));
        return static_cast<uint32_t>(res);
    }

    template <typename T = ValueType>
    uint32_t operator()(const T& val, uint32_t crc = 0xffffffff,
                        typename std::enable_if<sizeof(T) == 4>::type* = 0) {
        return _mm_crc32_u32(crc, *alias_cast<const uint32_t*>(&val));
    }

    template <typename T = ValueType>
    uint32_t operator()(const T& val, uint32_t crc = 0xffffffff,
                        typename std::enable_if<sizeof(T) == 2>::type* = 0) {
        return _mm_crc32_u16(crc, *alias_cast<const uint16_t*>(&val));
    }

    template <typename T = ValueType>
    uint32_t operator()(const T& val, const uint64_t crc = 0xffffffff,
                        typename std::enable_if<sizeof(T) == 1>::type* = 0) {
        return _mm_crc32_u8(crc, *alias_cast<const uint8_t*>(&val));
    }
};


/** CRC32C lookup tables */
struct hash_crc32_tables {
static const uint32_t crc_tableil8_o32[256];
static const uint32_t crc_tableil8_o40[256];
static const uint32_t crc_tableil8_o48[256];
static const uint32_t crc_tableil8_o56[256];
static const uint32_t crc_tableil8_o64[256];
static const uint32_t crc_tableil8_o72[256];
static const uint32_t crc_tableil8_o80[256];
static const uint32_t crc_tableil8_o88[256];
};

/**
 * Fallback CRC32C implementation in software.
 */
template <typename ValueType>
struct hash_crc32_fallback {
    // CRC32C, adapted from Evan Jones' BSD-licensed implementation at
    // http://www.evanjones.ca/crc32c.html
    uint32_t slicing_by_8(uint32_t crc, const void* data, size_t length) {
        const char* p_buf = (const char*) data;

        // Handle leading misaligned bytes
        size_t initial_bytes = (sizeof(int32_t) - (intptr_t)p_buf) & (sizeof(int32_t) - 1);
        if (length < initial_bytes) initial_bytes = length;
        for (size_t li = 0; li < initial_bytes; li++) {
            crc = hash_crc32_tables::crc_tableil8_o32[(crc ^ *p_buf++) & 0x000000FF] ^ (crc >> 8);
        }

        length -= initial_bytes;
        size_t running_length = length & ~(sizeof(uint64_t) - 1);
        size_t end_bytes = length - running_length;

        for (size_t li = 0; li < running_length/8; li++) {
            crc ^= *(uint32_t*) p_buf;
            p_buf += 4;
            uint32_t term1 = hash_crc32_tables::crc_tableil8_o88[crc & 0x000000FF] ^
                hash_crc32_tables::crc_tableil8_o80[(crc >> 8) & 0x000000FF];
            uint32_t term2 = crc >> 16;
            crc = term1 ^
                hash_crc32_tables::crc_tableil8_o72[term2 & 0x000000FF] ^
                hash_crc32_tables::crc_tableil8_o64[(term2 >> 8) & 0x000000FF];
            term1 = hash_crc32_tables::crc_tableil8_o56[(*(uint32_t *)p_buf) & 0x000000FF] ^
                hash_crc32_tables::crc_tableil8_o48[((*(uint32_t *)p_buf) >> 8) & 0x000000FF];

            term2 = (*(uint32_t *)p_buf) >> 16;
            crc = crc ^ term1 ^
                hash_crc32_tables::crc_tableil8_o40[term2  & 0x000000FF] ^
                hash_crc32_tables::crc_tableil8_o32[(term2 >> 8) & 0x000000FF];
            p_buf += 4;
        }

        for (size_t li=0; li < end_bytes; li++) {
            crc = hash_crc32_tables::crc_tableil8_o32[(crc ^ *p_buf++) & 0x000000FF] ^ (crc >> 8);
        }

        return crc;
    }

    uint32_t operator()(const ValueType& val, const uint32_t crc = 0xffffffff) {
        return slicing_by_8(crc, (void*)&val, sizeof(ValueType));
    }
};


// If SSE4.2 is available, use the hardware implementation, which is roughly
// four to five times faster than the software fallback (less for small sizes).
#ifdef THRILL_HAVE_SSE4_2
template <typename T>
using hash_crc32 = hash_crc32_intel<T>;
#else
template <typename T>
using hash_crc32 = hash_crc32_fallback<T>;
#endif


} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_HASH_HEADER

/******************************************************************************/
