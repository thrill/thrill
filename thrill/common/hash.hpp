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
template<typename To, typename From>
struct alias_cast_t {
    static_assert(sizeof(To) == sizeof(From),
                  "Cannot cast types of different sizes");
    union {
        From * in;
        To * out;
    };
};

template<typename To, typename From>
To & alias_cast(From & raw_data) {
    alias_cast_t<To, From> ac;
    ac.in = &raw_data;
    return *ac.out;
}

template<typename To, typename From>
const To & alias_cast(const From & raw_data) {
    alias_cast_t<const To, const From> ac;
    ac.in = &raw_data;
    return *ac.out;
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
            crc_carry = _mm_crc32_u64(crc_carry, *(const uint64_t*) p_buf);
            p_buf += sizeof(uint64_t);
        }
        crc = (uint32_t) crc_carry; // discard the rest
        length &= 7; // remaining length

        // ugly switch statement, faster than a loop-based version
        switch (length) {
        case 7:
            crc = _mm_crc32_u8(crc, *p_buf++);
        case 6:
            crc = _mm_crc32_u16(crc, *(const uint16_t*) p_buf);
            p_buf += 2;
            // case 5 is below: 4 + 1
        case 4:
            crc = _mm_crc32_u32(crc, *(const uint32_t*) p_buf);
            break;
        case 3:
            crc = _mm_crc32_u8(crc, *p_buf++);
        case 2:
            crc = _mm_crc32_u16(crc, *(const uint16_t*) p_buf);
            break;
        case 5:
            crc = _mm_crc32_u32(crc, *(const uint32_t*) p_buf);
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
        return hash_bytes((const void*)&val, sizeof(T), crc);
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

// CRC32C, adapted from Evan Jones' BSD-licensed implementation at
// http://www.evanjones.ca/crc32c.html
uint32_t crc32_slicing_by_8(uint32_t crc, const void* data, size_t length);

/**
 * Fallback CRC32C implementation in software.
 */
template <typename ValueType>
struct hash_crc32_fallback {
    uint32_t operator()(const ValueType& val, const uint32_t crc = 0xffffffff) {
        return crc32_slicing_by_8(crc, (const void*)&val, sizeof(ValueType));
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
