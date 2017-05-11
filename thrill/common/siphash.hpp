/*******************************************************************************
 * thrill/common/siphash.hpp
 *
 * SipHash Implementations borrowed under Public Domain license from
 * https://github.com/floodyberry/siphash
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_SIPHASH_HEADER
#define THRILL_COMMON_SIPHASH_HEADER

#include <cstdint>
#include <cstdlib>

#if defined(_MSC_VER)

#include <intrin.h>

#define ROTL64(a, b) _rotl64(a, b)

#if (_MSC_VER > 1200) || defined(_mm_free)
#define __SSE2__
#endif

#else // !defined(_MSC_VER)

#define ROTL64(a, b) (((a) << (b)) | ((a) >> (64 - b)))

#endif // !defined(_MSC_VER)

#if defined(__SSE2__)
#include <emmintrin.h>
#endif

namespace thrill {
namespace common {

static inline
uint64_t siphash_plain(const uint8_t key[16], const uint8_t* m, size_t len) {

    uint64_t v0, v1, v2, v3;
    uint64_t mi, k0, k1;
    uint64_t last7;
    size_t i, blocks;

    k0 = *reinterpret_cast<const uint64_t*>(key + 0);
    k1 = *reinterpret_cast<const uint64_t*>(key + 8);
    v0 = k0 ^ 0x736f6d6570736575ull;
    v1 = k1 ^ 0x646f72616e646f6dull;
    v2 = k0 ^ 0x6c7967656e657261ull;
    v3 = k1 ^ 0x7465646279746573ull;

    last7 = (uint64_t)(len & 0xff) << 56;

#define sipcompress()    \
    v0 += v1; v2 += v3;  \
    v1 = ROTL64(v1, 13); \
    v3 = ROTL64(v3, 16); \
    v1 ^= v0; v3 ^= v2;  \
    v0 = ROTL64(v0, 32); \
    v2 += v1; v0 += v3;  \
    v1 = ROTL64(v1, 17); \
    v3 = ROTL64(v3, 21); \
    v1 ^= v2; v3 ^= v0;  \
    v2 = ROTL64(v2, 32);

    for (i = 0, blocks = (len & ~7); i < blocks; i += 8) {
        mi = *reinterpret_cast<const uint64_t*>(m + i);
        v3 ^= mi;
        sipcompress();
        sipcompress();
        v0 ^= mi;
    }

    switch (len - blocks) {
    case 7: last7 |= (uint64_t)m[i + 6] << 48;
    case 6: last7 |= (uint64_t)m[i + 5] << 40;
    case 5: last7 |= (uint64_t)m[i + 4] << 32;
    case 4: last7 |= (uint64_t)m[i + 3] << 24;
    case 3: last7 |= (uint64_t)m[i + 2] << 16;
    case 2: last7 |= (uint64_t)m[i + 1] << 8;
    case 1: last7 |= (uint64_t)m[i + 0];
    case 0:
    default:;
    }

    v3 ^= last7;
    sipcompress();
    sipcompress();
    v0 ^= last7;
    v2 ^= 0xff;
    sipcompress();
    sipcompress();
    sipcompress();
    sipcompress();

#undef sipcompress

    return v0 ^ v1 ^ v2 ^ v3;
}

/******************************************************************************/
// SSE2 vectorization

#if defined(__SSE2__)

union siphash_packedelem64 {
    uint64_t u[2];
    __m128i  v;
};

/* 0,2,1,3 */
static const siphash_packedelem64 siphash_init[2] = {
    {
        { 0x736f6d6570736575ull, 0x6c7967656e657261ull }
    },
    {
        { 0x646f72616e646f6dull, 0x7465646279746573ull }
    }
};

static const siphash_packedelem64 siphash_final = {
    { 0x0000000000000000ull, 0x00000000000000ffull }
};

static inline
uint64_t siphash_sse2(const uint8_t key[16], const uint8_t* m, size_t len) {

    __m128i k, v02, v20, v13, v11, v33, mi;
    uint64_t last7;
    uint32_t lo, hi;
    size_t i, blocks;

    k = _mm_loadu_si128((const __m128i*)(key + 0));
    v02 = siphash_init[0].v;
    v13 = siphash_init[1].v;
    v02 = _mm_xor_si128(v02, _mm_unpacklo_epi64(k, k));
    v13 = _mm_xor_si128(v13, _mm_unpackhi_epi64(k, k));

    last7 = (uint64_t)(len & 0xff) << 56;

#define sipcompress()                                                          \
    v11 = v13;                                                                 \
    v33 = _mm_shuffle_epi32(v13, _MM_SHUFFLE(1, 0, 3, 2));                     \
    v11 = _mm_or_si128(_mm_slli_epi64(v11, 13), _mm_srli_epi64(v11, 64 - 13)); \
    v02 = _mm_add_epi64(v02, v13);                                             \
    v33 = _mm_shufflelo_epi16(v33, _MM_SHUFFLE(2, 1, 0, 3));                   \
    v13 = _mm_unpacklo_epi64(v11, v33);                                        \
    v13 = _mm_xor_si128(v13, v02);                                             \
    v20 = _mm_shuffle_epi32(v02, _MM_SHUFFLE(0, 1, 3, 2));                     \
    v11 = v13;                                                                 \
    v33 = _mm_shuffle_epi32(v13, _MM_SHUFFLE(1, 0, 3, 2));                     \
    v11 = _mm_or_si128(_mm_slli_epi64(v11, 17), _mm_srli_epi64(v11, 64 - 17)); \
    v20 = _mm_add_epi64(v20, v13);                                             \
    v33 = _mm_or_si128(_mm_slli_epi64(v33, 21), _mm_srli_epi64(v33, 64 - 21)); \
    v13 = _mm_unpacklo_epi64(v11, v33);                                        \
    v02 = _mm_shuffle_epi32(v20, _MM_SHUFFLE(0, 1, 3, 2));                     \
    v13 = _mm_xor_si128(v13, v20);

    for (i = 0, blocks = (len & ~7); i < blocks; i += 8) {
        mi = _mm_loadl_epi64((const __m128i*)(m + i));
        v13 = _mm_xor_si128(v13, _mm_slli_si128(mi, 8));
        sipcompress();
        sipcompress();
        v02 = _mm_xor_si128(v02, mi);
    }

    switch (len - blocks) {
    case 7: last7 |= (uint64_t)m[i + 6] << 48;
    case 6: last7 |= (uint64_t)m[i + 5] << 40;
    case 5: last7 |= (uint64_t)m[i + 4] << 32;
    case 4: last7 |= (uint64_t)m[i + 3] << 24;
    case 3: last7 |= (uint64_t)m[i + 2] << 16;
    case 2: last7 |= (uint64_t)m[i + 1] << 8;
    case 1: last7 |= (uint64_t)m[i + 0];
    case 0:
    default:;
    }

    mi = _mm_unpacklo_epi32(
        _mm_cvtsi32_si128((uint32_t)last7), _mm_cvtsi32_si128((uint32_t)(last7 >> 32)));
    v13 = _mm_xor_si128(v13, _mm_slli_si128(mi, 8));
    sipcompress();
    sipcompress();
    v02 = _mm_xor_si128(v02, mi);
    v02 = _mm_xor_si128(v02, siphash_final.v);
    sipcompress();
    sipcompress();
    sipcompress();
    sipcompress();

    v02 = _mm_xor_si128(v02, v13);
    v02 = _mm_xor_si128(v02, _mm_shuffle_epi32(v02, _MM_SHUFFLE(1, 0, 3, 2)));
    lo = _mm_cvtsi128_si32(v02);
    hi = _mm_cvtsi128_si32(_mm_srli_si128(v02, 4));

#undef sipcompress

    return ((uint64_t)hi << 32) | lo;
}

#endif  // defined(__SSE2__)

/******************************************************************************/
// Switch between available implementations

static inline
uint64_t siphash(const uint8_t key[16], const uint8_t* m, size_t len) {
#if defined(__SSE2__)
    return siphash_sse2(key, m, len);
#else
    return siphash_plain(key, m, len);
#endif
}

#undef ROTL64

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_SIPHASH_HEADER

/******************************************************************************/
