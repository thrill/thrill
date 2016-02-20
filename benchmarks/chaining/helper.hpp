/*******************************************************************************
 * benchmarks/chaining/helper.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_BENCHMARKS_CHAINING_HELPER_HEADER
#define THRILL_BENCHMARKS_CHAINING_HELPER_HEADER

#include <cstdlib>

struct KeyValue {
    size_t key;
    size_t value;
};

#define map                                               \
    Map([](const KeyValue& elem) {                        \
            return KeyValue { elem.key, elem.value + 1 }; \
        })

#define map10                                           \
    Map([](const KeyValue& elem) {                      \
            KeyValue kv1 { elem.key, elem.value + 1 };  \
            KeyValue kv2 { kv1.key, kv1.value + 1 };    \
            KeyValue kv3 { kv2.key, kv2.value + 1 };    \
            KeyValue kv4 { kv3.key, kv3.value + 1 };    \
            KeyValue kv5 { kv4.key, kv4.value + 1 };    \
            KeyValue kv6 { kv5.key, kv5.value + 1 };    \
            KeyValue kv7 { kv6.key, kv6.value + 1 };    \
            KeyValue kv8 { kv7.key, kv7.value + 1 };    \
            KeyValue kv9 { kv8.key, kv8.value + 1 };    \
            return KeyValue { kv9.key, kv9.value + 1 }; \
        })

#endif // !THRILL_BENCHMARKS_CHAINING_HELPER_HEADER

/******************************************************************************/
