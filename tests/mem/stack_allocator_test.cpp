/*******************************************************************************
 * tests/mem/stack_allocator_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/mem/stack_allocator.hpp>

#include <gtest/gtest.h>
#include <thrill/common/logger.hpp>

#include <deque>
#include <string>
#include <vector>

using namespace thrill;

TEST(StackAllocator, Test1) {
    static constexpr bool debug = false;

    using CharAlloc = mem::StackAllocator<char, 128>;
    using IntAlloc = mem::StackAllocator<int, 128>;

    using s_string = std::basic_string<char, std::char_traits<char>, CharAlloc>;

    LOG << "string";
    {
        mem::Arena<128> arena;
        const char* text = "abcdefghijklmnopqrstuvwxyz";
        {
            s_string str(text, CharAlloc(arena));
            ASSERT_LE(27u, arena.used());

            str = s_string("abc", CharAlloc(arena));
            ASSERT_EQ("abc", str);
        }
    }
    LOG << "vector";
    {
        mem::Arena<128> arena;
        std::vector<int, IntAlloc> my_vector {
            IntAlloc(arena)
        };

        // push more data than in our arena
        for (int i = 0; i < 100; ++i) {
            my_vector.push_back(i);
        }

        for (int i = 0; i < 100; ++i) {
            ASSERT_EQ(i, my_vector[i]);
        }
    }
    LOG << "deque";
    {
        mem::Arena<128> arena;
        std::deque<int, IntAlloc> my_deque {
            IntAlloc(arena)
        };

        // push more data than in our arena
        for (int i = 0; i < 100; ++i) {
            my_deque.push_back(i);
        }

        for (int i = 0; i < 100; ++i) {
            ASSERT_EQ(i, my_deque[i]);
        }
    }
}

namespace thrill {
namespace mem {

// forced instantiations
template class Arena<128>;
template class StackAllocator<int, 128>;

} // namespace mem
} // namespace thrill

/******************************************************************************/
