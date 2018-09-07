/*******************************************************************************
 * misc/memprofile2stats.cpp
 *
 * Unfinished tool to aggregate malloc profile statistics to find malloc()
 * hotspots in Thrill.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <algorithm>
#include <iostream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

struct Entry {
    //! address of caller
    void   * ptr;
    //! depth in stack
    size_t depth;
    //! bytes allocated
    size_t size;

    bool operator == (const Entry& b) const {
        return ptr == b.ptr && depth == b.depth && size == b.size;
    }
};

using EntrySize = std::pair<Entry, size_t>;

bool operator < (const EntrySize& a, const EntrySize& b) {
    return std::tie(a.second, a.first.size) < std::tie(b.second, b.first.size);
}

struct EntryHasher {
    size_t operator () (const Entry& e) const noexcept {
        return std::hash<void*>()(e.ptr)
               ^ std::hash<size_t>()(e.depth) ^ std::hash<size_t>()(e.size);
    }
};

//! map to count occurrences
std::unordered_map<Entry, size_t, EntryHasher> count_map;

int main() {
    std::string line;

    while (std::getline(std::cin, line))
    {
        size_t size;
        void* addrlist[16];

        int r = sscanf(
            line.data(),
            "malloc_tracker ### profile "
            "%zu %p %p %p %p %p %p %p %p %p %p %p %p %p %p %p %p\n",
            &size,
            &addrlist[0], &addrlist[1], &addrlist[2], &addrlist[3],
            &addrlist[4], &addrlist[5], &addrlist[6], &addrlist[7],
            &addrlist[8], &addrlist[9], &addrlist[10], &addrlist[11],
            &addrlist[12], &addrlist[13], &addrlist[14], &addrlist[15]);

        if (r != 17) continue;

        for (size_t i = 0; i < 16; ++i)
        {
            if (!addrlist[i]) break;
            count_map[Entry { addrlist[i], i, size }]++;
        }
    }

    std::vector<EntrySize> count_vec(count_map.begin(), count_map.end());

    std::sort(count_vec.begin(), count_vec.end());

    for (size_t i = count_vec.size() - 10000; i < count_vec.size(); ++i)
    {
        const EntrySize& es = count_vec[i];

        std::cout << es.second << '\t'
                  << es.first.size << '\t'
                  << es.first.ptr << '\t'
                  << es.first.depth << '\n';
    }

    return 0;
}

/******************************************************************************/
