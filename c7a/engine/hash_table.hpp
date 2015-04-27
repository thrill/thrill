//
// Created by Matthias Stumpp on 26/04/15.
//

#ifndef C7A_HASH_TABLE_HPP
#define C7A_HASH_TABLE_HPP

#include <map>
#include <iostream>
#include <c7a/common/logger.hpp>

namespace c7a {
namespace engine {

struct Key
{
    std::string first;

    bool operator==(const Key &other) const
    {
        return (first == other.first);
    }
};

struct KeyHasher
{
    std::size_t operator()(const std::string& k) const
    {
        return std::hash<std::string>()(k);
    }
};

// add to buffer for node id
// get largest item (buffer) / need later, use random select first
// get items in buffer for node id

typedef unsigned long size_type;

template<typename K, typename V>
class HashTable
{

static const bool debug = true;

public:

    // TODO: should be private, actually extend unordered_map,
    std::unordered_map<K, V> _hash_m;

    // TODO: here i am just reimplementing some methods which is not good,
    // just want to hide some which are not safe to use, yet
    void insert(std::pair<K, V> &p, std::function<V (V, V)> f_reduce)
    {
        // TODO: improve
        auto it = _hash_m.find( p.first );
        if (it == _hash_m.end()) {
            _hash_m.insert({ p.first , p.second });
        } else {
            (*it).second = f_reduce((*it).second, p.second);
        }
    }

    void print() {
        unsigned n = _hash_m.bucket_count();

        LOG << "bucket size: "
            << n;

        for (unsigned i = 0; i < n; ++i) {

            LOG << "bucket #"
                << i
                << " of size "
                << _hash_m.bucket_size(i)
                << " contains: ";

            for (auto it = _hash_m.begin(i); it != _hash_m.end(i); ++it) {

                LOG << "["
                    << it->first
                    << ":"
                    << it->second
                    << "] ";

            }
            std::cout << "\n";
        }
        return;
    }
};

}
}

#endif //C7A_HASH_TABLE_HPP