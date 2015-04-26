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
    std::size_t operator()(const Key& k) const
    {
        return std::hash<std::string>()(k.first);
    }
};

// add to buffer for node id
// get largest item (buffer) / need later, use random select first
// get items in buffer for node id

template<typename K, typename V>
class HashTable
{

static const bool debug = true;

public:

    void insert(std::pair<K, V> &p, std::function<V (V, V)> f_reduce)
    {
        // TODO: improve
        auto it = _hash_m.find({ p.first });
        if (it == _hash_m.end()) {
            _hash_m.insert({ { p.first }, p.second });
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
                    << it->first.first
                    << ":"
                    << it->second
                    << "] ";

            }
            std::cout << "\n";
        }
        return;
    }

private:
    std::unordered_map<c7a::engine::Key, V, c7a::engine::KeyHasher> _hash_m;
};

}
}

#endif //C7A_HASH_TABLE_HPP