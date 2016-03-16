/*******************************************************************************
 * thrill/common/lru_cache.hpp
 *
 * Borrowed from https://github.com/lamerman/cpp-lru-cache by Alexander
 * Ponomarev under BSD license and modified for Thrill's block pool.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2013 Alexander Ponomarev
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_LRU_CACHE_HEADER
#define THRILL_COMMON_LRU_CACHE_HEADER

#include <cassert>
#include <cstddef>
#include <functional>
#include <list>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace thrill {
namespace common {

/*!
 * This is an expected O(1) LRU cache which contains a set of (key,value)
 * elements. Elements can be put() into LRU cache, and later retrieves using
 * get(). Insertion and retrieval will remark the elements as most recently
 * used, pushing all other back in priority. The LRU cache itself does not limit
 * the number of items, because it has no eviction mechanism. Instead, the user
 * program must check size() after an insert and may extract the least recently
 * used element.
 */
template <typename Key, typename Value>
class LruCache
{
public:
    using KeyValuePair = typename std::pair<Key, Value>;

    using List = typename std::list<KeyValuePair>;
    using ListIterator = typename List::iterator;

    using Map = typename std::unordered_map<Key, ListIterator>;

    //! put or replace/touch item in LRU cache
    void put(const Key& key, const Value& value) {
        // first try to find an existing key
        typename Map::iterator it = map_.find(key);
        if (it != map_.end()) {
            list_.erase(it->second);
            map_.erase(it);
        }

        // insert key into linked list at the front (most recently used)
        list_.push_front(KeyValuePair(key, value));
        // store iterator to linked list entry in map
        map_[key] = list_.begin();
    }

    //! get and touch value from LRU cache for key.
    const Value & get(const Key& key) {
        typename Map::iterator it = map_.find(key);
        if (it == map_.end()) {
            throw std::range_error("There is no such key in cache");
        }
        else {
            list_.splice(list_.begin(), list_, it->second);
            return it->second->second;
        }
    }

    //! test if key exists in LRU cache
    bool exists(const Key& key) const {
        return map_.find(key) != map_.end();
    }

    //! return number of items in LRU cache
    size_t size() const {
        return map_.size();
    }

    //! return the least recently used key value pair
    KeyValuePair pop() {
        assert(size());
        typename List::iterator last = list_.end();
        --last;
        KeyValuePair out = *last;
        map_.erase(last->first);
        list_.pop_back();
        return out;
    }

private:
    //! list of entries in least-recently used order.
    List list_;
    //! map for accelerated access to keys
    Map map_;
};

/*!
 * This is an expected O(1) LRU cache which contains a set of key-only
 * elements. Elements can be put() into LRU cache, and tested for existence
 * using exists(). Insertion and touch() will remark the elements as most
 * recently used, pushing all other back in priority. The LRU cache itself does
 * not limit the number of items, because it has no eviction mechanism. Instead,
 * the user program must check size() after an insert and may extract the least
 * recently used element.
 */
template <typename Key, typename Alloc = std::allocator<Key> >
class LruCacheSet
{
public:
    using List = typename std::list<Key, Alloc>;
    using ListIterator = typename List::iterator;

    using Map = typename std::unordered_map<
              Key, ListIterator, std::hash<Key>, std::equal_to<Key>,
              typename Alloc::template rebind<
                  std::pair<const Key, ListIterator> >::other>;

    explicit LruCacheSet(const Alloc& alloc = Alloc())
        : list_(alloc), map_(alloc) { }

    //! put or replace/touch item in LRU cache
    void put(const Key& key) {
        // first try to find an existing key
        typename Map::iterator it = map_.find(key);
        if (it != map_.end()) {
            list_.erase(it->second);
            map_.erase(it);
        }

        // insert key into linked list at the front (most recently used)
        list_.push_front(key);
        // store iterator to linked list entry in map
        map_[key] = list_.begin();
    }

    //! touch value from LRU cache for key.
    void touch(const Key& key) {
        typename Map::iterator it = map_.find(key);
        if (it == map_.end()) {
            throw std::range_error("There is no such key in cache");
        }
        else {
            list_.splice(list_.begin(), list_, it->second);
        }
    }

    //! remove key from LRU cache
    void erase(const Key& key) {
        typename Map::iterator it = map_.find(key);
        if (it == map_.end()) {
            throw std::range_error("There is no such key in cache");
        }
        else {
            list_.erase(it->second);
            map_.erase(it);
        }
    }

    //! test if key exists in LRU cache
    bool exists(const Key& key) const {
        return map_.find(key) != map_.end();
    }

    //! return number of items in LRU cache
    size_t size() const {
        return map_.size();
    }

    //! return the least recently used key value pair
    Key pop() {
        assert(size());
        typename List::iterator last = list_.end();
        --last;
        Key out = *last;
        map_.erase(out);
        list_.pop_back();
        return out;
    }

private:
    //! list of entries in least-recently used order.
    List list_;
    //! map for accelerated access to keys
    Map map_;
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_LRU_CACHE_HEADER

/******************************************************************************/
