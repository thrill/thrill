//
// Created by Matthias Stumpp on 26/04/15.
//

#ifndef C7A_HASH_TABLE_HPP
#define C7A_HASH_TABLE_HPP

#include <map>
#include <iostream>
#include <c7a/common/logger.hpp>
#include <string>
#include <vector>
#include <stdexcept>

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

struct h_result {
    std::size_t seg_idx;
    std::size_t seg_num;
    std::size_t global_idx;
};

template <typename K, typename V>
struct node {
    std::pair<K, V> v;
    node *n;
};

template<typename K, typename V>
class HashTable
{

static const bool debug = true;

public:

    HashTable(int num_p, std::function<V (V, V)> f_reduce) {
        if (num_p > b_size) {
            throw std::invalid_argument("num processors must be less than num buckets");
        }
        p_size = num_p;
        //b_size = p_size*10; // scale bucket size based on num of processors TODO: implement resize
        alpha_size = b_size/p_size;
        p_items_size = new int[p_size];
        for (int i=0; i<p_size; i++) { // TODO: just a tmp fix
            p_items_size[i] = 0;
        }
        f_red = f_reduce;
    }

    ~HashTable() {
    }

    // insert new item
    void insert(std::pair<K, V> &p)
    {
        h_result h = hash(p.first);

        LOG << "key: "
            << p.first
            << " to idx: "
            << h.global_idx;

        // bucket is empty
        if (a[h.global_idx] == nullptr) {

            LOG << "bucket empty, inserting...";

            node<K, V> *n = new node<K, V>;
            n->v = p;
            n->n = nullptr;
            a[h.global_idx] = n;

            // increase counter for processor
            p_items_size[h.seg_num]++;

            // increase total counter
            total_items_size++;

        // bucket is not empty
        } else {

            LOG << "bucket not empty, checking if key already exists...";

            // check if item with same key
            node<K, V> *current = a[h.global_idx];
            std::pair<K,V> *current_pair;
            do {
                current_pair = &current->v;
                if (p.first == current_pair->first) {

                    LOG << "match of key: "
                        << p.first
                        << " and "
                        << current_pair->first
                        << " ... reducing...";

                    // reduce
                    LOG << "before reduce: "
                        << current_pair->second
                        << " and "
                        << p.second;

                    (*current_pair).second = f_red(current_pair->second, p.second);

                    LOG << "after reduce: "
                        << current_pair->second;

                    LOG << "...finished reduce!";

                    break;
                }

                current = current->n;
            } while (current != nullptr);

            // no item found with key
            if (current == nullptr) {

                LOG << "key doesn't exists in bucket, appending...";

                // insert at first pos
                node<K, V> *n = new node<K, V>;
                n->v = p;
                n->n = a[h.global_idx];
                a[h.global_idx] = n;

                // increase counter for processor
                p_items_size[h.seg_num]++;

                // increase total counter
                total_items_size++;

                LOG << "key appendend, metrics updated!";
            }
        }
    }

    // returns items of segment with max size
    std::vector<std::pair<K, V>> pop() {

        // get segment with max size
        int currMax = 0;
        int currentIdx = 0;
        for (int i=0; i<p_size; i++) {
            if (p_items_size[i] > currMax) {
                currMax = p_items_size[i];
                currentIdx = i;
            }
        }

            LOG << "currMax: "
                << currMax
                << " currentIdx: "
                << currentIdx
                << " currentIdx*alpha_size: "
                << currentIdx*alpha_size
                << " CurrentIdx*alpha_size+alpha_size-1 "
                << currentIdx*alpha_size+alpha_size-1;

        // retrieve items
        std::vector<std::pair<K, V>> popedItems;
        for (int i=currentIdx*alpha_size; i<=currentIdx*alpha_size+alpha_size-1; i++) {
            if (a[i] != nullptr) {
                node<K, V> *current = a[i];
                do {
                    popedItems.push_back(current->v);
                    current = current->n;
                } while (current != nullptr);
                a[i] = nullptr;
            }
        }

        // reset processor specific counter
        p_items_size[currentIdx] = 0;

        // reset counters
        total_items_size -=currMax;

        return popedItems;
    }

    std::size_t size() {
        return total_items_size;
    }

    void resize() {
        LOG << "to be implemneted";
    }

    // prints content of hash table
    void print() {

        for (int i=0; i<b_size; i++) {
            if (a[i] == nullptr) {

                LOG << "bucket "
                    << i
                    << " empty";

            } else {

                std::string log = "";

                // check if item with same key
                node<K, V> *current = a[i];
                std::pair<K,V> current_pair;
                do {
                    current_pair = current->v;

                    log += "(";
                    log += current_pair.first;
                    log += ", ";
                    log += std::to_string(current_pair.second); // TODO: to_string works on primitives
                    log += ") ";

                    current = current->n;
                } while (current != nullptr);

                LOG << "bucket "
                    << i
                    << ": "
                    << log;
            }
        }

        return;
    }

private:

    int p_size = 0; // processor size

    int alpha_size = 0; // num buckets per processor

    int* p_items_size; // total num items per processor

    std::size_t total_items_size = 0; // total sum of items

    static const int b_size = 100; // bucket size

    std::function<V (V, V)> f_red;

    node<K, V> *a[b_size] = { nullptr }; // TODO: fix this static assignment

    h_result hash(std::string v) {

        h_result *h = new h_result();

        // idx within segment
        std::size_t h1 = std::hash<std::string>()(v);
        h->seg_idx = h1 % alpha_size;
        std::cout << h1 << " " << h->seg_idx << std::endl;

        // segment num -> which processor
        h->seg_num = h->seg_idx % p_size;

        // global idx
        h->global_idx = h->seg_idx + h->seg_num*alpha_size;

        return *h;
    }
};

}
}

#endif //C7A_HASH_TABLE_HPP