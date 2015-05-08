/*******************************************************************************
 * c7a/engine/worker.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#ifndef C7A_ENGINE_WORKER_HEADER
#define C7A_ENGINE_WORKER_HEADER

#include <vector>
#include <functional>
#include <map>
#include <unordered_map>
#include <iostream>
#include "mock-network.hpp"
#include <thread>
#include <function>
#include <mutex>
#include <c7a/data/serializer.hpp>
#include <c7a/common/logger.hpp>
#include <c7a/engine/hash_table.hpp>

namespace c7a {

namespace engine {

class Worker
{
// logging mode
    static const bool debug = true;

// max size per hashtable
    static const int maxMapSize = 10;

public:
    Worker(size_t id, size_t num_other_workers, MockNetwork& net) :
        _id(id), _numOtherWorkers(num_other_workers), _mockSelect(net, id) { }

    template <typename K, typename V>
    void reduce(std::pair<K, V> w, std::function<V(V, V)> f_reduce)
    {
        // insert word into hash table
        ht.insert(w, f_reduce);
        //print(ht._hash_m);
        //LOG << "===";

        // if size of map is greater than some threshold
        // do some partial flush
        if (ht._hash_m.size() > maxMapSize) {
            partialFlush<std::string, int>(f_reduce);
        }
    }

    template <typename K, typename V>
    void flush(std::function<V(V, V)> f_reduce)
    {
        //LOG << "flush";
        //print(ht._hash_m);
        while (ht._hash_m.size() != 0) {
            partialFlush<K, V>(f_reduce);
        }
    }

    //////////
    // post operation
    //////////
    template <typename K, typename V>
    void receive(std::function<V(V, V)> f_reduce)
    {
        size_t out_sender;
        std::string out_data;

        int received = 0;
        // Assumption: Only receive one data package per worker
        while (received < _numOtherWorkers - 1) {
            // wait for data from other workers
            _mockSelect.receiveFromAnyString(&out_sender, &out_data);

            // deserialize incoming data
            auto pairToBeReduced = c7a::data::Deserialize<std::pair<K, V> >(out_data);

            LOG << "worker_id: "
                << std::to_string(_id)
                << " received from worker_id: "
                << std::to_string(out_sender)
                << " data: "
                << "(" << pairToBeReduced.first << "," << pairToBeReduced.second << ")";

            // local reduce
            localReduce(dataLocalReduce, pairToBeReduced, f_reduce);

            //TODO: implement some stop criterion
            received++;
        }
    }

private:
    // this worker's id
    size_t _id;

    // The worker needs to know the ids of all other workers
    size_t _numOtherWorkers;

    // keep the mock select
    MockSelect _mockSelect;

    std::map<std::string, int> dataLocalReduce;

    // hash table
    HashTable<std::string, int> ht;

    // flushes data of a certain node
    template <typename K, typename V>
    void partialFlush(std::function<V(V, V)> f_reduce)
    {
        //LOG << "partialFlush";
        if (ht._hash_m.size() == 0)
            return;
        int bucketCount = 0;
        int bIdx;
        for (int i = 0; i < ht._hash_m.bucket_count() - 1; ++i) {
            bucketCount = ht._hash_m.bucket_size(i);
            if (bucketCount > 0) {
                bIdx = i;
            }
        }

        /*while (bucketCount == 0) {
            // TODO eventually replace with Mersenne-Twister
            bIdx = 0 + (rand() % (int)((ht._hash_m.bucket_count()-1)-0+1));
            //LOG << "random bucket "
            //<< bIdx;
            bucketCount = ht._hash_m.bucket_size(bIdx);
            //LOG << "bucketCount "
            //<< bucketCount;
        }*/

        // retrieve all elements from bucket
        int tw = bIdx % _numOtherWorkers;
        //LOG << "target worker "
        //<< tw;

        // get all elements from that bucket
        for (auto it = ht._hash_m.begin(bIdx); it != ht._hash_m.end(bIdx); ++it) {
            std::pair<K, V> pairToBeReduced = *it;

            //LOG << "processing from bucket "
            //<< "(" << pairToBeReduced.first << ", " << pairToBeReduced.second << ")";

            // erase elem
            auto it2 = ht._hash_m.erase(pairToBeReduced.first);
            // check if elem successfully removed
            /*if (it2 == ht._hash_m.end()) {
                LOG << "something went wrong internally";
                continue;
            }*/

            // check if bIdx is mapped to current worker
            // them immediately local reduce
            if (tw == _id) {
                /*LOG << "worker_id: "
                << std::to_string(_id)
                << " send to worker_id: "
                << std::to_string(tw)
                << " data: "
                << "(" << pairToBeReduced.first << ", " << pairToBeReduced.second << ")";*/

                localReduce(dataLocalReduce, pairToBeReduced, f_reduce);

                // otherwise send to target worker
            }
            else {
                /*LOG << "worker_id: "
                << std::to_string(_id)
                << " send to worker_id: "
                << std::to_string(tw)
                << " data: "
                << "(" << pairToBeReduced.first << ", " << pairToBeReduced.second << ")";*/

                // serialize payload
                auto payloadSer = c7a::data::Serialize<std::pair<K, V> >(pairToBeReduced);

                // send payload to target worker
                _mockSelect.sendToWorkerString(tw, payloadSer);
            }
        }
    }

    template <typename K, typename V>
    void print(std::unordered_map<K, V> map)
    {
        std::cout << "mymap's buckets contain:\n";
        for (unsigned i = 0; i < map.bucket_count(); ++i) {
            std::cout << "bucket #" << i << " contains:";
            for (auto local_it = map.begin(i); local_it != map.end(i); ++local_it)
                std::cout << " " << local_it->first << ":" << local_it->second;
            std::cout << std::endl;
        }
        for (auto it = map.cbegin(); it != map.cend(); ++it) {
            LOG << "worker_id: "
                << _id
                << " data: "
                << "(" << it->first << "," << it->second << ")";
        }
    }

    template <typename K, typename V>
    void localReduce(std::map<K, V>& dataReduced,
                     std::pair<K, V>& pairToBeReduced, std::function<V(V, V)> f_reduce)
    {
        // key already cached, then reduce using lambda fn
        auto res = dataReduced.find(pairToBeReduced.first);
        if (res != dataReduced.end()) {
            V red = f_reduce(res->second, pairToBeReduced.second);
            res->second = red;

            // key not yet cached, just cache
        }
        else {
            //dataReduced.insert(std::make_pair(p.first, p.second));
            dataReduced.insert(pairToBeReduced);
        }
    }

    // @brief hash some input string
    // @param size if interval
    /*int hash(const std::string key, size_t size) {
        int hashVal = 0;

        for(int i = 0; i<key.length();  i++)
            hashVal = 37*hashVal+key[i];

        hashVal %= size;

        if(hashVal<0)
            hashVal += size;

        return hashVal;
    }*/
};

} // namespace engine

} // namespace c7a

#endif // !C7A_ENGINE_WORKER_HEADER

/******************************************************************************/
