/*******************************************************************************
 * c7a/engine/worker.hpp
 *
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
#include <mutex>
#include <c7a/data/serializer.hpp>
#include <c7a/common/logger.hpp>
#include <c7a/engine/hash_table.hpp>

namespace c7a {
namespace engine {

class Worker
{

static const bool debug = true;

public:

    Worker(size_t id, size_t num_other_workers, MockNetwork& net) :
            _id(id), _numOtherWorkers(num_other_workers), _mockSelect(net, id) {
    }

    void print() {
        LOG << "worker "
            << _id;
    }

    template<typename K, typename V>
    void reduce(const std::vector<K> &w) {

        // declare reduce function
        std::function<V (V, V)> f_reduce = [] (const V val1, const V val2) ->V { return val1 + val2; };

        std::vector<K> words = w;
        std::map<K, V> dataGlobalReduce;
        std::map<K, V> dataLocalReduce;

        // create key/value pairs from words
        // actually, will get them from map operation,
        // just simulate here
        std::vector<std::pair<K, V>> wordPairs;

        for (auto word : words)
            wordPairs.push_back(std::make_pair(word, 1));

        //////////
        // pre operation
        //////////

        // iterate over K,V pairs and reduce
        for (auto it = wordPairs.begin(); it != wordPairs.end(); it++) {
            // get the pair
            std::pair<K, V> pairToBeReduced = *it;

            // reduce pair
            ht.insert(pairToBeReduced, f_reduce);

            // TODO remove, just tmp, being replaced by custom hash table
            localReduce<K, V>(dataLocalReduce, pairToBeReduced, f_reduce);
        }

        ht.print();

        //////////
        // main operation
        //////////

        // send data to other workers
        for(auto it = dataGlobalReduce.begin(); it != dataGlobalReduce.end(); it++) {
            std::pair<K, V> p = *it;

            // compute hash value from key representing id of target worker
            int targetWorker = hash(p.first, _numOtherWorkers);

            LOG << "word: "
                << p.first
                << " target worker: "
                << std::to_string(targetWorker);

            // if target worker equals _id,
            // keep data on the same worker
            if (targetWorker == _id) {

                // add data to be reduced
                dataLocalReduce.insert(p);

                LOG << "payload: "
                    << "word: "
                    << std::string(p.first)
                    << " count: "
                    << std::to_string(p.second)
                    << " stays on worker_id: "
                    << std::to_string(targetWorker);

            // data to be send to another worker
            } else {

                LOG << "send payload : "
                    << "word: "
                    << std::string(p.first)
                    << " count: "
                    << std::to_string(p.second)
                    << " to worker_id: "
                    << std::to_string(targetWorker);

                // serialize payload
                auto payloadSer = c7a::data::Serialize<std::pair<K, V>>(p);

                // send payload to target worker
                _mockSelect.sendToWorkerString(targetWorker, payloadSer);
            }
        }

        // inform all workers about no more data is send
        /*auto payloadSer = Serialize<std::pair<K, V>>(p);
        for (int n=0; n<_numOtherWorkers; n++) {
            if (n != _id)
                _mockSelect.sendToWorkerString(n, payloadSer);
        }*/

        //////////
        // post operation
        //////////

        size_t out_sender;
        std::string out_data;

        int received = 0;
        // Assumption: Only receive one data package per worker
        while (received < _numOtherWorkers-1) {
            // wait for data from other workers
            _mockSelect.receiveFromAnyString(&out_sender, &out_data);

            // deserialize incoming data
            auto pairToBeReduced = c7a::data::Deserialize<std::pair<K, V>>(out_data);

            LOG << "worker_id: "
                << std::to_string(_id)
                << " received from worker_id: "
                << std::to_string(out_sender)
                << " data: "
                << "(" << pairToBeReduced.first << "," << pairToBeReduced.second << ")";

            // local reduce
            localReduce<K, V>(dataLocalReduce, pairToBeReduced, f_reduce);

            //TODO: implement some stop criterion
            received++;
        }

        print(dataLocalReduce);
    }

private:
    // this worker's id
    size_t _id;

    // The worker needs to know the ids of all other workers
    size_t _numOtherWorkers;

    template<typename K, typename V>
    void print(std::map<K, V> map) {
        for(auto it = map.cbegin(); it != map.cend(); ++it) {
            LOG << "worker_id: "
                << _id
                << " data: "
                << "(" << it->first << "," << it->second << ")";
        }
    }

    // keep the mock select
    MockSelect _mockSelect;

    HashTable<std::string, int> ht;

    // @brief hash some input string
    // @param size if interval
    int hash(const std::string key, size_t size) {
        int hashVal = 0;

        for(int i = 0; i<key.length();  i++)
            hashVal = 37*hashVal+key[i];

        hashVal %= size;

        if(hashVal<0)
            hashVal += size;

        return hashVal;
    }

    template<typename K, typename V>
    void localReduce(std::map<K, V> &dataReduced,
                     std::pair<K, V> &pairToBeReduced, std::function<V (V, V)> f_reduce) {

        // key already cached, then reduce using lambda fn
        auto res = dataReduced.find(pairToBeReduced.first);
        if (res != dataReduced.end()) {
            V red = f_reduce(res->second, pairToBeReduced.second);
            res->second = red;

        // key not yet cached, just cache
        } else {
            //dataReduced.insert(std::make_pair(p.first, p.second));
            dataReduced.insert(pairToBeReduced);
        }
    }
};

}
}

#endif // !C7A_ENGINE_WORKER_HEADER

/******************************************************************************/
