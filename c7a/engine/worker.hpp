/*******************************************************************************
 * c7a/engine/worker.hpp
 *
 ******************************************************************************/

#ifndef C7A_ENGINE_WORKER_HEADER
#define C7A_ENGINE_WORKER_HEADER

#include <vector>
#include <functional>
#include <map>
#include <iostream>
#include "mock-network.hpp"
#include <thread>
#include <mutex>
#include "Logger.hpp"

namespace c7a {
namespace engine {

class Worker
{
public:

    Worker(size_t id, const std::vector<int> &otherWorkers, MockNetwork& net) :
            _id(id), _otherWorkers(otherWorkers), _mockSelect(net, id) {
    }

    void print() {
        std::cout << "worker " << _id << std::endl;
    }

    template<typename K, typename V>
    void reduce(const std::vector<K> &w) {

        std::vector<K> words = w;
        std::map<K, V> wordsReducedSend;
        std::map<K, V> wordsReducedReceived;

        // create key/value pairs from words
        // actually, will get them from map operation,
        // just simulate here
        std::vector<std::pair<K, V>> wordPairs;

        for (auto it = words.begin(); it != words.end(); it++) {
            wordPairs.push_back(std::pair<K, V>(*it, 1));
        }

        // declare reduce function
        std::function<V (V, V)> f_reduce = [] (const V val1, const V val2) ->V { return val1 + val2; };

        //////////
        // pre operation
        //////////

        // iterate over K,V pairs and reduce
        for (auto it = wordPairs.begin(); it != wordPairs.end(); it++) {

            std::pair<K, V> p = *it;

            // key already cached
            auto res = wordsReducedSend.find(p.first);
            if (res != wordsReducedSend.end()) {
                V red = f_reduce(res->second, p.second);
                res->second = red;

            // key not cached, just insert
            } else {
                wordsReducedSend.insert(std::make_pair(p.first, p.second));
            }
        }

        //////////
        // main operation
        //////////

        // iterate over map with reduce data
        for(auto it = wordsReducedSend.begin(); it != wordsReducedSend.end(); it++) {

            std::pair<K, V> p = *it;
            p.first;
            p.second;

            // compute hash value
            // TODO: fix hashing
            int targetWorker = hash(p.first, _otherWorkers.size()+1);

            std::string msg = "word: " + p.first + " target worker: " + std::to_string(targetWorker);
            Logger::instance().log(msg);

            std::string payload = "word: " + std::string(p.first) + " count: " + std::to_string(p.second);

            // data stays on same worker
            if (targetWorker == _id) {
                wordsReducedReceived.insert(p);

                std::string msg = "payload: " + payload + " stays on worker: " + std::to_string(targetWorker);
                Logger::instance().log(msg);

            // data to be send to other worker
            } else {
                // serialize payload

                std::string msg = "send payload : " + payload + " to worker: " + std::to_string(targetWorker);
                Logger::instance().log(msg);

                // TODO: cache data to be send, send as once
                _mockSelect.sendToWorkerString(targetWorker, payload);
            }
        }

        //////////
        // post operation
        //////////

        size_t out_sender;
        std::string out_data;

        int received = 0;
        while (received < _otherWorkers.size()) {
            _mockSelect.receiveFromAnyString(&out_sender, &out_data);
            // TODO: deserialize data
            // actually insert received data to: _wordsReducedReceived

            std::string msg = "worker " + std::to_string(_id) + " received from: " + std::to_string(out_sender) + " data: " + out_data;
            Logger::instance().log(msg);

            received++;
        }

        // TODO: local reduce
    }

private:
    // this worker's id
    size_t _id;

    // The worker needs to know the ids of all other workers
    std::vector<int> _otherWorkers;

    template<typename K, typename V>
    void print(std::map<K, V> map) {
        for(auto it = map.cbegin(); it != map.cend(); ++it) {
            std::cout << it->first << " " << it->second << "\n";
        }
    }

    // keep the mock select
    MockSelect _mockSelect;

    // @brief hash some input string
    // @param size if interval
    int hash(const std::string key, int size) {
        int hashVal = 0;

        for(int i = 0; i<key.length();  i++)
            hashVal = 37*hashVal+key[i];

        hashVal %= size;

        if(hashVal<0)
            hashVal += size;

        return hashVal;
    }
};

}
}

#endif // !C7A_ENGINE_WORKER_HEADER

/******************************************************************************/
