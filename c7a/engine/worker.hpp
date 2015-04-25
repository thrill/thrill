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

namespace c7a {
namespace engine {

template<typename K, typename V>
class Worker
{
public:

    Worker(int id, const std::vector<int> &otherWorkers, const std::vector<K> words) :
            _id(id), _otherWorkers(otherWorkers), _words(words) {

        reduce();
    }

    void reduce() {

        // create key/value pairs from words
        // actually, will get them from map operation,
        // just simulate here
        std::vector<std::pair<K, V>> wordPairs;

        for (std::vector<std::string>::iterator it = _words.begin(); it != _words.end(); it++) {
            wordPairs.push_back(std::pair<K, V>(*it, 1));
        }

        // declare reduce function
        std::function<V (V, V)> f_reduce = [] (const V val1, const V val2) ->V { return val1 + val2; };

        // iterate over K,V pairs and reduce
        for (std::vector<std::pair<std::string, int>>::iterator it = wordPairs.begin(); it != wordPairs.end(); it++) {

            std::pair<K, V> p = *it;

            // key already cached
            auto res = _wordsReduced.find(p.first);
            if (res != _wordsReduced.end()) {
                V red = f_reduce(res->second, p.second);
                res->second = red;

            // key not cached, just insert
            } else {
                _wordsReduced.insert(std::make_pair(p.first, p.second));
            }
        }

        // print map with reduced values
        print();
    }

private:
    // this worker's id
    int _id;

    // The worker needs to know the ids of all other workers
    std::vector<int> _otherWorkers;

    // iterator for
    std::vector<K> _words;

    std::map<K, V> _wordsReduced;

    void print() {
        for(auto it = _wordsReduced.cbegin(); it != _wordsReduced.cend(); ++it) {
            std::cout << it->first << " " << it->second << "\n";
        }
    }
};

}
}

#endif // !C7A_ENGINE_WORKER_HEADER

/******************************************************************************/
