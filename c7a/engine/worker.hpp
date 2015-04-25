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

    Worker(int id, const std::vector<int> &otherWorkers, const std::vector<K> &words) :
            _id(id), _otherWorkers(otherWorkers), _words(words) {

        reduce();
    }

    void reduce() {

        // create key/value pairs from words
        // actually, will get them from map operation,
        // just simulate here
        std::vector<std::pair<K, V>> wordPairs;

        for (auto word : _words)
            wordPairs.push_back(make_pair(word, 1));

        // declare reduce function
        std::function<V (V, V)> f_reduce =
            [] (const V &val1, const V &val2) { return val1 + val2; };

        // iterate over K,V pairs and reduce
        for (auto wordPair : wordPairs) {
            // key already in reduce map
            auto res = _wordsReduced.find(wordPair.first);
            if (res != _wordsReduced.end()) {
                V red = f_reduce(res -> second, wordPair.second);
                res -> second = red;

            // key not in map, just insert it
            } else {
                _wordsReduced.insert(wordPair);
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

    // @brief hash some input string
    //
    // @param size if interval
    int hash(const std::string &key, int size) {
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
