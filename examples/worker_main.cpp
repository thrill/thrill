/*******************************************************************************
 * examples/worker_main.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/engine/hash_table.hpp>

#include <sstream>
#include <unordered_map>
#include <vector>
#include <iostream>
#include <queue>
#include <array>
#include <utility>
#include <string>

int main()
{
    using WordPair = std::pair<std::string, int>;
    auto key = [](WordPair in) { return in.first; };
    auto red_fn = [](WordPair in1, WordPair in2) { return std::make_pair(in1.first, in1.second + in2.second); };

    c7a::engine::HashTable<decltype(key), decltype(red_fn)> ht(10, key, red_fn);

    std::pair<std::string, int> v1 = { "word1", 1 };
    ht.insert(v1);
    std::pair<std::string, int> v2 = { "word1", 1 };
    ht.insert(v2);
    std::pair<std::string, int> v3 = { "word3", 1 };
    ht.insert(v3);
    std::pair<std::string, int> v4 = { "word4", 1 };
    ht.insert(v4);
    std::pair<std::string, int> v5 = { "word5", 1 };
    ht.insert(v5);
    std::pair<std::string, int> v6 = { "word6", 1 };
    ht.insert(v6);
    std::pair<std::string, int> v7 = { "word7", 1 };
    ht.insert(v7);
    std::pair<std::string, int> v8 = { "word8", 1 };
    ht.insert(v8);

    ht.print();
    std::cout << "totel item size: " << ht.size() << std::endl;
    std::vector<std::pair<std::string, int> > r = ht.pop();
    std::cout << "retrieved num items: " << r.size() << std::endl;
    std::cout << "totel item size: " << ht.size() << std::endl;
    ht.print();
    r = ht.pop();
    std::cout << "retrieved num items: " << r.size() << std::endl;
    std::cout << "totel item size: " << ht.size() << std::endl;
    ht.print();

    return 0;
}

/******************************************************************************/
