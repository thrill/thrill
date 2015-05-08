#include <sstream>
#include <unordered_map>
#include <vector>
#include <iostream>
#include <queue>
#include <array>
#include <utility>
#include "../c7a/engine/hash_table.hpp"

int main()
{
    c7a::engine::HashTable <std::string, int> ht(10, [](const int val1, const int val2) ->int { return val1 + val2; });

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
    std::vector<std::pair<std::string, int>> r = ht.pop();
    std::cout << "retrieved num items: " << r.size() << std::endl;
    ht.print();
    r = ht.pop();
    std::cout << "retrieved num items: " << r.size() << std::endl;
    ht.print();

    return 0;
}