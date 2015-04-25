#include "../c7a/engine/worker.hpp"

int main()
{
    c7a::engine::Worker<std::string, int> w1(1, {2, 3}, { "word", "word", "word" });
    c7a::engine::Worker<std::string, int> w2(2, {1, 3}, { "word", "word", "word" });
    c7a::engine::Worker<std::string, int> w3(3, {1, 2}, { "word", "word", "word" });

    return 0;
}
