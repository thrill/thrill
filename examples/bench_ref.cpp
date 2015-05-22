/*******************************************************************************
 * examples/bench.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/api/dia.hpp>
#include <c7a/api/context.hpp>
#include <c7a/core/reduce_pre_table.hpp>
#include <c7a/core/reduce_pre_table_bench.hpp>

#include <functional>
#include <cstdio>
#include <cstdlib>
#include <ctime>

int main(int argc, char* argv[])
{
     auto emit = [](int in) {
                    in = in;
                    //std::cout << in << std::endl;
                };

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };
    
    srand (time(NULL));

    c7a::core::ReducePreTableBench<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(10, key_ex, red_fn, { emit });

    clock_t time = std::clock();

    int end = std::stoi(argv[1]);

    for (int i = 0; i < end; i++) {
        table.Insert(rand() % 100);
    }

    table.Flush();

    time = std::clock() - time;

    printf( "%f", ((double) (time * 1000000) / (double) CLOCKS_PER_SEC) );
    //printf(std::endl);
    

    //std::cout << (time * 1000000) / (double) CLOCKS_PER_SEC << std::endl;

    return 0;
}

/******************************************************************************/
