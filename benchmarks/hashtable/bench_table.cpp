/*******************************************************************************
 * examples/bench.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/api/dia.hpp>
#include <c7a/core/reduce_pre_table.hpp>
#include <c7a/common/stats_timer.hpp>
#include <c7a/common/cmdline_parser.hpp>

int main(int argc, char* argv[]) {

    c7a::common::CmdlineParser clp;
    
    c7a::net::NetDispatcher dispatcher;
    c7a::net::ChannelMultiplexer multiplexer(dispatcher);
    c7a::data::DataManager manager(multiplexer);

    auto id = manager.AllocateDIA();

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int /* in2 */) {
                      return in1;
                  };

    srand(time(NULL));

    clp.SetVerboseProcess(false);

    unsigned int size = 1;
    clp.AddUInt('s', "size", "S", size,
                "Fill hashtable with S random integers");

    unsigned int workers = 1;
    clp.AddUInt('w',"workers", "W", workers, 
                "Open hashtable with W workers, default = 1.");
    
    unsigned int modulo = 1000;
    clp.AddUInt('m',"modulo", modulo, 
                "Open hashtable with keyspace size of M.");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    //clp.PrintResult();

    std::vector<int> elements(size);

    for (size_t i = 0; i < elements.size(); i++) {
        elements[i] = rand() % modulo;
    }

    std::vector<c7a::data::BlockEmitter<int>> emitter;
    for (size_t i = 0; i < workers; i++) {
        emitter.emplace_back(manager.GetLocalEmitter<int>(id));
    }
    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), c7a::data::BlockEmitter<int>>
    table(workers, key_ex, red_fn, emitter);
    
    c7a::common::StatsTimer<true> timer(true);

    for (size_t i = 0; i < size; i++) {
        table.Insert(std::move(elements[i]));
    }
    table.Flush();
    
    timer.Stop();
    std::cout << timer.Microseconds() << std::endl;



    return 0;
}

/******************************************************************************/
