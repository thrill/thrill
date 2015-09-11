/*******************************************************************************
 * benchmarks/page_rank/page_rank.cpp
 *
 * Part of Project Thrill.
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/string.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/context.hpp>

#include <random>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

size_t iterations = 200;
size_t workers = 1;

void PrintSQLPlotTool(std::string datatype, size_t workers, int iterations, int time) {
    static const bool debug = true;
    LOG << "RESULT"
          << " datatype=" << datatype
          << " workers=" << workers
          << " repeats=" << iterations
          << " time=" << time;
}

//! Network benchmarking. 
void net_test(thrill::api::Context& ctx) {
    auto &flow = ctx.flow_control_channel();

    thrill::common::StatsTimer<true> t;

    size_t dummy = +4915221495089; 
   
    for(size_t i = 0; i < iterations; i++) {
        t.Start();
        dummy = flow.Broadcast(dummy);
        t.Stop(); 
    }

    PrintSQLPlotTool("size_t", workers, iterations, t.Microseconds()); 
}

int main(int argc, char** argv) {
    
    thrill::common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    return thrill::api::Run(net_test);
}

/******************************************************************************/
