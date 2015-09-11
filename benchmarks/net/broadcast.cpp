/*******************************************************************************
 * benchmarks/page_rank/page_rank.cpp
 *
 * Minimalistic broadcast benchmark to test different net implementations. 
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
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

unsigned int iterations = 200;
unsigned int repeats = 1;

void PrintSQLPlotTool(std::string datatype, size_t workers, int iterations, int time) {
    static const bool debug = true;
    LOG << "RESULT"
        << " datatype=" << datatype
        << " workers=" << workers
        << " repeats=" << iterations
        << " time=" << time
        << " time_per_op[ms]=" << static_cast<double>(time) / iterations;
}

//! Network benchmarking. 
void net_test(thrill::api::Context& ctx) {
    auto &flow = ctx.flow_control_channel();

    for (size_t r = 0; r < repeats; ++r) {
        thrill::common::StatsTimer<true> t;

        size_t dummy = +4915221495089;

        t.Start();
        for(size_t i = 0; i < iterations; i++) {
            dummy = flow.Broadcast(dummy);
        }
        t.Stop();

        size_t n = ctx.num_workers();
        size_t time = t.Microseconds();
        // calculate maximum time.
        time = flow.AllReduce(time, thrill::common::maximum<size_t>());

        if(ctx.my_rank() == 0)
            PrintSQLPlotTool("size_t", n, iterations, time);
    }
}

int main(int argc, char** argv) {
    
    thrill::common::CmdlineParser clp;

    clp.SetVerboseProcess(false);
    
    clp.AddUInt('i', "iterations", iterations,
                "Count of iterations");

    clp.AddUInt('r', "repeats", repeats,
                "Repeat experiment a number of times.");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    return thrill::api::Run(net_test);
}

/******************************************************************************/
