/*******************************************************************************
 * benchmarks/data/stream_read_write.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <thrill/api/context.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/thread_pool.hpp>

#include <iostream>
#include <random>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "data_generators.hpp"

using namespace thrill; // NOLINT
using common::StatsTimer;

//! Creates two threads (workers) that work with one context instance
//! one worker sends elements to the other worker.
//! Number of elements depends on the number of bytes.
//! one RESULT line will be printed for each iteration
//! All iterations use the same generated data.
//! Variable-length elements range between 1 and 100 bytes
template <typename Type>
void ConductExperiment(uint64_t bytes, int iterations, api::Context& ctx, const std::string& type_as_string) {

    auto data = generate<Type>(bytes, 1, 100);
    common::ThreadPool pool;
    for (int i = 0; i < iterations; i++) {
        auto stream = ctx.GetNewCatStream();
        StatsTimer<true> write_timer;
        pool.Enqueue([&data, &stream, &ctx, &write_timer]() {
                         auto writers = stream->OpenWriters();
                         assert(writers.size() == 1);
                         auto& writer = writers[0];
                         write_timer.Start();
                         for (auto& s : data) {
                             writer(s);
                         }
                         writer.Close();
                         write_timer.Stop();
                     });

        StatsTimer<true> read_timer;
        pool.Enqueue([&stream, &ctx, &read_timer]() {
                         auto readers = stream->OpenReaders();
                         assert(readers.size() == 1);
                         auto& reader = readers[0];
                         read_timer.Start();
                         while (reader.HasNext()) {
                             reader.Next<Type>();
                         }
                         read_timer.Stop();
                     });
        pool.LoopUntilEmpty();
        std::cout << "RESULT"
                  << " datatype=" << type_as_string
                  << " size=" << bytes
                  << " write_time=" << write_timer
                  << " read_time=" << read_timer
                  << std::endl;
    }
}

int main(int argc, const char** argv) {
    common::NameThisThread("benchmark");

    common::CmdlineParser clp;
    clp.SetDescription("thrill::data benchmark for disk I/O");
    clp.SetAuthor("Tobias Sturm <mail@tobiassturm.de>");
    int iterations;
    uint64_t bytes;
    std::string type;
    clp.AddBytes('b', "bytes", bytes, "number of bytes to process");
    clp.AddParamInt("n", iterations, "Iterations");
    clp.AddParamString("type", type,
                       "data type (int, string, pair, triple)");
    if (!clp.Process(argc, argv)) return -1;

    using pair = std::pair<std::string, int>;
    using triple = std::tuple<std::string, int, std::string>;

    if (type == "int")
        api::RunLocalSameThread(std::bind(ConductExperiment<int>, bytes, iterations, std::placeholders::_1, type));
    else if (type == "size_t")
        api::RunLocalSameThread(std::bind(ConductExperiment<size_t>, bytes, iterations, std::placeholders::_1, type));
    else if (type == "string")
        api::RunLocalSameThread(std::bind(ConductExperiment<std::string>, bytes, iterations, std::placeholders::_1, type));
    else if (type == "pair")
        api::RunLocalSameThread(std::bind(ConductExperiment<pair>, bytes, iterations, std::placeholders::_1, type));
    else if (type == "triple")
        api::RunLocalSameThread(std::bind(ConductExperiment<triple>, bytes, iterations, std::placeholders::_1, type));
    else
        abort();
}

/******************************************************************************/
