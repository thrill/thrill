/*******************************************************************************
 * benchmarks/data/file_read_write.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/api/context.hpp>
#include <c7a/common/cmdline_parser.hpp>
#include <c7a/common/logger.hpp>
#include <c7a/common/thread_pool.hpp>
#include <c7a/core/job_manager.hpp>
#include <c7a/data/manager.hpp>

#include "data_generators.hpp"

#include <iostream>
#include <random>
#include <string>

using namespace c7a; // NOLINT

template <typename Type>
void ConductExperiment(uint64_t bytes, int iterations, api::Context& ctx1, api::Context& ctx2) {
    auto overall_timer = ctx1.stats().CreateTimer("all runs", "", true);
    auto data = generate<Type>(bytes, 1, 100);
    c7a::common::ThreadPool pool;
    for (int i = 0; i < iterations; i++) {
        pool.Enqueue([&data, &ctx1]() {
            auto channel = ctx1.data_manager().GetNewChannel();
            auto writers = channel->OpenWriters();
            assert(writers.size() == 2);
            auto& writer = writers[1];
            auto write_timer = ctx1.stats().CreateTimer("write single run", "", true);
            for (auto& s : data) {
                writer(s);
            }
            writer.Close();
            writers[0].Close();
            write_timer->Stop();
        });

        pool.Enqueue([&ctx2]() {
            auto channel = ctx2.data_manager().GetNewChannel();
            auto readers = channel->OpenReaders();
            assert(readers.size() == 2);
            auto& reader = readers[0];
            auto read_timer = ctx2.stats().CreateTimer("read single run", "", true);
            while (reader.HasNext()) {
                reader.Next<Type>();
            }
            read_timer->Stop();
        });
        pool.LoopUntilEmpty();
    }
    overall_timer->Stop();
}

int main(int argc, const char** argv) {
    core::JobManager jobMan1, jobMan2;
    c7a::common::ThreadPool connect_pool;
    std::vector<std::string> endpoints;
    endpoints.push_back("127.0.0.1:8000");
    endpoints.push_back("127.0.0.1:8001");
    connect_pool.Enqueue([&jobMan1, &endpoints](){
        jobMan1.Connect(0, net::Endpoint::ParseEndpointList(endpoints), 1);
    });

    connect_pool.Enqueue([&jobMan2, &endpoints](){
        jobMan2.Connect(1, net::Endpoint::ParseEndpointList(endpoints), 1);
    });
    connect_pool.LoopUntilEmpty();

    api::Context ctx1(jobMan1, 0);
    api::Context ctx2(jobMan2, 0);
    common::GetThreadDirectory().NameThisThread("benchmark");

    common::CmdlineParser clp;
    clp.SetDescription("c7a::data benchmark for disk I/O");
    clp.SetAuthor("Tobias Sturm <mail@tobiassturm.de>");
    int iterations;
    uint64_t bytes;
    std::string type;
    clp.AddBytes('b', "bytes", bytes, "number of bytes to process");
    clp.AddParamInt("n", iterations, "Iterations");
    clp.AddParamString("type", type,
                       "data type (int, string, pair, triple)");
    if (!clp.Process(argc, argv)) return -1;

    if (type == "int")
        ConductExperiment<int>(bytes, iterations, ctx1, ctx2);
    if (type == "string")
        ConductExperiment<std::string>(bytes, iterations, ctx1, ctx2);
    if (type == "pair")
        ConductExperiment<std::pair<std::string, int> >(bytes, iterations, ctx1, ctx2);
    if (type == "triple")
        ConductExperiment<std::tuple<std::string, int, std::string> >(bytes, iterations, ctx1, ctx2);
}

/******************************************************************************/
