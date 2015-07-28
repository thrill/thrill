/*******************************************************************************
 * benchmarks/data/file_writer.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/common/cmdline_parser.hpp>
#include <c7a/common/logger.hpp>
#include <c7a/core/job_manager.hpp>
#include <c7a/api/context.hpp>
#include <c7a/data/manager.hpp>

#include "data_generators.hpp"

#include <random>
#include <iostream>
#include <string>

using namespace c7a; // NOLINT


template<typename Type>
void ConductExperiment(uint64_t bytes, int iterations, api::Context& ctx) {
    auto overall_timer = ctx.stats().CreateTimer("all runs", "", true);
    for (int i = 0; i < iterations; i++) {

        auto file = ctx.data_manager().GetFile();
        auto writer = file.GetWriter();
        auto data = generate<Type>(bytes, 1, 100);

        std::cout << "writing " << bytes << " bytes" << std::endl;
        auto write_timer = ctx.stats().CreateTimer("write single run", "", true);
        for(auto& s : data) {
            writer(s);
        }
        writer.Close();
        write_timer->Stop();

        std::cout << "reading " << bytes << " bytes" << std::endl;
        auto reader = file.GetReader();
        auto read_timer = ctx.stats().CreateTimer("read single run", "", true);
        while(reader.HasNext())
            reader.Next<Type>();
        read_timer->Stop();
    }
    overall_timer->Stop();
}

int main(int argc, const char** argv) {
    core::JobManager jobMan;
    jobMan.Connect(0, net::Endpoint::ParseEndpointList("127.0.0.1:8000"), 1);
    api::Context ctx(jobMan, 0);
    common::GetThreadDirectory().NameThisThread("benchmark");

    common::CmdlineParser clp;
    clp.SetDescription("c7a::data benchmark for disk I/O");
    clp.SetAuthor("Tobias Sturm <mail@tobiassturm.de>");
    int iterations;
    uint64_t bytes;
    std::string type;
    clp.AddBytes(   'b', "bytes", bytes,  "number of bytes to process");
    clp.AddParamInt(   "n", iterations,  "Iterations");
    clp.AddParamString("type", type,
                                  "data type (int, string, pair, triple)");
    if (!clp.Process(argc, argv)) return -1;

    if (type == "int")
        ConductExperiment<int>(bytes, iterations, ctx);
    if (type == "string")
        ConductExperiment<std::string>(bytes, iterations, ctx);
    if (type == "pair")
        ConductExperiment<std::pair<std::string, int>>(bytes, iterations, ctx);
    if (type == "triple")
        ConductExperiment<std::tuple<std::string, int, std::string>>(bytes, iterations, ctx);
}

/******************************************************************************/
