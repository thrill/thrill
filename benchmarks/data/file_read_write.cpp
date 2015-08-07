/*******************************************************************************
 * benchmarks/data/file_read_write.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/api/context.hpp>
#include <c7a/common/cmdline_parser.hpp>
#include <c7a/common/logger.hpp>
#include <c7a/common/stats_timer.hpp>
#include <c7a/data/manager.hpp>

#include "data_generators.hpp"

#include <iostream>
#include <random>
#include <string>

using namespace c7a; // NOLINT

//! Writes and reads random elements from a file.
//! Elements are genreated before the timer startet
//! Number of elements depends on the number of bytes.
//! one RESULT line will be printed for each iteration
//! All iterations use the same generated data.
//! Variable-length elements range between 1 and 100 bytes
template <typename Type>
void ConductExperiment(uint64_t bytes, int iterations, api::Context& ctx, const std::string& type_as_string) {
    using namespace c7a::common;

    for (int i = 0; i < iterations; i++) {
        auto file = ctx.data_manager().GetFile();
        auto writer = file.GetWriter();
        auto data = generate<Type>(bytes, 1, 100);

        std::cout << "writing " << bytes << " bytes" << std::endl;
        StatsTimer<true> write_timer(true);
        for (auto& s : data) {
            writer(s);
        }
        writer.Close();
        write_timer.Stop();

        std::cout << "reading " << bytes << " bytes" << std::endl;
        auto reader = file.GetReader();
        StatsTimer<true> read_timer(true);
        while (reader.HasNext())
            reader.Next<Type>();
        read_timer.Stop();
        std::cout << "RESULT"
                  << " datatype=" << type_as_string
                  << " size=" << bytes
                  << " write_time=" << write_timer.Microseconds()
                  << " read_time=" << read_timer.Microseconds()
                  << std::endl;
    }
}

int main(int argc, const char** argv) {
    common::NameThisThread("benchmark");

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

    using pair = std::pair<std::string, int>;
    using triple = std::tuple<std::string, int, std::string>;

    if (type == "int")
        api::RunSameThread(std::bind(ConductExperiment<int>, bytes, iterations, std::placeholders::_1, type));
    else if (type == "size_t")
        api::RunSameThread(std::bind(ConductExperiment<size_t>, bytes, iterations, std::placeholders::_1, type));
    else if (type == "string")
        api::RunSameThread(std::bind(ConductExperiment<std::string>, bytes, iterations, std::placeholders::_1, type));
    else if (type == "pair")
        api::RunSameThread(std::bind(ConductExperiment<pair>, bytes, iterations, std::placeholders::_1, type));
    else if (type == "triple")
        api::RunSameThread(std::bind(ConductExperiment<triple>, bytes, iterations, std::placeholders::_1, type));
    else
        abort();
}

/******************************************************************************/
