/*******************************************************************************
 * benchmarks/data/file_read_write.cpp
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
#include <thrill/common/stats_timer.hpp>

#include <iostream>
#include <random>
#include <string>
#include <tuple>

#include "data_generators.hpp"

using namespace thrill; // NOLINT
using common::StatsTimer;

//! Writes and reads random elements from a file.
//! Elements are genreated before the timer startet
//! Number of elements depends on the number of bytes.
//! one RESULT line will be printed for each iteration
//! All iterations use the same generated data.
//! Variable-length elements range between 1 and 100 bytes
template <typename Type>
void ConductExperiment(uint64_t bytes, unsigned iterations, api::Context& ctx, const std::string& type_as_string) {

    for (unsigned i = 0; i < iterations; i++) {
        auto file = ctx.GetFile();
        auto writer = file.GetWriter();
        auto data = Generator<Type>(bytes);

        std::cout << "writing " << bytes << " bytes" << std::endl;
        StatsTimer<true> write_timer(true);
        while (data.HasNext()) {
            writer(data.Next());
        }
        writer.Close();
        write_timer.Stop();

        std::cout << "reading " << bytes << " bytes" << std::endl;
        auto reader = file.GetConsumeReader();
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
    clp.SetDescription("thrill::data benchmark for disk I/O");
    clp.SetAuthor("Tobias Sturm <mail@tobiassturm.de>");
    unsigned iterations = 1;
    uint64_t bytes;
    std::string type;
    clp.AddBytes('b', "bytes", bytes, "number of bytes to process");
    clp.AddUInt('n', "iterations", iterations, "Iterations (default: 1)");
    clp.AddParamString("type", type,
                       "data type (size_t, string, pair, triple)");
    if (!clp.Process(argc, argv)) return -1;

    using pair = std::tuple<std::string, size_t>;
    using triple = std::tuple<std::string, size_t, std::string>;

    if (type == "size_t")
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
