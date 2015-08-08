/*******************************************************************************
 * benchmarks/data/channel.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/api/context.hpp>
#include <c7a/common/cmdline_parser.hpp>
#include <c7a/common/functional.hpp>
#include <c7a/common/logger.hpp>

#include "data_generators.hpp"

#include <iostream>
#include <random>
#include <string>

using namespace c7a; // NOLINT

unsigned g_iterations = 1;
uint64_t g_bytes;

template <typename Type>
void ConductExperiment(
    api::Context& ctx, const std::string& type_as_string) {
    using namespace c7a::common;

    for (size_t src = 0; src < ctx.num_workers(); ++src) {
        for (size_t tgt = 0; tgt < ctx.num_workers(); ++tgt) {
            // transmit data from worker src -> tgt: only send data if we are
            // tgt, but as tgt receive from all.

            auto channel = ctx.GetNewChannel();

            // write phase
            StatsTimer<true> write_timer(true);
            {
                auto writers = channel->OpenWriters();

                if (ctx.my_rank() == src) {
                    auto data = Generator<Type>(g_bytes);

                    auto& writer = writers[tgt];
                    while (data.HasNext()) {
                        writer(data.Next());
                    }
                }
            }
            write_timer.Stop();

            // read phase
            StatsTimer<true> read_timer(true);
            {
                auto reader = channel->OpenReader();

                while (reader.HasNext()) {
                    reader.Next<Type>();
                }
            }
            read_timer.Stop();

            size_t read_microsecs = read_timer.Microseconds();
            read_microsecs = ctx.AllReduce(read_microsecs, common::maximum<size_t>());

            size_t write_microsecs = write_timer.Microseconds();
            write_microsecs = ctx.AllReduce(write_microsecs, common::maximum<size_t>());

            if (ctx.my_rank() == 0) {
                std::cout
                    << "RESULT"
                    << " datatype=" << type_as_string
                    << " size=" << g_bytes
                    << " src=" << src << " tgt=" << tgt
                    << " write_time=" << write_microsecs
                    << " read_time=" << read_microsecs
                    << " write_speed_MiBs=" << (g_bytes / write_microsecs * 1e6 / 1024 / 1024)
                    << " read_speed_MiBs=" << (g_bytes / read_microsecs * 1e6 / 1024 / 1024)
                    << std::endl;
            }
        }
    }
}

int main(int argc, const char** argv) {
    common::NameThisThread("benchmark");

    common::CmdlineParser clp;
    clp.SetDescription("c7a::data benchmark for Channel I/O");
    clp.SetAuthor("Tobias Sturm <mail@tobiassturm.de>");
    std::string type;
    clp.AddBytes('b', "bytes", g_bytes, "number of bytes to process");
    clp.AddUInt('n', "iterations", g_iterations, "Iterations (default: 1)");
    clp.AddParamString("type", type,
                       "data type (size_t, string, pair, triple)");
    if (!clp.Process(argc, argv)) return -1;

    using pair = std::tuple<std::string, size_t>;
    using triple = std::tuple<std::string, size_t, std::string>;

    if (type == "size_t")
        api::Run([&](Context& ctx) {
                     return ConductExperiment<size_t>(ctx, type);
                 });
    else if (type == "string")
        api::Run([&](Context& ctx) {
                     return ConductExperiment<std::string>(ctx, type);
                 });
    else if (type == "pair")
        api::Run([&](Context& ctx) {
                     return ConductExperiment<pair>(ctx, type);
                 });
    else if (type == "triple")
        api::Run([&](Context& ctx) {
                     return ConductExperiment<triple>(ctx, type);
                 });
    else
        abort();
}

/******************************************************************************/
