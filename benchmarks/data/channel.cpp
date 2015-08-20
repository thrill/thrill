/*******************************************************************************
 * benchmarks/data/channel.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <thrill/api/context.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>

#include <iostream>
#include <random>
#include <string>
#include <tuple>

#include "data_generators.hpp"

using namespace thrill; // NOLINT
using common::StatsTimer;

unsigned g_iterations = 1;
uint64_t g_bytes;

template <typename Type>
void ExperimentAllPairs(
    api::Context& ctx, const std::string& type_as_string) {

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
            read_microsecs =
                ctx.AllReduce(read_microsecs, common::maximum<size_t>());

            size_t write_microsecs = write_timer.Microseconds();
            write_microsecs =
                ctx.AllReduce(write_microsecs, common::maximum<size_t>());

            if (ctx.my_rank() == 0) {
                std::cout
                    << "RESULT"
                    << " datatype=" << type_as_string
                    << " size=" << g_bytes
                    << " src=" << src << " tgt=" << tgt
                    << " write_time=" << write_microsecs
                    << " read_time=" << read_microsecs
                    << " write_speed_MiBs="
                    << (g_bytes / write_microsecs * 1e6 / 1024 / 1024)
                    << " read_speed_MiBs="
                    << (g_bytes / read_microsecs * 1e6 / 1024 / 1024)
                    << std::endl;
            }
        }
    }
}

template <typename Type>
void ExperimentFull(
    api::Context& ctx, const std::string& type_as_string) {

    // transmit data to all workers.

    auto channel = ctx.GetNewChannel();

    // write phase
    StatsTimer<true> write_timer(true);
    {
        auto writers = channel->OpenWriters();
        auto data = Generator<Type>(g_bytes);

        while (data.HasNext()) {
            Type value = data.Next();
            for (size_t tgt = 0; tgt < ctx.num_workers(); ++tgt) {
                writers[tgt](value);
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
    read_microsecs =
        ctx.AllReduce(read_microsecs, common::maximum<size_t>());

    size_t write_microsecs = write_timer.Microseconds();
    write_microsecs =
        ctx.AllReduce(write_microsecs, common::maximum<size_t>());

    uint64_t host_volume = ctx.num_workers() * g_bytes;
    uint64_t total_volume = ctx.num_workers() * ctx.num_workers() * g_bytes;

    if (ctx.my_rank() == 0) {
        std::cout
            << "RESULT"
            << " datatype=" << type_as_string
            << " size=" << g_bytes
            << " write_time=" << write_microsecs
            << " read_time=" << read_microsecs
            << " write_speed_MiBs="
            << (g_bytes / write_microsecs * 1e6 / 1024 / 1024)
            << " read_speed_MiBs="
            << (g_bytes / read_microsecs * 1e6 / 1024 / 1024)
            << " host_write_speed_MiBs="
            << (host_volume / write_microsecs * 1e6 / 1024 / 1024)
            << " host_read_speed_MiBs="
            << (host_volume / read_microsecs * 1e6 / 1024 / 1024)
            << " total_write_speed_MiBs="
            << (total_volume / write_microsecs * 1e6 / 1024 / 1024)
            << " total_read_speed_MiBs="
            << (total_volume / read_microsecs * 1e6 / 1024 / 1024)
            << std::endl;
    }
}

template <typename Type>
void Experiment(
    const std::string& experiment,
    api::Context& ctx, const std::string& type_as_string) {

    if (experiment == "AllPairs") {
        ExperimentAllPairs<Type>(ctx, type_as_string);
    }
    else if (experiment == "Full") {
        ExperimentFull<Type>(ctx, type_as_string);
    }
    else {
        die("Invalid experiment " << experiment);
    }
}

int main(int argc, const char** argv) {
    common::NameThisThread("benchmark");

    common::CmdlineParser clp;
    clp.SetDescription("thrill::data benchmark for Channel I/O");
    clp.SetAuthor("Tobias Sturm <mail@tobiassturm.de>");

    clp.AddBytes('b', "bytes", g_bytes, "number of bytes to process");
    clp.AddUInt('n', "iterations", g_iterations, "Iterations (default: 1)");

    std::string experiment;
    clp.AddParamString("experiment", experiment,
                       "experiment: AllPairs, Full");

    std::string type;
    clp.AddParamString("type", type,
                       "data type (size_t, string, pair, triple)");

    if (!clp.Process(argc, argv)) return -1;

    using pair = std::tuple<std::string, size_t>;
    using triple = std::tuple<std::string, size_t, std::string>;

    if (type == "size_t")
        api::Run([&](Context& ctx) {
                     return Experiment<size_t>(experiment, ctx, type);
                 });
    else if (type == "string")
        api::Run([&](Context& ctx) {
                     return Experiment<std::string>(experiment, ctx, type);
                 });
    else if (type == "pair")
        api::Run([&](Context& ctx) {
                     return Experiment<pair>(experiment, ctx, type);
                 });
    else if (type == "triple")
        api::Run([&](Context& ctx) {
                     return Experiment<triple>(experiment, ctx, type);
                 });
    else
        abort();
}

/******************************************************************************/
