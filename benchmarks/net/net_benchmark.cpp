/*******************************************************************************
 * benchmarks/net/net_benchmark.cpp
 *
 * Network backend benchmarks:
 * - 1-factor ping pong latency benchmark
 * - 1-factor full bandwidth test
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/api/context.hpp>
#include <thrill/common/aggregate.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/common/string.hpp>

#include <string>
#include <utility>
#include <vector>

std::string benchmark;

unsigned int outer_repeats = 1;
unsigned int inner_repeats = 1;

using namespace thrill; // NOLINT

// matrix of measured latencies
using AggDouble = common::Aggregate<double>;
using AggMatrix = std::vector<std::vector<AggDouble> >;

AggMatrix AddAggMatrix(const AggMatrix& a, const AggMatrix& b) {
    AggMatrix m = a;
    for (size_t i = 0; i < a.size(); ++i) {
        for (size_t j = 0; j < a.size(); ++j) {
            m[i][j] += b[i][j];
        }
    }
    return m;
}

/******************************************************************************/
//! perform a 1-factor ping pong latency test

unsigned int iterations = 1;

void PingPongLatencyTest(api::Context& ctx) {

    // only work with first thread on this host.
    if (ctx.local_worker_id() != 0) return;

    net::Group& group = ctx.flow_control_channel().group();

    // a counter to count and match ping pongs.
    size_t counter = 0;

    AggMatrix latency(
        group.num_hosts(), std::vector<AggDouble>(group.num_hosts()));

    for (size_t outer_repeat = 0;
         outer_repeat < outer_repeats; ++outer_repeat) {

        common::StatsTimer<true> timer;

        timer.Start();
        for (size_t iteration = 0; iteration < iterations; iteration++) {
            // perform 1-factor ping pongs (without barriers)
            for (size_t round = 0; round < group.OneFactorSize(); ++round) {

                size_t peer = group.OneFactorPeer(round);

                sLOG0 << "round" << round
                      << "me" << ctx.host_rank() << "peer" << peer;

                auto Sender =
                    [&]() {
                        common::StatsTimer<true> inner_timer(true);

                        for (size_t inner_repeat = 0;
                             inner_repeat < inner_repeats; inner_repeat++) {

                            // send ping to peer
                            size_t value = counter++;
                            group.SendTo(peer, value);

                            // wait for ping
                            group.ReceiveFrom(peer, &value);
                            assert(value == counter);
                        }
                        inner_timer.Stop();

                        double avg =
                            static_cast<double>(inner_timer.Microseconds()) /
                            static_cast<double>(inner_repeats);

                        sLOG0 << "bandwidth" << ctx.host_rank() << "->" << peer
                              << "round" << round
                              << "iteration" << iteration
                              << "latency" << avg;

                        latency[ctx.host_rank()][peer].Add(avg);
                    };

                auto Receiver =
                    [&]() {
                        for (size_t inner_repeat = 0;
                             inner_repeat < inner_repeats; inner_repeat++) {

                            // wait for ping
                            size_t value;
                            group.ReceiveFrom(peer, &value);
                            assert(value == counter);

                            // increment counters
                            counter++, value++;

                            // send pong
                            group.SendTo(peer, value);
                        }
                    };

                if (ctx.host_rank() < peer) {
                    Sender();
                    Receiver();
                }
                else if (ctx.host_rank() > peer) {
                    Receiver();
                    Sender();
                }
                else {
                    // not participating in this round
                    ++counter;
                }
            }
        }
        timer.Stop();

        size_t time = timer.Microseconds();
        // calculate maximum time.
        group.AllReduce(time, common::maximum<size_t>());

        if (ctx.my_rank() == 0) {
            LOG1 << "RESULT"
                 << " benchmark=" << benchmark
                 << " hosts=" << ctx.num_hosts()
                 << " outer_repeat=" << outer_repeat
                 << " iterations=" << iterations
                 << " inner_repeats=" << inner_repeats
                 << " ping_pongs=" << counter
                 << " time[us]=" << time
                 << " time_per_ping_pong[us]="
                 << static_cast<double>(time) / static_cast<double>(counter);
        }
    }

    // reduce matrix to root.
    group.ReduceToRoot(latency, AddAggMatrix);

    // print matrix
    if (ctx.my_rank() == 0) {
        for (size_t i = 0; i < latency.size(); ++i) {
            std::ostringstream os2;
            for (size_t j = 0; j < latency.size(); ++j) {
                os2 << common::str_snprintf(
                    64, "%8.1f/%8.3f",
                    latency[i][j].Avg(), latency[i][j].StdDev());
            }
            LOG1 << os2.str();
        }
    }
}

int RunPingPongLatencyTest(int argc, char* argv[]) {

    common::CmdlineParser clp;

    clp.AddUInt('R', "outer_repeats", outer_repeats,
                "Repeat whole experiment a number of times.");

    inner_repeats = 100;

    clp.AddUInt('r', "inner_repeats", inner_repeats,
                "Repeat inner experiment a number of times.");

    clp.AddParamUInt("iterations", iterations,
                     "Repeat 1-factor iterations a number of times.");

    if (!clp.Process(argc, argv))
        return -1;

    return api::Run(PingPongLatencyTest);
}

/******************************************************************************/
//! perform a 1-factor bandwidth test

uint64_t bandwidth_size = 1024 * 1024 * 1024llu;

uint64_t block_size = 2 * 1024 * 1024;

void BandwidthTest(api::Context& ctx) {

    // only work with first thread on this host.
    if (ctx.local_worker_id() != 0) return;

    net::Group& group = ctx.flow_control_channel().group();

    // a counter to count and match messages
    size_t counter = 0;

    AggMatrix bandwidth(
        group.num_hosts(), std::vector<AggDouble>(group.num_hosts()));

    // data block to send or receive
    size_t block_count = bandwidth_size / block_size;
    std::vector<size_t> data_block(block_size / sizeof(size_t));
    std::fill(data_block.begin(), data_block.end(), 42u);

    for (size_t outer_repeat = 0;
         outer_repeat < outer_repeats; ++outer_repeat) {

        common::StatsTimer<true> timer;

        timer.Start();
        for (size_t inner_repeat = 0;
             inner_repeat < inner_repeats; inner_repeat++) {
            // perform 1-factor ping pongs (without barriers)
            for (size_t iter = 0; iter < group.OneFactorSize(); ++iter) {

                size_t peer_id = group.OneFactorPeer(iter);
                net::Connection& peer = group.connection(peer_id);

                sLOG0 << "round iter" << iter
                      << "me" << ctx.host_rank() << "peer" << peer;

                auto Sender =
                    [&]() {
                        common::StatsTimer<true> inner_timer(true);
                        // send blocks to peer
                        for (size_t i = 0; i != block_count; ++i) {
                            data_block.front() = counter;
                            data_block.back() = counter;
                            ++counter;
                            peer.SyncSend(data_block.data(), block_size);
                        }

                        // wait for response pong
                        size_t value;
                        peer.Receive(&value);
                        assert(value == counter);

                        inner_timer.Stop();

                        double bw =
                            static_cast<double>(block_count * block_size) /
                            static_cast<double>(inner_timer.Microseconds()) *
                            1e6 / 1024.0 / 1024.0;

                        sLOG1 << "bandwidth" << ctx.host_rank() << "->" << peer_id
                              << bw << "MiB/s"
                              << "time"
                              << (static_cast<double>(inner_timer.Microseconds()) * 1e-6);

                        bandwidth[ctx.host_rank()][peer_id].Add(bw);
                    };

                auto Receiver =
                    [&]() {
                        // receive blocks from peer
                        for (size_t i = 0; i != block_count; ++i) {
                            peer.SyncRecv(data_block.data(), block_size);
                            die_unequal(data_block.front(), counter);
                            die_unequal(data_block.back(), counter);

                            ++counter;
                        }

                        // send ping
                        peer.Send(counter);
                    };

                if (ctx.host_rank() < peer_id) {
                    Sender();
                    Receiver();
                }
                else if (ctx.host_rank() > peer_id) {
                    Receiver();
                    Sender();
                }
                else {
                    // not participating in this round
                    counter += block_count;
                }
            }
        }
        timer.Stop();

        size_t time = timer.Microseconds();
        // calculate maximum time.
        group.AllReduce(time, common::maximum<size_t>());

        if (ctx.my_rank() == 0) {
            LOG1 << "RESULT"
                 << " benchmark=" << benchmark
                 << " hosts=" << ctx.num_hosts()
                 << " outer_repeat=" << outer_repeat
                 << " inner_repeats=" << inner_repeats
                 << " ping_pongs=" << counter
                 << " time[us]=" << time
                 << " time_per_ping_pong[us]="
                 << static_cast<double>(time) / static_cast<double>(counter);
        }
    }

    // reduce matrix to root.
    group.ReduceToRoot(bandwidth, AddAggMatrix);

    // print matrix
    if (ctx.my_rank() == 0) {
        for (size_t i = 0; i < bandwidth.size(); ++i) {
            std::ostringstream os2;
            for (size_t j = 0; j < bandwidth.size(); ++j) {
                os2 << common::str_snprintf(
                    64, "%8.1f/%8.3f",
                    bandwidth[i][j].Avg(), bandwidth[i][j].StdDev());
            }
            LOG1 << os2.str();
        }
    }
}

int RunBandwidthTest(int argc, char* argv[]) {

    common::CmdlineParser clp;

    clp.AddUInt('r', "inner_repeats", inner_repeats,
                "Repeat inner experiment a number of times.");

    clp.AddUInt('R', "outer_repeats", outer_repeats,
                "Repeat whole experiment a number of times.");

    clp.AddBytes('B', "block_size", block_size,
                 "Block size used to transfered data (default: 2 MiB).");

    clp.AddParamBytes("size", bandwidth_size,
                      "Amount of data transfered between peers (example: 1 GiB).");

    if (!clp.Process(argc, argv))
        return -1;

    return api::Run(BandwidthTest);
}

/******************************************************************************/

int main(int argc, char** argv) {

    if (argc <= 1) {
        std::cout
            << "Usage: " << argv[0] << " <benchmark>" << std::endl
            << std::endl
            << "    ping_pong  - 1-factor latency" << std::endl
            << "    bandwidth  - 1-factor bandwidth" << std::endl
            << std::endl;
        return 0;
    }

    benchmark = argv[1];

    if (benchmark == "ping_pong") {
        return RunPingPongLatencyTest(argc - 1, argv + 1);
    }
    else if (benchmark == "bandwidth") {
        return RunBandwidthTest(argc - 1, argv + 1);
    }
    else {
        return -1;
    }
}

/******************************************************************************/
