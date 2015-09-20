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

//! perform a 1-factor ping pong latency test
void PingPongLatencyTest(api::Context& ctx) {

    // only work with first thread on this host.
    if (ctx.local_worker_id() != 0) return;

    net::Group& group = ctx.flow_control_channel().group();

    // a counter to count and match ping pongs.
    size_t counter = 0;

    for (size_t outer_repeat = 0;
         outer_repeat < outer_repeats; ++outer_repeat) {

        common::StatsTimer<true> timer;

        timer.Start();
        for (size_t inner_repeat = 0;
             inner_repeat < inner_repeats; inner_repeat++) {
            // perform 1-factor ping pongs (without barriers)
            for (size_t iter = 0; iter < ctx.num_hosts(); ++iter) {

                size_t peer = group.OneFactorPeer(iter);

                sLOG0 << "round iter" << iter
                      << "me" << ctx.host_rank() << "peer" << peer;

                if (ctx.host_rank() < peer) {
                    // send ping to peer
                    size_t value = counter++;
                    group.SendTo(peer, value);

                    // wait for ping
                    group.ReceiveFrom(peer, &value);
                    assert(value == counter);
                }
                else if (ctx.host_rank() > peer) {
                    // wait for ping
                    size_t value;
                    group.ReceiveFrom(peer, &value);
                    assert(value == counter);

                    // increment counters
                    counter++, value++;

                    // send pong
                    group.SendTo(peer, value);
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
                 << " inner_repeats=" << inner_repeats
                 << " ping_pongs=" << counter
                 << " time[us]=" << time
                 << " time_per_ping_pong[us]="
                 << static_cast<double>(time) / static_cast<double>(counter);
        }
    }
}

//! perform a 1-factor bandwidth test
void BandwidthTest(api::Context& ctx) {

    // only work with first thread on this host.
    if (ctx.local_worker_id() != 0) return;

    net::Group& group = ctx.flow_control_channel().group();

    // a counter to count and match messages
    size_t counter = 0;

    // data block to send or receive
    static const size_t block_count = 1024;
    static const size_t block_size = 16 * 1024 * 1024;
    std::vector<size_t> data_block(block_size / sizeof(size_t));
    std::fill(data_block.begin(), data_block.end(), 42u);

    for (size_t outer_repeat = 0;
         outer_repeat < outer_repeats; ++outer_repeat) {

        common::StatsTimer<true> timer;

        timer.Start();
        for (size_t inner_repeat = 0;
             inner_repeat < inner_repeats; inner_repeat++) {
            // perform 1-factor ping pongs (without barriers)
            for (size_t iter = 0; iter < ctx.num_hosts(); ++iter) {

                size_t peer_id = group.OneFactorPeer(iter);
                net::Connection& peer = group.connection(peer_id);

                sLOG0 << "round iter" << iter
                      << "me" << ctx.host_rank() << "peer" << peer;

                if (ctx.host_rank() < peer_id) {
                    common::StatsTimer<true> bwtimer(true);
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

                    bwtimer.Stop();

                    sLOG1 << "bandwidth" << ctx.host_rank() << "->" << peer_id
                          << ((block_count * block_size) /
                        (static_cast<double>(bwtimer.Microseconds()) * 1e-6)
                        / 1024.0 / 1024.0)
                          << "MiB/s"
                          << "time"
                          << (static_cast<double>(bwtimer.Microseconds()) * 1e-6);
                }
                else if (ctx.host_rank() > peer_id) {
                    // receive blocks from peer
                    for (size_t i = 0; i != block_count; ++i) {
                        peer.SyncRecv(data_block.data(), block_size);
                        die_unequal(data_block.front(), counter);
                        die_unequal(data_block.back(), counter);

                        ++counter;
                    }

                    // send ping
                    peer.Send(counter);
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
}

//! Network benchmarking.
void net_test(api::Context& ctx) {
    if (benchmark == "pingpong") {
        PingPongLatencyTest(ctx);
    }
    else if (benchmark == "bandwidth") {
        BandwidthTest(ctx);
    }
    else {
        die("Unknown benchmark " + benchmark);
    }
}

int main(int argc, char** argv) {

    common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    clp.AddUInt('r', "inner_repeats", inner_repeats,
                "Repeat inner experiment a number of times.");

    clp.AddUInt('R', "outer_repeats", outer_repeats,
                "Repeat whole experiment a number of times.");

    clp.AddParamString("benchmark", benchmark,
                       "name of benchmark to run:\n"
                       "  pingpong - 1-factor latency\n"
                       "  bandwidth - 1-factor full bandwidth");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    return api::Run(net_test);
}

/******************************************************************************/
