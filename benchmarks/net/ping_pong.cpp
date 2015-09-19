/*******************************************************************************
 * benchmarks/net/ping_pong.cpp
 *
 * 1-factor ping pong latency benchmark
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

unsigned int iterations = 1000;
unsigned int repeats = 1;

using namespace thrill;

//! perform a 1-factor ping pong latency test
void PingPongLatencyTest(api::Context& ctx) {

    // only work with first thread on this host.
    if (ctx.local_worker_id() != 0) return;

    net::Group& group = ctx.flow_control_channel().group();

    //! a counter to count and match ping pongs.
    size_t it = 0;

    for (size_t repeat = 0; repeat < repeats; ++repeat) {

        common::StatsTimer<true> timer;

        size_t p = ctx.num_hosts();

        timer.Start();
        for (size_t i = 0; i < iterations; i++) {
            // perform 1-factor ping pongs (without barriers)
            size_t me = ctx.host_rank();
            for (size_t i = 0; i < p; ++i) {

                size_t peer = group.OneFactorPeer(i);

                sLOG0 << "round i" << i << "me" << me << "peer" << peer;

                if (me < peer) {
                    // send ping to peer
                    size_t value = it++;
                    group.SendTo(peer, value);

                    // wait for ping
                    group.ReceiveFrom(peer, &value);
                    assert(value == it);
                }
                else if (me > peer) {
                    // wait for ping
                    size_t value;
                    group.ReceiveFrom(peer, &value);
                    assert(value == it);

                    // increment counters
                    it++, value++;

                    // send pong
                    group.SendTo(peer, value);
                }
                else {
                    // not participating in this round
                    ++it;
                }
            }
        }
        timer.Stop();

        size_t time = timer.Microseconds();
        // calculate maximum time.
        group.AllReduce(time, common::maximum<size_t>());

        if (ctx.my_rank() == 0) {
            LOG1 << "RESULT"
                 << " hosts=" << ctx.num_hosts()
                 << " repeat=" << repeat
                 << " iterations=" << iterations
                 << " ping_pongs=" << it
                 << " time[us]=" << time
                 << " time_per_ping_pong[us]=" << static_cast<double>(time) / it;
        }
    }
}

//! Network benchmarking.
void net_test(api::Context& ctx) {
    if (benchmark == "pingpong") {
        PingPongLatencyTest(ctx);
    }
    else {
        die("Unknown benchmark " + benchmark);
    }
}

int main(int argc, char** argv) {

    common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    clp.AddUInt('i', "iterations", iterations,
                "Count of iterations");

    clp.AddUInt('r', "repeats", repeats,
                "Repeat experiment a number of times.");

    clp.AddParamString("benchmark", benchmark,
                       "name of benchmark to run:\n"
                       "  pingpong - latency test");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    return api::Run(net_test);
}

/******************************************************************************/
