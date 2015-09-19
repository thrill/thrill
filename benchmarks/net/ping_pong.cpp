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

#include <random>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

unsigned int iterations = 1000;
unsigned int repeats = 1;

using namespace thrill;

//! Network benchmarking.
int net_test(api::Context& ctx) {
    if (ctx.workers_per_host() != 1) {
        die("Net benchmarks work only with one worker per host.");
    }

    auto& flow = ctx.flow_control_channel();
    net::Group& group = flow.group();

    for (size_t r = 0; r < repeats; ++r) {
        //! a counter to count and match ping pongs.
        size_t it = 0;

        common::StatsTimer<true> t;

        size_t p = ctx.num_hosts();

        t.Start();
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
        t.Stop();

        size_t time = t.Microseconds();
        // calculate maximum time.
        time = flow.AllReduce(time, common::maximum<size_t>());

        if (ctx.my_rank() == 0) {
            LOG1 << "RESULT"
                 << " hosts=" << p
                 << " iterations=" << iterations
                 << " ping_pongs=" << it
                 << " time[us]=" << time
                 << " time_per_ping_pong[us]=" << static_cast<double>(time) / it;
        }
    }

    return 0;
}

int main(int argc, char** argv) {

    common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    clp.AddUInt('i', "iterations", iterations,
                "Count of iterations");

    clp.AddUInt('r', "repeats", repeats,
                "Repeat experiment a number of times.");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    return api::Run(net_test);
}

/******************************************************************************/
