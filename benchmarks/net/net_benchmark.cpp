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
#include <thrill/common/matrix.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/common/string.hpp>

#include <string>
#include <utility>
#include <vector>

using namespace thrill; // NOLINT

std::string benchmark;

// matrix of measured latencies
using AggDouble = common::Aggregate<double>;
using AggMatrix = common::Matrix<AggDouble>;

/******************************************************************************/
//! perform a 1-factor ping pong latency test

class PingPongLatency
{
public:
    int Run(int argc, char* argv[]) {

        common::CmdlineParser clp;

        clp.AddUInt('R', "outer_repeats", outer_repeats_,
                    "Repeat whole experiment a number of times.");

        clp.AddParamUInt("iterations", iterations_,
                         "Repeat 1-factor iterations a number of times.");

        clp.AddUInt('r', "inner_repeats", inner_repeats_,
                    "Repeat inner experiment a number of times.");

        if (!clp.Process(argc, argv))
            return -1;

        return api::Run(
            [=](api::Context& ctx) {
                // make a copy of this (for local workers)
                PingPongLatency local = *this;
                return local.Test(ctx);
            });
    }

    void Test(api::Context& ctx);

    void Sender(api::Context& ctx, size_t peer, size_t iteration) {

        net::Group& group = ctx.flow_control_channel().group();

        // do an extra ping/pong round to synchronize.
        {
            // send ping to peer
            size_t value = counter_++;
            group.SendTo(peer, value);

            // wait for ping
            group.ReceiveFrom(peer, &value);
            die_unequal(value, counter_);
        }

        common::StatsTimer<true> inner_timer(true);

        for (size_t inner = 0; inner < inner_repeats_; ++inner) {

            // send ping to peer
            size_t value = counter_++;
            group.SendTo(peer, value);

            // wait for ping
            group.ReceiveFrom(peer, &value);
            die_unequal(value, counter_);
        }
        inner_timer.Stop();

        double avg =
            static_cast<double>(inner_timer.Microseconds()) /
            static_cast<double>(inner_repeats_);

        sLOG0 << "bandwidth" << ctx.host_rank() << "->" << peer
              << "iteration" << iteration
              << "latency" << avg;

        latency_(ctx.host_rank(), peer).Add(avg);
    }

    void Receiver(api::Context& ctx, size_t peer) {

        net::Group& group = ctx.flow_control_channel().group();

        for (size_t inner = 0; inner < inner_repeats_ + 1; ++inner) {

            // wait for ping
            size_t value;
            group.ReceiveFrom(peer, &value);
            die_unequal(value, counter_);

            // increment counter
            counter_++;

            // send pong
            group.SendTo(peer, counter_);
        }
    }

protected:
    //! whole experiment
    unsigned int outer_repeats_ = 1;

    //! iterations of 1-factor
    unsigned int iterations_ = 1;

    //! inner ping-pong repetitions
    unsigned int inner_repeats_ = 100;

    //! globally synchronized ping/pong counter to count and match ping pongs.
    size_t counter_ = 0;

    //! n x n matrix of measured latencies
    AggMatrix latency_;
};

void PingPongLatency::Test(api::Context& ctx) {

    // only work with first thread on this host.
    if (ctx.local_worker_id() != 0) return;

    net::Group& group = ctx.flow_control_channel().group();

    latency_ = AggMatrix(group.num_hosts());

    for (size_t outer_repeat = 0;
         outer_repeat < outer_repeats_; ++outer_repeat) {

        common::StatsTimer<true> timer;

        timer.Start();
        for (size_t iteration = 0; iteration < iterations_; iteration++) {
            // perform 1-factor ping pongs (without barriers)
            for (size_t round = 0; round < group.OneFactorSize(); ++round) {

                size_t peer = group.OneFactorPeer(round);

                sLOG0 << "round" << round
                      << "me" << ctx.host_rank() << "peer" << peer;

                if (ctx.host_rank() < peer) {
                    Sender(ctx, peer, iteration);
                    Receiver(ctx, peer);
                }
                else if (ctx.host_rank() > peer) {
                    Receiver(ctx, peer);
                    Sender(ctx, peer, iteration);
                }
                else {
                    // not participating in this round
                    counter_ += 2 * (inner_repeats_ + 1);
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
                 << " iterations=" << iterations_
                 << " inner_repeats=" << inner_repeats_
                 << " ping_pongs=" << counter_
                 << " time[us]=" << time
                 << " time_per_ping_pong[us]="
                 << static_cast<double>(time) / static_cast<double>(counter_);
        }
    }

    // reduce (add) matrix to root.
    group.ReduceToRoot(latency_);

    // print matrix
    if (ctx.my_rank() == 0) {
        for (size_t i = 0; i < latency_.rows(); ++i) {
            std::ostringstream os2;
            for (size_t j = 0; j < latency_.columns(); ++j) {
                os2 << common::str_sprintf(
                    "%8.1f/%8.3f",
                    latency_(i, j).Avg(), latency_(i, j).StdDev());
            }
            LOG1 << os2.str();
        }
    }
}

/******************************************************************************/
//! perform a 1-factor bandwidth test

class Bandwidth
{
public:
    int Run(int argc, char* argv[]) {

        common::CmdlineParser clp;

        clp.AddUInt('r', "inner_repeats", inner_repeats_,
                    "Repeat inner experiment a number of times.");

        clp.AddUInt('R', "outer_repeats", outer_repeats_,
                    "Repeat whole experiment a number of times.");

        clp.AddBytes('B', "block_size", block_size_,
                     "Block size used to transfered data (default: 2 MiB).");

        clp.AddParamBytes("size", data_size_,
                          "Amount of data transfered between peers (example: 1 GiB).");

        if (!clp.Process(argc, argv))
            return -1;

        return api::Run(
            [=](api::Context& ctx) {
                // make a copy of this (for local workers)
                Bandwidth local = *this;
                return local.Test(ctx);
            });
    }

    void Test(api::Context& ctx);

    void Sender(api::Context& ctx, size_t peer_id, size_t inner_repeat) {

        net::Group& group = ctx.flow_control_channel().group();
        net::Connection& peer = group.connection(peer_id);

        common::StatsTimer<true> inner_timer(true);
        // send blocks to peer
        for (size_t i = 0; i != block_count_; ++i) {
            data_block_.front() = counter_;
            data_block_.back() = counter_;
            ++counter_;
            peer.SyncSend(data_block_.data(), block_size_);
        }

        // wait for response pong
        size_t value;
        peer.Receive(&value);
        die_unequal(value, counter_);

        inner_timer.Stop();

        double bw =
            static_cast<double>(block_count_ * block_size_) /
            static_cast<double>(inner_timer.Microseconds()) *
            1e6 / 1024.0 / 1024.0;

        sLOG0 << "bandwidth" << ctx.host_rank() << "->" << peer_id
              << "inner_repeat" << inner_repeat
              << bw << "MiB/s"
              << "time"
              << (static_cast<double>(inner_timer.Microseconds()) * 1e-6);

        bandwidth_(ctx.host_rank(), peer_id).Add(bw);
    }

    void Receiver(api::Context& ctx, size_t peer_id) {
        net::Group& group = ctx.flow_control_channel().group();
        net::Connection& peer = group.connection(peer_id);

        // receive blocks from peer
        for (size_t i = 0; i != block_count_; ++i) {
            peer.SyncRecv(data_block_.data(), block_size_);
            die_unequal(data_block_.front(), counter_);
            die_unequal(data_block_.back(), counter_);

            ++counter_;
        }

        // send ping
        peer.Send(counter_);
    }

protected:
    //! whole experiment
    unsigned int outer_repeats_ = 1;

    //! inner repetitions
    unsigned int inner_repeats_ = 1;

    //! globally synchronized ping/pong counter to count and match packets.
    size_t counter_ = 0;

    //! total 1-1 transmission size
    uint64_t data_size_ = 1024 * 1024 * 1024llu;

    //! block size of transmission
    uint64_t block_size_ = 2 * 1024 * 1024;

    //! calculated number of blocks to send (rounded down)
    size_t block_count_;

    //! send and receive buffer
    std::vector<size_t> data_block_;

    //! n x n matrix of measured bandwidth
    AggMatrix bandwidth_;
};

void Bandwidth::Test(api::Context& ctx) {

    // only work with first thread on this host.
    if (ctx.local_worker_id() != 0) return;

    net::Group& group = ctx.flow_control_channel().group();

    bandwidth_ = AggMatrix(group.num_hosts());

    // data block to send or receive
    block_count_ = data_size_ / block_size_;
    data_block_.resize(block_size_ / sizeof(size_t), 42u);

    for (size_t outer_repeat = 0;
         outer_repeat < outer_repeats_; ++outer_repeat) {

        common::StatsTimer<true> timer;

        timer.Start();
        for (size_t inner_repeat = 0;
             inner_repeat < inner_repeats_; inner_repeat++) {
            // perform 1-factor ping pongs (without barriers)
            for (size_t round = 0; round < group.OneFactorSize(); ++round) {

                size_t peer = group.OneFactorPeer(round);

                sLOG0 << "round" << round
                      << "me" << ctx.host_rank() << "peer_id" << peer;

                if (ctx.host_rank() < peer) {
                    Sender(ctx, peer, inner_repeat);
                    Receiver(ctx, peer);
                }
                else if (ctx.host_rank() > peer) {
                    Receiver(ctx, peer);
                    Sender(ctx, peer, inner_repeat);
                }
                else {
                    // not participating in this round
                    counter_ += 2 * block_count_;
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
                 << " inner_repeats=" << inner_repeats_
                 << " ping_pongs=" << counter_
                 << " time[us]=" << time
                 << " time_per_ping_pong[us]="
                 << static_cast<double>(time) / static_cast<double>(counter_);
        }
    }

    // reduce (add) matrix to root.
    group.ReduceToRoot(bandwidth_);

    // print matrix
    if (ctx.my_rank() == 0) {
        for (size_t i = 0; i < bandwidth_.rows(); ++i) {
            std::ostringstream os2;
            for (size_t j = 0; j < bandwidth_.columns(); ++j) {
                os2 << common::str_sprintf(
                    "%8.1f/%8.3f",
                    bandwidth_(i, j).Avg(), bandwidth_(i, j).StdDev());
            }
            LOG1 << os2.str();
        }
    }
}

/******************************************************************************/

void Usage(const char* argv0) {
    std::cout
        << "Usage: " << argv0 << " <benchmark>" << std::endl
        << std::endl
        << "    ping_pong  - 1-factor latency" << std::endl
        << "    bandwidth  - 1-factor bandwidth" << std::endl
        << std::endl;
}

int main(int argc, char** argv) {

    if (argc <= 1) {
        Usage(argv[0]);
        return 0;
    }

    benchmark = argv[1];

    if (benchmark == "ping_pong") {
        return PingPongLatency().Run(argc - 1, argv + 1);
    }
    else if (benchmark == "bandwidth") {
        return Bandwidth().Run(argc - 1, argv + 1);
    }
    else {
        Usage(argv[0]);
        return -1;
    }
}

/******************************************************************************/
