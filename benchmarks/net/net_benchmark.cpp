/*******************************************************************************
 * benchmarks/net/net_benchmark.cpp
 *
 * Network backend benchmarks:
 * - 1-factor ping pong latency benchmark
 * - 1-factor full bandwidth test
 * - fcc Broadcast
 * - fcc PrefixSum
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/context.hpp>
#include <thrill/common/aggregate.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/matrix.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/common/string.hpp>
#include <thrill/net/dispatcher.hpp>
#include <tlx/cmdline_parser.hpp>

#include <iostream>
#include <string>
#include <utility>
#include <vector>

using namespace thrill; // NOLINT

std::string benchmark;

//! calculate MiB/s given byte size and microsecond time.
double CalcMiBs(size_t bytes, const std::chrono::microseconds::rep& microsec) {
    return static_cast<double>(bytes) / 1024.0 / 1024.0
           / static_cast<double>(microsec) * 1e6;
}

//! calculate MiB/s given byte size and timer.
double CalcMiBs(size_t bytes, const common::StatsTimer& timer) {
    return CalcMiBs(bytes, timer.Microseconds());
}

// matrix of measured latencies or bandwidths
using AggDouble = tlx::Aggregate<double>;
using AggMatrix = common::Matrix<AggDouble>;

//! print avg/stddev matrix
void PrintMatrix(const AggMatrix& m) {
    for (size_t i = 0; i < m.rows(); ++i) {
        std::ostringstream os;
        for (size_t j = 0; j < m.columns(); ++j) {
            os << tlx::ssprintf(
                "%8.1f/%8.3f", m(i, j).avg(), m(i, j).stdev());
        }
        LOG1 << os.str();
    }
}

/******************************************************************************/
//! perform a 1-factor ping pong latency test

class PingPongLatency
{
public:
    int Run(int argc, char* argv[]) {

        tlx::CmdlineParser clp;

        clp.add_unsigned('R', "outer_repeats", outer_repeats_,
                         "Repeat whole experiment a number of times.");

        clp.add_param_unsigned("iterations", iterations_,
                               "Repeat 1-factor iterations a number of times.");

        clp.add_unsigned('r', "inner_repeats", inner_repeats_,
                         "Repeat inner experiment a number of times.");

        if (!clp.process(argc, argv)) return -1;

        return api::Run(
            [=](api::Context& ctx) {
                // make a copy of this for local workers
                PingPongLatency local = *this;
                return local.Test(ctx);
            });
    }

    void Test(api::Context& ctx);

    void Sender(api::Context& ctx, size_t peer, size_t iteration) {

        net::Group& group = ctx.net.group();

        // do an extra ping/pong round to synchronize.
        {
            // send ping to peer
            size_t value = counter_++;
            group.SendTo(peer, value);

            // wait for ping
            group.ReceiveFrom(peer, &value);
            die_unequal(value, counter_);
        }

        common::StatsTimerStart inner_timer;

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

        latency_(ctx.host_rank(), peer).add(avg);
    }

    void Receiver(api::Context& ctx, size_t peer) {

        net::Group& group = ctx.net.group();

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

private:
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

    net::Group& group = ctx.net.group();

    latency_ = AggMatrix(group.num_hosts());

    for (size_t outer_repeat = 0;
         outer_repeat < outer_repeats_; ++outer_repeat) {

        common::StatsTimerStopped timer;

        timer.Start();
        counter_ = 0;
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
            std::cout
                << "RESULT"
                << " benchmark=" << benchmark
                << " hosts=" << ctx.num_hosts()
                << " outer_repeat=" << outer_repeat
                << " iterations=" << iterations_
                << " inner_repeats=" << inner_repeats_
                << " ping_pongs=" << counter_
                << " time[us]=" << time
                << " time_per_ping_pong[us]="
                << static_cast<double>(time) / static_cast<double>(counter_)
                << std::endl;
        }
    }

    // reduce (add) matrix to root.
    group.Reduce(latency_);

    // print matrix
    if (ctx.my_rank() == 0)
        PrintMatrix(latency_);
}

/******************************************************************************/
//! perform a 1-factor bandwidth test

class Bandwidth
{
public:
    int Run(int argc, char* argv[]) {

        tlx::CmdlineParser clp;

        clp.add_unsigned('r', "inner_repeats", inner_repeats_,
                         "Repeat inner experiment a number of times.");

        clp.add_unsigned('R', "outer_repeats", outer_repeats_,
                         "Repeat whole experiment a number of times.");

        clp.add_bytes('B', "block_size", block_size_,
                      "Block size used to transfered data (default: 2 MiB).");

        clp.add_param_bytes("size", data_size_,
                            "Amount of data transfered between peers (example: 1 GiB).");

        if (!clp.process(argc, argv)) return -1;

        return api::Run(
            [=](api::Context& ctx) {
                // make a copy of this for local workers
                Bandwidth local = *this;
                return local.Test(ctx);
            });
    }

    void Test(api::Context& ctx);

    void Sender(api::Context& ctx, size_t peer_id, size_t inner_repeat) {

        net::Group& group = ctx.net.group();
        net::Connection& peer = group.connection(peer_id);

        common::StatsTimerStart inner_timer;
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

        double bw = CalcMiBs(block_count_ * block_size_, inner_timer);

        sLOG0 << "bandwidth" << ctx.host_rank() << "->" << peer_id
              << "inner_repeat" << inner_repeat
              << bw << "MiB/s"
              << "time"
              << (static_cast<double>(inner_timer.Microseconds()) * 1e-6);

        bandwidth_(ctx.host_rank(), peer_id).add(bw);
    }

    void Receiver(api::Context& ctx, size_t peer_id) {
        net::Group& group = ctx.net.group();
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

private:
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

    net::Group& group = ctx.net.group();

    bandwidth_ = AggMatrix(group.num_hosts());

    // data block to send or receive
    block_count_ = data_size_ / block_size_;
    data_block_.resize(block_size_ / sizeof(size_t), 42u);

    for (size_t outer_repeat = 0;
         outer_repeat < outer_repeats_; ++outer_repeat) {

        common::StatsTimerStopped timer;

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
            std::cout
                << "RESULT"
                << " benchmark=" << benchmark
                << " hosts=" << ctx.num_hosts()
                << " outer_repeat=" << outer_repeat
                << " inner_repeats=" << inner_repeats_
                << " time[us]=" << time
                << " time_per_ping_pong[us]="
                << static_cast<double>(time) / static_cast<double>(counter_)
                << std::endl;
        }
    }

    // reduce (add) matrix to root.
    group.Reduce(bandwidth_);

    // print matrix
    if (ctx.my_rank() == 0)
        PrintMatrix(bandwidth_);
}

/******************************************************************************/

class Broadcast
{
public:
    int Run(int argc, char* argv[]) {

        tlx::CmdlineParser clp;

        clp.add_unsigned('r', "inner_repeats", inner_repeats_,
                         "Repeat inner experiment a number of times.");

        clp.add_unsigned('R', "outer_repeats", outer_repeats_,
                         "Repeat whole experiment a number of times.");

        if (!clp.process(argc, argv)) return -1;

        return api::Run(
            [=](api::Context& ctx) {
                // make a copy of this for local workers
                Broadcast local = *this;
                return local.Test(ctx);
            });
    }

    void Test(api::Context& ctx) {

        for (size_t outer = 0; outer < outer_repeats_; ++outer) {

            common::StatsTimerStopped t;

            size_t dummy = +4915221495089;

            t.Start();
            for (size_t inner = 0; inner < inner_repeats_; ++inner) {
                dummy = ctx.net.Broadcast(dummy);
            }
            t.Stop();

            size_t n = ctx.num_workers();
            size_t time = t.Microseconds();
            // calculate maximum time.
            time = ctx.net.AllReduce(time, common::maximum<size_t>());

            if (ctx.my_rank() == 0) {
                std::cout
                    << "RESULT"
                    << " datatype=" << "size_t"
                    << " operation=" << "broadcast"
                    << " workers=" << n
                    << " inner_repeats=" << inner_repeats_
                    << " time[us]=" << time
                    << " time_per_op[us]="
                    << static_cast<double>(time) / inner_repeats_
                    << std::endl;
            }
        }
    }

private:
    //! whole experiment
    unsigned int outer_repeats_ = 1;

    //! inner repetitions
    unsigned int inner_repeats_ = 200;
};

/******************************************************************************/

class PrefixSum
{
public:
    int Run(int argc, char* argv[]) {

        tlx::CmdlineParser clp;

        clp.add_unsigned('r', "inner_repeats", inner_repeats_,
                         "Repeat inner experiment a number of times.");

        clp.add_unsigned('R', "outer_repeats", outer_repeats_,
                         "Repeat whole experiment a number of times.");

        if (!clp.process(argc, argv)) return -1;

        return api::Run(
            [=](api::Context& ctx) {
                // make a copy of this for local workers
                PrefixSum local = *this;
                return local.Test(ctx);
            });
    }

    void Test(api::Context& ctx) {

        for (size_t outer = 0; outer < outer_repeats_; ++outer) {

            common::StatsTimerStopped t;

            t.Start();
            for (size_t inner = 0; inner < inner_repeats_; ++inner) {
                // prefixsum a different value in each iteration
                size_t value = inner + ctx.my_rank();
                value = ctx.net.PrefixSum(value);
                die_unequal(value,
                            inner * (ctx.my_rank() + 1)
                            + ctx.my_rank() * (ctx.my_rank() + 1) / 2);
            }
            t.Stop();

            size_t n = ctx.num_workers();
            size_t time = t.Microseconds();
            // calculate maximum time.
            time = ctx.net.AllReduce(time, common::maximum<size_t>());

            if (ctx.my_rank() == 0) {
                std::cout
                    << "RESULT"
                    << " datatype=" << "size_t"
                    << " operation=" << "prefixsum"
                    << " workers=" << n
                    << " inner_repeats=" << inner_repeats_
                    << " time[us]=" << time
                    << " time_per_op[us]="
                    << static_cast<double>(time) / inner_repeats_
                    << std::endl;
            }
        }
    }

private:
    //! whole experiment
    unsigned int outer_repeats_ = 1;

    //! inner repetitions
    unsigned int inner_repeats_ = 200;
};

/******************************************************************************/

class AllReduce
{
public:
    int Run(int argc, char* argv[]) {

        tlx::CmdlineParser clp;

        clp.add_unsigned('r', "inner_repeats", inner_repeats_,
                         "Repeat inner experiment a number of times.");

        clp.add_unsigned('R', "outer_repeats", outer_repeats_,
                         "Repeat whole experiment a number of times.");

        if (!clp.process(argc, argv)) return -1;

        return api::Run(
            [=](api::Context& ctx) {
                // make a copy of this for local workers
                AllReduce local = *this;
                return local.Test(ctx);
            });
    }

    void Test(api::Context& ctx) {

        for (size_t outer = 0; outer < outer_repeats_; ++outer) {

            common::StatsTimerStopped t;

            size_t n = ctx.num_workers();

            t.Start();
            for (size_t inner = 0; inner < inner_repeats_; ++inner) {
                // allreduce a different value in each iteration
                size_t value = inner + ctx.my_rank();
                value = ctx.net.AllReduce(value);
                size_t expected = (n + inner) * ((n + inner) - 1) / 2 - inner * (inner - 1) / 2;
                die_unequal(value, expected);
            }
            t.Stop();

            size_t time = t.Microseconds();
            // calculate maximum time.
            time = ctx.net.AllReduce(time, common::maximum<size_t>());

            if (ctx.my_rank() == 0) {
                std::cout
                    << "RESULT"
                    << " datatype=" << "size_t"
                    << " operation=" << "allreduce"
                    << " workers=" << n
                    << " inner_repeats=" << inner_repeats_
                    << " time[us]=" << time
                    << " time_per_op[us]="
                    << static_cast<double>(time) / inner_repeats_
                    << std::endl;
            }
        }
    }

private:
    //! whole experiment
    unsigned int outer_repeats_ = 1;

    //! inner repetitions
    unsigned int inner_repeats_ = 200;
};

/******************************************************************************/

class RandomBlocks
{
    static constexpr bool debug = false;

public:
    int Run(int argc, char* argv[]) {

        tlx::CmdlineParser clp;

        clp.add_bytes('b', "block_size", block_size_,
                      "Size of blocks transmitted, default: 2 MiB");

        clp.add_unsigned('l', "limit_active", limit_active_,
                         "Number of simultaneous active requests, default: 16");

        clp.add_unsigned('r', "request", num_requests_,
                         "Number of blocks transmitted across all hosts, default: 100");

        clp.add_unsigned('R', "outer_repeats", outer_repeats_,
                         "Repeat whole experiment a number of times.");

        if (!clp.process(argc, argv)) return -1;

        return api::Run(
            [=](api::Context& ctx) {
                // make a copy of this for local workers
                RandomBlocks local = *this;
                return local.Test(ctx);
            });
    }

    void Test(api::Context& ctx) {

        common::StatsTimerStopped t;

        // only work with first thread on this host.
        if (ctx.local_worker_id() == 0)
        {
            mem::Manager mem_manager(nullptr, "Dispatcher");

            group_ = &ctx.net.group();
            std::unique_ptr<net::Dispatcher> dispatcher =
                group_->ConstructDispatcher();
            dispatcher_ = dispatcher.get();

            t.Start();

            for (size_t outer = 0; outer < outer_repeats_; ++outer)
            {
                rnd_ = std::default_random_engine(123456);

                active_ = 0;
                remaining_requests_ = num_requests_;

                while (active_ < limit_active_ && remaining_requests_ > 0)
                {
                    if (MaybeStartRequest()) {
                        ++active_;
                    }
                }

                dispatcher_->Loop();
            }

            t.Stop();

            // must clean up dispatcher prior to using group for other things.
        }

        size_t time = t.Microseconds();
        // calculate maximum time.
        time = ctx.net.AllReduce(time, common::maximum<size_t>());

        if (ctx.my_rank() == 0) {
            std::cout
                << "RESULT"
                << " operation=" << "rblocks"
                << " hosts=" << group_->num_hosts()
                << " requests=" << num_requests_
                << " block_size=" << block_size_
                << " limit_active=" << limit_active_
                << " time[us]=" << time
                << " time_per_op[us]="
                << static_cast<double>(time) / num_requests_
                << " total_bytes=" << block_size_ * num_requests_
                << " total_bandwidth[MiB/s]="
                << CalcMiBs(block_size_ * num_requests_, time)
                << std::endl;
        }
    }

    void OnComplete() {
        --active_;

        LOG << "OnComplete active_=" << active_
            << " remaining_requests_=" << remaining_requests_;

        while (remaining_requests_ > 0) {
            if (MaybeStartRequest()) {
                ++active_;
                break;
            }
        }

        if (remaining_requests_ == 0 && active_ == 0) {
            LOG << "terminate";
            dispatcher_->Terminate();
        }
    }

    bool MaybeStartRequest() {
        // pick next random send/recv pairs
        size_t s_rank = rnd_() % group_->num_hosts();
        size_t r_rank = rnd_() % group_->num_hosts();
        size_t my_rank = group_->my_host_rank();

        if (s_rank == r_rank)
            return false;

        // some processor pairs is going to do a request.
        --remaining_requests_;

        if (my_rank == s_rank) {
            // allocate block and fill with junk
            net::Buffer block(block_size_);

            size_t* sbuffer = reinterpret_cast<size_t*>(block.data());
            for (size_t i = 0; i < block_size_ / sizeof(size_t); ++i)
                sbuffer[i] = i;
            void* p = (void*)block.data();

            dispatcher_->AsyncWrite(
                group_->connection(r_rank), /* seq */ 0, std::move(block),
                [this, p](net::Connection& /* c */) {
                    LOG << "AsyncWrite complete " << p;
                    OnComplete();
                });

            return true;
        }
        else if (my_rank == r_rank) {

            dispatcher_->AsyncRead(
                group_->connection(s_rank), /* seq */ 0, block_size_,
                [this](net::Connection& /* c */, net::Buffer&& block) {
                    LOG << "AsyncRead complete " << (void*)block.data();
                    OnComplete();
                });

            return true;
        }

        return false;
    }

protected:
    //! whole experiment repetitions
    unsigned int outer_repeats_ = 1;

    //! total number of blocks transmitted across all hosts
    unsigned int num_requests_ = 100;

    //! size of blocks transmitted
    uint64_t block_size_ = 2 * 1024 * 1024;

    //! limit on the number of simultaneous active requests
    unsigned int limit_active_ = 16;

    //! communication group
    net::Group* group_;

    //! async dispatcher
    net::Dispatcher* dispatcher_;

    //! currently active requests
    size_t active_;

    //! remaining requests
    size_t remaining_requests_;

    //! random generator
    std::default_random_engine rnd_;
};

/******************************************************************************/

class RandomBlocksSeries : public RandomBlocks
{
    static constexpr bool debug = false;

public:
    using Super = RandomBlocks;

    int Run(int argc, char* argv[]) {

        tlx::CmdlineParser clp;

        clp.add_bytes('s', "size", total_bytes_,
                      "Total bytes transfered per experiment, default: 128 MiB");

        clp.add_bytes('b', "min_block_size", min_block_size_,
                      "Minimum size of blocks transmitted, default: 512 KiB");

        clp.add_bytes('B', "max_block_size", max_block_size_,
                      "Maximum size of blocks transmitted, default: 8 MiB");

        clp.add_bytes('l', "min_limit_active", min_limit_active_,
                      "Minimum number of simultaneous active requests, default: 16");

        clp.add_bytes('L', "max_limit_active", max_limit_active_,
                      "maximum number of simultaneous active requests, default: 512");

        if (!clp.process(argc, argv)) return -1;

        return api::Run(
            [=](api::Context& ctx) {
                // make a copy of this for local workers
                RandomBlocksSeries local = *this;
                return local.Test(ctx);
            });
    }

    void Test(api::Context& ctx) {

        for (size_t block_size = min_block_size_;
             block_size <= max_block_size_; block_size *= 2) {

            for (size_t limit_active = min_limit_active_;
                 limit_active <= max_limit_active_; limit_active *= 2) {

                Super::num_requests_ = total_bytes_ / block_size;
                Super::block_size_ = block_size;
                Super::limit_active_ = limit_active;
                Super::Test(ctx);
            }
        }
    }

protected:
    //! total bytes transfered
    uint64_t total_bytes_ = 128 * 1024 * 1024;

    //! size of blocks transmitted minimum
    uint64_t min_block_size_ = 512 * 1024;

    //! size of blocks transmitted maximum
    uint64_t max_block_size_ = 8 * 1024 * 1024;

    //! min limit on the number of simultaneous active requests
    unsigned int min_limit_active_ = 16;

    //! max limit on the number of simultaneous active requests
    unsigned int max_limit_active_ = 512;
};

/******************************************************************************/

void Usage(const char* argv0) {
    std::cout
        << "Usage: " << argv0 << " <benchmark>" << std::endl
        << std::endl
        << "    ping_pong  - 1-factor latency" << std::endl
        << "    bandwidth  - 1-factor bandwidth" << std::endl
        << "    broadcast  - FCC Broadcast operation" << std::endl
        << "    prefixsum  - FCC PrefixSum operation" << std::endl
        << "    allreduce  - FCC PrefixSum operation" << std::endl
        << "    rblocks    - random block transmissions" << std::endl
        << "    rblocks_series - series of rblocks experiments" << std::endl
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
    else if (benchmark == "broadcast") {
        return Broadcast().Run(argc - 1, argv + 1);
    }
    else if (benchmark == "prefixsum") {
        return PrefixSum().Run(argc - 1, argv + 1);
    }
    else if (benchmark == "allreduce") {
        return AllReduce().Run(argc - 1, argv + 1);
    }
    else if (benchmark == "rblocks") {
        return RandomBlocks().Run(argc - 1, argv + 1);
    }
    else if (benchmark == "rblocks_series") {
        return RandomBlocksSeries().Run(argc - 1, argv + 1);
    }
    else {
        Usage(argv[0]);
        return -1;
    }
}

/******************************************************************************/
