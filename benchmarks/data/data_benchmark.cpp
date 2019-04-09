/*******************************************************************************
 * benchmarks/data/data_benchmark.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/context.hpp>
#include <thrill/common/aggregate.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/matrix.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/data/block_queue.hpp>
#include <tlx/cmdline_parser.hpp>
#include <tlx/thread_pool.hpp>

#include <algorithm>
#include <iostream>
#include <random>
#include <string>
#include <tuple>
#include <vector>

#include "data_generators.hpp"

using namespace thrill; // NOLINT
using common::StatsTimer;
using common::StatsTimerStart;
using common::StatsTimerStopped;

using pair_type = std::tuple<std::string, size_t>;
using triple_type = std::tuple<size_t, size_t, size_t>;

//! calculate MiB/s given byte size and microsecond time.
double CalcMiBs(size_t bytes, const std::chrono::microseconds::rep& microsec) {
    return static_cast<double>(bytes) / 1024.0 / 1024.0
           / static_cast<double>(microsec) * 1e6;
}

//! calculate MiB/s given byte size and timer.
double CalcMiBs(size_t bytes, const StatsTimer& timer) {
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
//! Base Class for Experiments with Generator<> instances

class DataGeneratorExperiment
{
public:
    void AddCmdline(tlx::CmdlineParser& clp) {

        clp.add_bytes('b', "bytes", bytes_,
                      "number of bytes to process (default 1024)");

        clp.add_size_t('s', "block_size", data::default_block_size,
                       "block size (system default)");

        clp.add_bytes('l', "lower", min_size_,
                      "lower bound for variable element length (default 1)");

        clp.add_bytes('u', "upper", max_size_,
                      "upper bound for variable element length (default 100)");

        clp.add_param_string("type", type_as_string_,
                             "data type (size_t, string, pair, triple)");
    }

protected:
    //! total bytes to process (default: 1024)
    uint64_t bytes_ = 1024;

    //! lower bound for variable element length (default 1)
    uint64_t min_size_ = 1;

    //! upper bound for variable element length (default 100)
    uint64_t max_size_ = 100;

    //! experiment data type
    std::string type_as_string_;
};

/******************************************************************************/
//! Writes and reads random elements from a file.  Elements are genreated before
//! the timer startet Number of elements depends on the number of bytes.  one
//! RESULT line will be printed for each iteration All iterations use the same
//! generated data.  Variable-length elements range between 1 and 100 bytes per
//! default

class FileExperiment : public DataGeneratorExperiment
{
public:
    int Run(int argc, char* argv[]) {

        tlx::CmdlineParser clp;

        clp.set_description("thrill::data benchmark for disk I/O");
        clp.set_author("Tobias Sturm <mail@tobiassturm.de>");

        DataGeneratorExperiment::AddCmdline(clp);

        clp.add_unsigned(
            'n', "iterations", iterations_, "Iterations (default: 1)");

        clp.add_param_string("reader", reader_type_,
                             "reader type (consume, keep)");

        if (!clp.process(argc, argv)) return -1;

        api::RunLocalSameThread(
            [=](api::Context& ctx) {
                if (type_as_string_ == "size_t")
                    Test<size_t>(ctx);
                else if (type_as_string_ == "string")
                    Test<std::string>(ctx);
                else if (type_as_string_ == "pair")
                    Test<pair_type>(ctx);
                else if (type_as_string_ == "triple")
                    Test<triple_type>(ctx);
                else
                    abort();
            });

        return 0;
    }

    template <typename Type>
    void Test(api::Context& ctx) {

        if (reader_type_ != "consume" && reader_type_ != "keep")
            abort();
        bool consume = reader_type_ == "consume";

        for (unsigned i = 0; i < iterations_; i++) {
            auto file = ctx.GetFile(nullptr);
            auto writer = file.GetWriter();
            auto data = Generator<Type>(bytes_, min_size_, max_size_);

            StatsTimerStart write_timer;
            while (data.HasNext()) {
                writer.Put(data.Next());
            }
            writer.Close();
            write_timer.Stop();

            StatsTimerStart read_timer;
            auto reader = file.GetReader(consume);
            while (reader.HasNext())
                reader.Next<Type>();
            read_timer.Stop();

            LOG1 << "RESULT"
                 << " experiment=" << "file"
                 << " datatype=" << type_as_string_
                 << " size=" << bytes_
                 << " block_size=" << data::default_block_size
                 << " avg_element_size="
                 << static_cast<double>(min_size_ + max_size_) / 2.0
                 << " reader=" << reader_type_
                 << " write_time=" << write_timer
                 << " read_time=" << read_timer
                 << " write_speed_MiBs=" << CalcMiBs(bytes_, write_timer)
                 << " read_speed_MiBs=" << CalcMiBs(bytes_, read_timer);
        }
    }

private:
    //! number of iterations to run
    unsigned iterations_ = 1;

    //! reader type: consume or keep
    std::string reader_type_;
};

/******************************************************************************/
//! Writes and reads random elements to / from block queue with 2 threads
//! Elements are genreated before the timer startet Number of elements depends
//! on the number of bytes.  one RESULT line will be printed for each iteration
//! All iterations use the same generated data.  Variable-length elements range
//! between 1 and 100 bytes per default

class BlockQueueExperiment : public DataGeneratorExperiment
{
public:
    int Run(int argc, char* argv[]) {

        tlx::CmdlineParser clp;

        clp.set_description("thrill::data benchmark for disk I/O");
        clp.set_author("Tobias Sturm <mail@tobiassturm.de>");

        DataGeneratorExperiment::AddCmdline(clp);

        clp.add_unsigned(
            'n', "iterations", iterations_, "Iterations (default: 1)");

        clp.add_unsigned(
            't', "threads", num_threads_, "Number of threads (default: 1)");

        clp.add_param_string("reader", reader_type_,
                             "reader type (consume, keep)");

        if (!clp.process(argc, argv)) return -1;

        api::RunLocalSameThread(
            [=](api::Context& ctx) {
                if (type_as_string_ == "size_t")
                    Test<size_t>(ctx);
                else if (type_as_string_ == "string")
                    Test<std::string>(ctx);
                else if (type_as_string_ == "pair")
                    Test<pair_type>(ctx);
                else if (type_as_string_ == "triple")
                    Test<triple_type>(ctx);
                else
                    abort();
            });

        return 0;
    }

    template <typename Type>
    void Test(api::Context& ctx) {

        if (reader_type_ != "consume" && reader_type_ != "keep")
            abort();
        bool consume = reader_type_ == "consume";

        tlx::ThreadPool pool(num_threads_ + 1);
        for (unsigned i = 0; i < iterations_; i++) {
            auto queue = data::BlockQueue(ctx.block_pool(), 0, /* dia_id */ 0);
            auto data = Generator<Type>(bytes_, min_size_, max_size_);

            StatsTimerStopped write_timer;
            pool.enqueue(
                [&]() {
                    auto writer = queue.GetWriter();
                    write_timer.Start();
                    while (data.HasNext()) {
                        writer.Put(data.Next());
                    }
                    writer.Close();
                    write_timer.Stop();
                });

            std::chrono::microseconds::rep read_time = 0;

            for (size_t thread = 0; thread < num_threads_; thread++) {
                pool.enqueue(
                    [&]() {
                        StatsTimerStart read_timer;
                        auto reader = queue.GetReader(consume, 0);
                        while (reader.HasNext())
                            reader.Next<Type>();
                        read_timer.Stop();
                        // REVIEW(ts): this is a data race!
                        read_time = std::max(read_time, read_timer.Microseconds());
                    });
            }
            pool.loop_until_empty();
            LOG1 << "RESULT"
                 << " experiment=" << "block_queue"
                 << " workers=" << ctx.num_workers()
                 << " hosts=" << ctx.num_hosts()
                 << " datatype=" << type_as_string_
                 << " size=" << bytes_
                 << " block_size=" << data::default_block_size
                 << " avg_element_size="
                 << static_cast<double>(min_size_ + max_size_) / 2.0
                 << " reader=" << reader_type_
                 << " write_time=" << write_timer.SecondsDouble()
                 << " read_time=" << read_time
                 << " write_speed_MiBs=" << CalcMiBs(bytes_, write_timer)
                 << " read_speed_MiBs=" << CalcMiBs(bytes_, read_time)
                 << " threads=" << num_threads_;
        }
    }

private:
    //! number of iterations to run
    unsigned iterations_ = 1;

    //! reader type: consume or keep
    std::string reader_type_;

    //! number of threads used
    unsigned num_threads_ = 1;
};

/******************************************************************************/

template <typename Stream>
class StreamOneFactorExperiment : public DataGeneratorExperiment
{
    static constexpr bool debug = true;

public:
    int Run(int argc, char* argv[]) {

        tlx::CmdlineParser clp;

        DataGeneratorExperiment::AddCmdline(clp);

        clp.add_unsigned('r', "inner_repeats", inner_repeats_,
                         "Repeat inner experiment a number of times.");

        clp.add_unsigned('R', "outer_repeats", outer_repeats_,
                         "Repeat whole experiment a number of times.");

        clp.add_param_string("reader", reader_type_,
                             "reader type (consume, keep)");

        if (!clp.process(argc, argv)) return -1;

        if (reader_type_ != "consume" && reader_type_ != "keep")
            abort();
        consume_ = (reader_type_ == "consume");

        api::Run(
            [=](api::Context& ctx) {
                // make a copy of this for local workers
                StreamOneFactorExperiment<Stream> local = *this;

                if (type_as_string_ == "size_t")
                    local.Test<size_t>(ctx);
                else if (type_as_string_ == "string")
                    local.Test<std::string>(ctx);
                else if (type_as_string_ == "pair")
                    local.Test<pair_type>(ctx);
                else if (type_as_string_ == "triple")
                    local.Test<triple_type>(ctx);
                else
                    abort();
            });

        return 0;
    }

    template <typename Type>
    void Test(api::Context& ctx);

    template <typename Type>
    void Sender(api::Context& ctx, size_t peer_id, size_t inner_repeat) {

        auto stream = ctx.GetNewStream<Stream>(/* dia_id */ 0);
        auto data = Generator<Type>(bytes_, min_size_, max_size_);

        StatsTimerStart write_timer;
        {
            auto writers = stream->GetWriters();
            while (data.HasNext())
                writers[peer_id].Put(data.Next());
        }
        write_timer.Stop();

        {
            // this opens and closes the readers. this must be done, otherwise
            // the reader will wait infinitely on the loopback!
            auto reader = stream->GetReader(/* consume */ true);
        }

        stream.reset();

        double bw = CalcMiBs(data.TotalBytes(), write_timer);

        sLOG << "send bandwidth" << ctx.my_rank() << "->" << peer_id
             << "inner_repeat" << inner_repeat
             << bw << "MiB/s"
             << "total_bytes" << data.TotalBytes()
             << "time" << write_timer;

        bandwidth_write_(ctx.my_rank(), peer_id).add(bw);
    }

    template <typename Type>
    void Receiver(api::Context& ctx, size_t peer_id, size_t inner_repeat) {

        auto stream = ctx.GetNewStream<Stream>(/* dia_id */ 0);

        // just to determine TotalBytes()
        auto data = Generator<Type>(bytes_, min_size_, max_size_);

        {
            // this opens and closes the writers. this must be done,
            // otherwise the reader will wait infinitely on the loopback!
            auto writers = stream->GetWriters();
        }

        StatsTimerStart read_timer;
        {
            auto reader = stream->GetReader(consume_);
            while (reader.HasNext()) {
                Type t = reader.template Next<Type>();
                die_unless(data.Next() == t);
            }
        }
        read_timer.Stop();

        stream.reset();

        double bw = CalcMiBs(data.TotalBytes(), read_timer);

        sLOG << "recv bandwidth" << ctx.my_rank() << "->" << peer_id
             << "inner_repeat" << inner_repeat
             << bw << "MiB/s"
             << "total_bytes" << data.TotalBytes()
             << "time" << read_timer;

        bandwidth_read_(ctx.my_rank(), peer_id).add(bw);
    }

private:
    //! reader type: consume or keep
    std::string reader_type_;

    //! whole experiment
    unsigned int outer_repeats_ = 1;

    //! inner repetitions
    unsigned int inner_repeats_ = 1;

    //! n x n matrix of measured bandwidth
    AggMatrix bandwidth_write_;

    //! n x n matrix of measured bandwidth
    AggMatrix bandwidth_read_;

    //! consuming reader
    bool consume_;
};

template <typename Stream>
template <typename Type>
void StreamOneFactorExperiment<Stream>::Test(api::Context& ctx) {

    bandwidth_write_ = AggMatrix(ctx.num_workers());
    bandwidth_read_ = AggMatrix(ctx.num_workers());

    for (size_t outer_repeat = 0;
         outer_repeat < outer_repeats_; ++outer_repeat) {

        StatsTimerStart timer;

        for (size_t inner_repeat = 0;
             inner_repeat < inner_repeats_; inner_repeat++) {
            // perform 1-factor bandwidth stream
            for (size_t round = 0;
                 round < common::CalcOneFactorSize(ctx.num_workers()); ++round) {

                size_t peer =
                    common::CalcOneFactorPeer(round, ctx.my_rank(), ctx.num_workers());

                sLOG0 << "round" << round
                      << "me" << ctx.my_rank() << "peer_id" << peer;

                if (ctx.my_rank() < peer) {
                    ctx.net.Barrier();
                    Sender<Type>(ctx, peer, inner_repeat);
                    ctx.net.Barrier();
                    Receiver<Type>(ctx, peer, inner_repeat);
                }
                else if (ctx.my_rank() > peer) {
                    ctx.net.Barrier();
                    Receiver<Type>(ctx, peer, inner_repeat);
                    ctx.net.Barrier();
                    Sender<Type>(ctx, peer, inner_repeat);
                }
                else {
                    // not participating in this round, but still have to
                    // allocate and close Streams.
                    ctx.net.Barrier();
                    auto stream1 = ctx.GetNewStream<Stream>(/* dia_id */ 0);
                    {
                        auto reader = stream1->GetReader(/* consume */ true);
                        auto writers = stream1->GetWriters();
                    }
                    stream1.reset();
                    ctx.net.Barrier();
                    auto stream2 = ctx.GetNewStream<Stream>(/* dia_id */ 0);
                    {
                        auto reader = stream2->GetReader(/* consume */ true);
                        auto writers = stream2->GetWriters();
                    }
                    stream2.reset();
                }
            }
        }
        timer.Stop();

        LOG1 << "RESULT"
             << " experiment=" << "stream_1factor"
             << " stream=" << typeid(Stream).name()
             << " workers=" << ctx.num_workers()
             << " hosts=" << ctx.num_hosts()
             << " datatype=" << type_as_string_
             << " size=" << bytes_
             << " block_size=" << data::default_block_size
             << " avg_element_size="
             << static_cast<double>(min_size_ + max_size_) / 2.0
             << " total_time=" << timer;
    }

    sLOG1 << "Worker" << ctx.my_rank() << "finished.";

    ctx.net.Barrier();

    if (ctx.my_rank() == 0)
        LOG1 << "All workers finished.";

    // reduce (add) matrix to root.
    bandwidth_write_ = ctx.net.AllReduce(bandwidth_write_);
    bandwidth_read_ = ctx.net.AllReduce(bandwidth_read_);

    // print matrix
    if (ctx.my_rank() == 0) {
        LOG1 << "bandwidth_write_";
        PrintMatrix(bandwidth_write_);
        LOG1 << "bandwidth_read_";
        PrintMatrix(bandwidth_read_);
    }
}

/******************************************************************************/

template <typename Stream>
class StreamAllToAllExperiment : public DataGeneratorExperiment
{
public:
    int Run(int argc, char* argv[]) {

        tlx::CmdlineParser clp;

        clp.set_description("thrill::data benchmark for disk I/O");
        clp.set_author("Tobias Sturm <mail@tobiassturm.de>");

        DataGeneratorExperiment::AddCmdline(clp);

        clp.add_unsigned(
            'n', "iterations", iterations_, "Iterations (default: 1)");

        clp.add_param_string("reader", reader_type_,
                             "reader type (consume, keep)");

        if (!clp.process(argc, argv)) return -1;

        api::Run(
            [=](api::Context& ctx) {
                // make a copy of this for local workers
                StreamAllToAllExperiment<Stream> local = *this;

                if (type_as_string_ == "size_t")
                    local.Test<size_t>(ctx);
                else if (type_as_string_ == "string")
                    local.Test<std::string>(ctx);
                else if (type_as_string_ == "pair")
                    local.Test<pair_type>(ctx);
                else if (type_as_string_ == "triple")
                    local.Test<triple_type>(ctx);
                else
                    abort();
            });

        return 0;
    }

    template <typename Type>
    void Test(api::Context& ctx) {

        if (reader_type_ != "consume" && reader_type_ != "keep")
            abort();
        bool consume = reader_type_ == "consume";

        tlx::ThreadPool pool(ctx.num_workers() + 1);

        for (unsigned i = 0; i < iterations_; i++) {

            StatsTimerStart total_timer;
            StatsTimerStopped read_timer;
            auto stream = ctx.GetNewStream<Stream>(/* dia_id */ 0);

            // start reader thread
            pool.enqueue(
                [&]() {
                    read_timer.Start();
                    auto reader = stream->GetReader(consume);
                    while (reader.HasNext())
                        reader.template Next<Type>();
                    read_timer.Stop();
                });

            // start writer threads: send to all workers
            auto writers = stream->GetWriters();
            std::chrono::microseconds::rep write_time = 0;
            for (size_t target = 0; target < ctx.num_workers(); target++) {
                pool.enqueue(
                    [&, target]() {
                        auto data = Generator<Type>(
                            bytes_ / ctx.num_workers(), min_size_, max_size_);

                        StatsTimerStart write_timer;
                        while (data.HasNext()) {
                            writers[target].Put(data.Next());
                        }
                        writers[target].Close();
                        write_timer.Stop();
                        // REVIEW(ts): this is a data race!
                        write_time = std::max(write_time, write_timer.Microseconds());
                    });
            }
            pool.loop_until_empty();

            total_timer.Stop();
            LOG1 << "RESULT"
                 << " experiment=" << "stream_all_to_all"
                 << " stream=" << typeid(Stream).name()
                 << " workers=" << ctx.num_workers()
                 << " hosts=" << ctx.num_hosts()
                 << " datatype=" << type_as_string_
                 << " size=" << bytes_
                 << " block_size=" << data::default_block_size
                 << " avg_element_size="
                 << static_cast<double>(min_size_ + max_size_) / 2.0
                 << " total_time=" << total_timer
                 << " write_time=" << write_time
                 << " read_time=" << read_timer
                 << " total_speed_MiBs=" << CalcMiBs(bytes_, total_timer)
                 << " write_speed_MiBs=" << CalcMiBs(bytes_, write_time)
                 << " read_speed_MiBs=" << CalcMiBs(bytes_, read_timer);
        }
    }

private:
    //! number of iterations to run
    unsigned iterations_ = 1;

    //! reader type: consume or keep
    std::string reader_type_;
};

/******************************************************************************/

template <typename Stream>
class StreamAllToAllCheckExperiment : public DataGeneratorExperiment
{
public:
    int Run(int argc, char* argv[]) {

        tlx::CmdlineParser clp;

        clp.set_description("thrill::data benchmark for disk I/O");
        clp.set_author("Timo Bingmann <tb@panthema.net>");

        DataGeneratorExperiment::AddCmdline(clp);

        clp.add_unsigned(
            'n', "iterations", iterations_, "Iterations (default: 1)");

        if (!clp.process(argc, argv)) return -1;

        api::Run(
            [=](api::Context& ctx) {
                // make a copy of this for local workers
                StreamAllToAllCheckExperiment<Stream> local = *this;

                if (type_as_string_ == "size_t")
                    local.Test<size_t>(ctx);
                else if (type_as_string_ == "string")
                    local.Test<std::string>(ctx);
                else if (type_as_string_ == "pair")
                    local.Test<pair_type>(ctx);
                else if (type_as_string_ == "triple")
                    local.Test<triple_type>(ctx);
                else
                    abort();
            });

        return 0;
    }

    template <typename Type>
    void Test(api::Context& ctx) {

        tlx::ThreadPool pool(2 * ctx.num_workers());

        for (unsigned i = 0; i < iterations_; i++) {

            StatsTimerStart total_timer;
            std::chrono::microseconds::rep read_time = 0;

            auto stream = ctx.GetNewStream<Stream>(/* dia_id */ 0);

            size_t my_rank = ctx.my_rank();

            // start reader threads: receive from all workers
            auto readers = stream->GetReaders();
            for (size_t source = 0; source < ctx.num_workers(); source++) {
                // if (!(my_rank == 0 && source == 1))
                //     continue;
                pool.enqueue(
                    [&, source]() {
                        auto data = Generator<Type>(
                            bytes_ / ctx.num_workers(), min_size_, max_size_,
                            /* seed */ 42 + 3 * source + 7 * my_rank);

                        StatsTimerStart read_timer;
                        size_t i = 0;
                        while (readers[source].HasNext()) {
                            auto x = readers[source].template Next<Type>();
                            auto y = data.Next();
                            if (x != y) {
                                sLOG1 << "mismatch"
                                      << i << x << y << *readers[source].byte_block();
                            }
                            ++i;
                        }
                        die_unless(!data.HasNext());
                        read_timer.Stop();
                        read_time = std::max(read_time, read_timer.Microseconds());
                    });
            }

            // start writer threads: send to all workers
            auto writers = stream->GetWriters();
            std::chrono::microseconds::rep write_time = 0;
            for (size_t target = 0; target < ctx.num_workers(); target++) {
                // if (!(my_rank == 1 && target == 0))
                //     continue;
                pool.enqueue(
                    [&, target]() {
                        auto data = Generator<Type>(
                            bytes_ / ctx.num_workers(), min_size_, max_size_,
                            /* seed */ 42 + 3 * my_rank + 7 * target);

                        StatsTimerStart write_timer;
                        while (data.HasNext()) {
                            writers[target].Put(data.Next());
                        }
                        writers[target].Close();
                        write_timer.Stop();
                        // REVIEW(ts): this is a data race!
                        write_time = std::max(write_time, write_timer.Microseconds());
                    });
            }
            pool.loop_until_empty();

            total_timer.Stop();
            LOG1 << "RESULT"
                 << " experiment=" << "stream_all_to_all_check"
                 << " stream=" << typeid(Stream).name()
                 << " workers=" << ctx.num_workers()
                 << " hosts=" << ctx.num_hosts()
                 << " datatype=" << type_as_string_
                 << " size=" << bytes_
                 << " block_size=" << data::default_block_size
                 << " avg_element_size="
                 << static_cast<double>(min_size_ + max_size_) / 2.0
                 << " total_time=" << total_timer
                 << " write_time=" << write_time
                 << " read_time=" << read_time
                 << " total_speed_MiBs=" << CalcMiBs(bytes_, total_timer)
                 << " write_speed_MiBs=" << CalcMiBs(bytes_, write_time)
                 << " read_speed_MiBs=" << CalcMiBs(bytes_, read_time);
        }
    }

private:
    //! number of iterations to run
    unsigned iterations_ = 1;
};

/******************************************************************************/

class ScatterExperiment : public DataGeneratorExperiment
{
public:
    int Run(int argc, char* argv[]) {

        tlx::CmdlineParser clp;

        clp.set_description("thrill::data benchmark for disk I/O");
        clp.set_author("Tobias Sturm <mail@tobiassturm.de>");

        DataGeneratorExperiment::AddCmdline(clp);

        clp.add_unsigned(
            'n', "iterations", iterations_, "Iterations (default: 1)");

        clp.add_param_string("reader", reader_type_,
                             "reader type (consume, keep)");

        if (!clp.process(argc, argv)) return -1;

        api::Run(
            [=](api::Context& ctx) {
                // make a copy of this for local workers
                ScatterExperiment local = *this;

                if (type_as_string_ == "size_t")
                    local.Test<size_t>(ctx);
                else if (type_as_string_ == "string")
                    local.Test<std::string>(ctx);
                else if (type_as_string_ == "pair")
                    local.Test<pair_type>(ctx);
                else if (type_as_string_ == "triple")
                    local.Test<triple_type>(ctx);
                else
                    abort();
            });

        return 0;
    }

    template <typename Type>
    void Test(api::Context& ctx) {

        if (reader_type_ != "consume" && reader_type_ != "keep")
            abort();
        bool consume = reader_type_ == "consume";

        for (unsigned i = 0; i < iterations_; i++) {

            StatsTimerStart total_timer;
            auto stream = ctx.GetNewStream<data::CatStream>(/* dia_id */ 0);
            data::File file(ctx.block_pool(), 0, /* dia_id */ 0);
            auto writer = file.GetWriter();
            if (ctx.my_rank() == 0) {
                Generator<Type> data = Generator<Type>(bytes_, min_size_, max_size_);
                while (data.HasNext())
                    writer.Put(data.Next());
            }
            else {
                Generator<Type> data = Generator<Type>(0, min_size_, max_size_);
                while (data.HasNext())
                    writer.Put(data.Next());
            }

            // start reader thread
            StatsTimerStopped read_timer;

            tlx::ThreadPool pool(2);
            pool.enqueue(
                [&]() {
                    read_timer.Start();
                    auto reader = stream->GetReader(consume);
                    while (reader.HasNext())
                        reader.template Next<Type>();
                    read_timer.Stop();
                });

            // start writer threads: send to all workers
            std::chrono::microseconds::rep write_time = 0;
            pool.enqueue(
                [&]() {
                    writer.Close();
                    std::vector<size_t> offsets;
                    offsets.push_back(0);
                    for (unsigned int w = 0; w < ctx.num_workers(); w++) {
                        if (ctx.my_rank() == 0) {
                            offsets.push_back(
                                file.num_items() / ctx.num_workers() * (w + 1));
                            std::cout << offsets.back() << std::endl;
                        }
                        else
                            offsets.push_back(0);
                    }

                    StatsTimerStart write_timer;
                    stream->Scatter<Type>(file, offsets);
                    write_timer.Stop();
                    write_time = std::max(write_time, write_timer.Microseconds());
                });

            pool.loop_until_empty();

            stream.reset();

            total_timer.Stop();
            LOG1 << "RESULT"
                 << " experiment=" << "stream_all_to_all"
                 << " stream=" << typeid(data::CatStream).name()
                 << " workers=" << ctx.num_workers()
                 << " hosts=" << ctx.num_hosts()
                 << " datatype=" << type_as_string_
                 << " size=" << bytes_
                 << " block_size=" << data::default_block_size
                 << " avg_element_size="
                 << static_cast<double>(min_size_ + max_size_) / 2.0
                 << " total_time=" << total_timer
                 << " write_time=" << write_time
                 << " read_time=" << read_timer
                 << " total_speed_MiBs=" << CalcMiBs(bytes_, total_timer)
                 << " write_speed_MiBs=" << CalcMiBs(bytes_, write_time)
                 << " read_speed_MiBs=" << CalcMiBs(bytes_, read_timer);
        }
    }

private:
    //! number of iterations to run
    unsigned iterations_ = 1;

    //! reader type: consume or keep
    std::string reader_type_;
};

/******************************************************************************/

void Usage(const char* argv0) {
    std::cout
        << "Usage: " << argv0 << " <benchmark>" << std::endl
        << std::endl
        << "    file                 - File and serialization speed" << std::endl
        << "    blockqueue           - BlockQueue test" << std::endl
        << "    cat_stream_1factor   - 1-factor bandwidth test using CatStream" << std::endl
        << "    mix_stream_1factor   - 1-factor bandwidth test using MixStream" << std::endl
        << "    cat_stream_all2all   - full bandwidth test using CatStream" << std::endl
        << "    mix_stream_all2all   - full bandwidth test using MixStream" << std::endl
        << "    stream_all2all_check - full bandwidth test using CatStream with verification" << std::endl
        << "    scatter              - CatStream scatter test" << std::endl
        << std::endl;
}

int main(int argc, char* argv[]) {

    common::NameThisThread("benchmark");

    if (argc <= 1) {
        Usage(argv[0]);
        return 0;
    }

    std::string benchmark = argv[1];

    if (benchmark == "file") {
        return FileExperiment().Run(argc - 1, argv + 1);
    }
    else if (benchmark == "blockqueue") {
        return BlockQueueExperiment().Run(argc - 1, argv + 1);
    }
    else if (benchmark == "cat_stream_1factor") {
        return StreamOneFactorExperiment<data::CatStream>().Run(
            argc - 1, argv + 1);
    }
    else if (benchmark == "mix_stream_1factor") {
        return StreamOneFactorExperiment<data::MixStream>().Run(
            argc - 1, argv + 1);
    }
    else if (benchmark == "cat_stream_all2all") {
        return StreamAllToAllExperiment<data::CatStream>().Run(
            argc - 1, argv + 1);
    }
    else if (benchmark == "mix_stream_all2all") {
        return StreamAllToAllExperiment<data::MixStream>().Run(
            argc - 1, argv + 1);
    }
    else if (benchmark == "stream_all2all_check") {
        return StreamAllToAllCheckExperiment<data::CatStream>().Run(
            argc - 1, argv + 1);
    }
    else if (benchmark == "scatter") {
        return ScatterExperiment().Run(argc - 1, argv + 1);
    }
    else {
        Usage(argv[0]);
        return -1;
    }
}

/******************************************************************************/
