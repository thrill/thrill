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
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/matrix.hpp>
#include <thrill/common/stat_logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/common/thread_pool.hpp>
#include <thrill/data/block_queue.hpp>

#include <algorithm>
#include <iostream>
#include <random>
#include <string>
#include <tuple>

#include "data_generators.hpp"

using namespace thrill; // NOLINT
using common::StatsTimer;

using pair_type = std::tuple<std::string, size_t>;
using triple_type = std::tuple<std::string, size_t, std::string>;

//! calculate MiB/s given byte size and microsecond time.
double CalcMiBs(size_t bytes, const std::chrono::microseconds::rep& microsec) {
    return static_cast<double>(bytes) / 1024.0 / 1024.0
           / static_cast<double>(microsec) * 1e6;
}

//! calculate MiB/s given byte size and timer.
double CalcMiBs(size_t bytes, const StatsTimer<true>& timer) {
    return CalcMiBs(bytes, timer.Microseconds());
}

// matrix of measured latencies or bandwidths
using AggDouble = common::Aggregate<double>;
using AggMatrix = common::Matrix<AggDouble>;

//! print avg/stddev matrix
void PrintMatrix(const AggMatrix& m) {
    for (size_t i = 0; i < m.rows(); ++i) {
        std::ostringstream os;
        for (size_t j = 0; j < m.columns(); ++j) {
            os << common::str_sprintf(
                "%8.1f/%8.3f", m(i, j).Avg(), m(i, j).StdDev());
        }
        LOG1 << os.str();
    }
}

/******************************************************************************/
//! Base Class for Experiments with Generator<> instances

class DataGeneratorExperiment
{
public:
    void AddCmdline(common::CmdlineParser& clp) {

        clp.AddBytes('b', "bytes", bytes_,
                     "number of bytes to process (default 1024)");

        clp.AddBytes('s', "block_size", block_size_,
                     "block size (system default)");

        clp.AddBytes('l', "lower", min_size_,
                     "lower bound for variable element length (default 1)");

        clp.AddBytes('u', "upper", max_size_,
                     "upper bound for variable element length (default 100)");

        clp.AddParamString("type", type_as_string_,
                           "data type (size_t, string, pair, triple)");
    }

protected:
    //! total bytes to process (default: 1024)
    uint64_t bytes_ = 1024;

    //! block size used
    uint64_t block_size_ = data::default_block_size;

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

        common::CmdlineParser clp;

        clp.SetDescription("thrill::data benchmark for disk I/O");
        clp.SetAuthor("Tobias Sturm <mail@tobiassturm.de>");

        DataGeneratorExperiment::AddCmdline(clp);

        clp.AddUInt('n', "iterations", iterations_, "Iterations (default: 1)");

        clp.AddParamString("reader", reader_type_,
                           "reader type (consume, keep)");

        if (!clp.Process(argc, argv)) return -1;

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
            auto file = ctx.GetFile();
            auto writer = file.GetWriter(block_size_);
            auto data = Generator<Type>(bytes_, min_size_, max_size_);

            StatsTimer<true> write_timer(true);
            while (data.HasNext()) {
                writer(data.Next());
            }
            writer.Close();
            write_timer.Stop();

            StatsTimer<true> read_timer(true);
            auto reader = file.GetReader(consume);
            while (reader.HasNext())
                reader.Next<Type>();
            read_timer.Stop();

            LOG1 << "RESULT"
                 << " experiment=" << "file"
                 << " datatype=" << type_as_string_
                 << " size=" << bytes_
                 << " block_size=" << block_size_
                 << " avg_element_size="
                 << static_cast<double>(min_size_ + max_size_) / 2.0
                 << " reader=" << reader_type_
                 << " write_time=" << write_timer.Microseconds()
                 << " read_time=" << read_timer.Microseconds()
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

template <typename Stream>
class StreamOneFactorExperiment : public DataGeneratorExperiment
{
    static const bool debug = true;

public:
    int Run(int argc, char* argv[]) {

        common::CmdlineParser clp;

        DataGeneratorExperiment::AddCmdline(clp);

        clp.AddUInt('r', "inner_repeats", inner_repeats_,
                    "Repeat inner experiment a number of times.");

        clp.AddUInt('R', "outer_repeats", outer_repeats_,
                    "Repeat whole experiment a number of times.");

        clp.AddParamString("reader", reader_type_,
                           "reader type (consume, keep)");

        if (!clp.Process(argc, argv)) return -1;

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

        auto stream = ctx.GetNewStream<Stream>();
        auto data = Generator<Type>(bytes_, min_size_, max_size_);

        StatsTimer<true> write_timer(true);
        {
            auto writers = stream->OpenWriters(block_size_);
            while (data.HasNext())
                writers[peer_id].PutItem(data.Next());
        }
        write_timer.Stop();

        stream->Close();

        double bw = CalcMiBs(data.TotalBytes(), write_timer);

        sLOG << "send bandwidth" << ctx.my_rank() << "->" << peer_id
             << "inner_repeat" << inner_repeat
             << bw << "MiB/s"
             << "total_bytes" << data.TotalBytes()
             << "time"
             << (static_cast<double>(write_timer.Microseconds()) * 1e-6);

        bandwidth_write_(ctx.my_rank(), peer_id).Add(bw);
    }

    template <typename Type>
    void Receiver(api::Context& ctx, size_t peer_id, size_t inner_repeat) {

        auto stream = ctx.GetNewStream<Stream>();

        // just to determine TotalBytes()
        auto data = Generator<Type>(bytes_, min_size_, max_size_);

        {
            // this opens and closes the writers. this must be done,
            // otherwise the reader will wait infinitely on the loopback!
            auto writers = stream->OpenWriters(block_size_);
        }

        StatsTimer<true> read_timer(true);
        {
            auto reader = stream->OpenAnyReader(consume_);
            while (reader.HasNext())
                reader.template Next<Type>();
        }
        read_timer.Stop();

        stream->Close();

        double bw = CalcMiBs(data.TotalBytes(), read_timer);

        sLOG << "recv bandwidth" << ctx.my_rank() << "->" << peer_id
             << "inner_repeat" << inner_repeat
             << bw << "MiB/s"
             << "total_bytes" << data.TotalBytes()
             << "time"
             << (static_cast<double>(read_timer.Microseconds()) * 1e-6);

        bandwidth_read_(ctx.my_rank(), peer_id).Add(bw);
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

        common::StatsTimer<true> timer;

        timer.Start();
        for (size_t inner_repeat = 0;
             inner_repeat < inner_repeats_; inner_repeat++) {
            // perform 1-factor ping pongs (without barriers)
            for (size_t round = 0;
                 round < common::CalcOneFactorSize(ctx.num_workers()); ++round) {

                size_t peer =
                    common::CalcOneFactorPeer(round, ctx.my_rank(), ctx.num_workers());

                sLOG << "round" << round
                     << "me" << ctx.my_rank() << "peer_id" << peer;

                if (ctx.my_rank() < peer) {
                    ctx.Barrier();
                    Sender<Type>(ctx, peer, inner_repeat);
                    ctx.Barrier();
                    Receiver<Type>(ctx, peer, inner_repeat);
                }
                else if (ctx.my_rank() > peer) {
                    ctx.Barrier();
                    Receiver<Type>(ctx, peer, inner_repeat);
                    ctx.Barrier();
                    Sender<Type>(ctx, peer, inner_repeat);
                }
                else {
                    // not participating in this round, but still have to
                    // allocate and close Streams.
                    ctx.Barrier();
                    auto stream1 = ctx.GetNewStream<Stream>();
                    stream1->Close();
                    ctx.Barrier();
                    auto stream2 = ctx.GetNewStream<Stream>();
                    stream2->Close();
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
             << " block_size=" << block_size_
             << " avg_element_size="
             << static_cast<double>(min_size_ + max_size_) / 2.0
             << " total_time=" << timer.Microseconds();
    }

    ctx.Barrier();

    // reduce (add) matrix to root.
    bandwidth_write_ = ctx.flow_control_channel().AllReduce(bandwidth_write_);
    bandwidth_read_ = ctx.flow_control_channel().AllReduce(bandwidth_read_);

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

        common::CmdlineParser clp;

        clp.SetDescription("thrill::data benchmark for disk I/O");
        clp.SetAuthor("Tobias Sturm <mail@tobiassturm.de>");

        DataGeneratorExperiment::AddCmdline(clp);

        clp.AddUInt('n', "iterations", iterations_, "Iterations (default: 1)");

        clp.AddParamString("reader", reader_type_,
                           "reader type (consume, keep)");

        if (!clp.Process(argc, argv)) return -1;

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

        for (unsigned i = 0; i < iterations_; i++) {

            StatsTimer<true> total_timer(true);
            StatsTimer<true> read_timer;
            auto stream = ctx.GetNewStream<Stream>();

            // start reader thread
            common::ThreadPool threads(ctx.num_workers() + 1);
            threads.Enqueue(
                [&]() {
                    read_timer.Start();
                    auto reader = stream->OpenAnyReader(consume);
                    while (reader.HasNext())
                        reader.template Next<Type>();
                    read_timer.Stop();
                });

            // start writer threads: send to all workers
            auto writers = stream->OpenWriters(block_size_);
            std::chrono::microseconds::rep write_time = 0;
            for (size_t target = 0; target < ctx.num_workers(); target++) {
                threads.Enqueue(
                    [&, target]() {
                        auto data = Generator<Type>(
                            bytes_ / ctx.num_workers(), min_size_, max_size_);

                        StatsTimer<true> write_timer(true);
                        while (data.HasNext()) {
                            writers[target](data.Next());
                        }
                        writers[target].Close();
                        write_timer.Stop();
                        // REVIEW(ts): this is a data race!
                        write_time = std::max(write_time, write_timer.Microseconds());
                    });
            }
            threads.LoopUntilEmpty();

            total_timer.Stop();
            LOG1 << "RESULT"
                 << " experiment=" << "stream_all_to_all"
                 << " stream=" << typeid(Stream).name()
                 << " workers=" << ctx.num_workers()
                 << " hosts=" << ctx.num_hosts()
                 << " datatype=" << type_as_string_
                 << " size=" << bytes_
                 << " block_size=" << block_size_
                 << " avg_element_size="
                 << static_cast<double>(min_size_ + max_size_) / 2.0
                 << " total_time=" << total_timer.Microseconds()
                 << " write_time=" << write_time
                 << " read_time=" << read_timer.Microseconds()
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

class ScatterExperiment : public DataGeneratorExperiment
{
public:
    int Run(int argc, char* argv[]) {

        common::CmdlineParser clp;

        clp.SetDescription("thrill::data benchmark for disk I/O");
        clp.SetAuthor("Tobias Sturm <mail@tobiassturm.de>");

        DataGeneratorExperiment::AddCmdline(clp);

        clp.AddUInt('n', "iterations", iterations_, "Iterations (default: 1)");

        clp.AddParamString("reader", reader_type_,
                           "reader type (consume, keep)");

        if (!clp.Process(argc, argv)) return -1;

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

            StatsTimer<true> total_timer(true);
            StatsTimer<true> read_timer;
            auto stream = ctx.GetNewStream<data::MixStream>();

            // start reader thread
            common::ThreadPool threads(2);
            threads.Enqueue(
                [&]() {
                    read_timer.Start();
                    auto reader = stream->OpenAnyReader(consume);
                    while (reader.HasNext())
                        reader.template Next<Type>();
                    read_timer.Stop();
                });

            // start writer threads: send to all workers
            std::chrono::microseconds::rep write_time = 0;
            threads.Enqueue(
                [&]() {
                    data::File file(ctx.block_pool());
                    auto writer = file.GetWriter();
                    if (ctx.my_rank() == 0) {
                        Generator<Type> data = Generator<Type>(bytes_, min_size_, max_size_);
                        while(data.HasNext())
                            writer(data.Next());
                    } else {
                        Generator<Type> data = Generator<Type>(0, min_size_, max_size_);
                        while(data.HasNext())
                            writer(data.Next());
                    }
                    std::vector<size_t> offsets;
                    for (unsigned int w = 0; w < ctx.num_workers(); w++) {
                        if (ctx.my_rank() == 0)
                            offsets.push_back(file.num_items() / ctx.num_workers() * (w + 1) - 1);
                        else
                            offsets.push_back(0);
                    }

                    StatsTimer<true> write_timer(true);
                    auto stream = ctx.GetNewMixStream();
                    stream->Scatter<Type>(file, offsets);
                    write_timer.Stop();
                    write_time = std::max(write_time, write_timer.Microseconds());
                });
            threads.LoopUntilEmpty();

            total_timer.Stop();
            LOG1 << "RESULT"
                 << " experiment=" << "stream_all_to_all"
                 << " stream=" << typeid(data::MixStream).name()
                 << " workers=" << ctx.num_workers()
                 << " hosts=" << ctx.num_hosts()
                 << " datatype=" << type_as_string_
                 << " size=" << bytes_
                 << " block_size=" << block_size_
                 << " avg_element_size="
                 << static_cast<double>(min_size_ + max_size_) / 2.0
                 << " total_time=" << total_timer.Microseconds()
                 << " write_time=" << write_time
                 << " read_time=" << read_timer.Microseconds()
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

#if SORRY_HOW_IS_THIS_DIFFERENT_FROM_ABOVE_TB

template <typename Type>
void StreamP(uint64_t bytes, size_t min_size, size_t max_size, unsigned iterations, api::Context& ctx, const std::string& type_as_string, size_t block_size) {

    for (unsigned i = 0; i < iterations; i++) {

        StatsTimer<true> write_timer, read_timer;
        for (size_t round = 0; round < ctx.num_workers(); round++) {
            size_t send_to = (ctx.my_rank() + round + 1) % ctx.num_workers();

            auto stream = ctx.GetNewMixStream();
            auto data = Generator<Type>(bytes / ctx.num_workers(), min_size, max_size);
            auto writers = stream->OpenWriters(block_size);
            write_timer.Start();
            while (data.HasNext()) {
                writers[send_to](data.Next());
            }
            for (auto& w : writers)
                w.Close();
            write_timer.Stop();

            read_timer.Start();
            auto reader = stream->OpenMixReader(true /*consume*/);
            while (reader.HasNext())
                reader.Next<Type>();
            read_timer.Stop();
        }
        LOG1 << "RESULT"
             << " experiment=" << "stream_1p"
             << " workers=" << ctx.num_workers()
             << " hosts=" << ctx.num_hosts()
             << " datatype=" << type_as_string
             << " size=" << bytes
             << " block_size=" << block_size
             << " avg_element_size=" << (min_size + max_size) / 2.0
             << " write_time=" << write_timer.Microseconds()
             << " read_time=" << read_timer.Microseconds()
             << " write_speed_MiBs=" << CalcMiBs(bytes_, write_timer)
             << " read_speed_MiBs=" << CalcMiBs(bytes_, read_timer);
    }
}

#endif

/******************************************************************************/

#if SORRY_I_DONT_UNDERSTAND_WHAT_THIS_TESTS_TB

template <typename Type>
void StreamAToBExperiment(api::Context& ctx) {

    for (unsigned i = 0; i < iterations; i++) {
        auto stream = ctx.GetNewCatStream();
        auto writers = stream->OpenWriters(block_size);
        auto data = Generator<Type>(bytes, min_size, max_size);

        StatsTimer<true> write_timer;
        if (ctx.my_rank() != 0) {
            write_timer.Start();
            while (data.HasNext()) {
                writers[0](data.Next());
            }
            for (auto& w : writers)
                w.Close();
            write_timer.Stop();
        }
        else
            for (auto& w : writers)
                w.Close();

        StatsTimer<true> read_timer;
        if (ctx.my_rank() == 0) {
            read_timer.Start();
            auto reader = stream->OpenCatReader(true /*consume*/);
            while (reader.HasNext())
                reader.Next<Type>();
            read_timer.Stop();
        }
        LOG1 << "RESULT"
             << " experiment=" << "stream_a_to_b"
             << " workers=" << ctx.num_workers()
             << " hosts=" << ctx.num_hosts()
             << " datatype=" << type_as_string
             << " size=" << bytes
             << " block_size=" << block_size
             << " avg_element_size=" << (min_size + max_size) / 2.0
             << " write_time=" << write_timer.Microseconds()
             << " read_time=" << read_timer.Microseconds()
             << " write_speed_MiBs=" << CalcMiBs(bytes_, write_timer)
             << " read_speed_MiBs=" << CalcMiBs(bytes_, read_timer);
    }
}

#endif

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

        common::CmdlineParser clp;

        clp.SetDescription("thrill::data benchmark for disk I/O");
        clp.SetAuthor("Tobias Sturm <mail@tobiassturm.de>");

        DataGeneratorExperiment::AddCmdline(clp);

        clp.AddUInt('n', "iterations", iterations_, "Iterations (default: 1)");

        clp.AddUInt('t', "threads", num_threads_, "Number of threads (default: 1)");

        clp.AddParamString("reader", reader_type_,
                           "reader type (consume, keep)");

        if (!clp.Process(argc, argv)) return -1;

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

        common::ThreadPool threads(num_threads_ + 1);
        for (unsigned i = 0; i < iterations_; i++) {
            auto queue = data::BlockQueue(ctx.block_pool());
            auto data = Generator<Type>(bytes_, min_size_, max_size_);

            StatsTimer<true> write_timer;
            threads.Enqueue(
                [&]() {
                    auto writer = queue.GetWriter(block_size_);
                    write_timer.Start();
                    while (data.HasNext()) {
                        writer(data.Next());
                    }
                    writer.Close();
                    write_timer.Stop();
                });

            std::chrono::microseconds::rep read_time = 0;

            for (size_t thread = 0; thread < num_threads_; thread++) {
                threads.Enqueue(
                    [&]() {
                        StatsTimer<true> read_timer(true);
                        auto reader = queue.GetReader(consume);
                        while (reader.HasNext())
                            reader.Next<Type>();
                        read_timer.Stop();
                        // REVIEW(ts): this is a data race!
                        read_time = std::max(read_time, read_timer.Microseconds());
                    });
            }
            threads.LoopUntilEmpty();
            LOG1 << "RESULT"
                 << " experiment=" << "block_queue"
                 << " workers=" << ctx.num_workers()
                 << " hosts=" << ctx.num_hosts()
                 << " datatype=" << type_as_string_
                 << " size=" << bytes_
                 << " block_size=" << block_size_
                 << " avg_element_size="
                 << static_cast<double>(min_size_ + max_size_) / 2.0
                 << " reader=" << reader_type_
                 << " write_time=" << write_timer.Microseconds()
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

void Usage(const char* argv0) {
    std::cout
        << "Usage: " << argv0 << " <benchmark>" << std::endl
        << std::endl
        << "    file                - File and Serialization Speed" << std::endl
        << "    cat_stream_1factor  - 1 factor bandwidth test using CatStream" << std::endl
        << "    mix_stream_1factor  - 1 factor bandwidth test using MixStream" << std::endl
        << "    cat_stream_all2all  - Full bandwidth test using CatStream" << std::endl
        << "    mix_stream_all2all  - Full bandwidth test using MixStream" << std::endl
        << "    blockqueue          - BlockQueue test" << std::endl
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
    else if (benchmark == "blockqueue") {
        return BlockQueueExperiment().Run(argc - 1, argv + 1);
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
