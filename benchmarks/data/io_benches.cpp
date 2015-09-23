/*******************************************************************************
 * benchmarks/data/io_benches.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/api/context.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stat_logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/common/thread_pool.hpp>
#include <thrill/data/block_queue.hpp>

#include <iostream>
#include <random>
#include <string>
#include <tuple>

#include "data_generators.hpp"

using namespace thrill; // NOLINT
using common::StatsTimer;

using pair_type = std::tuple<std::string, size_t>;
using triple_type = std::tuple<std::string, size_t, std::string>;

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
                           "reader type (consume, non-consume)");

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

        if (reader_type_ != "consume" && reader_type_ != "non-consume")
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
                 << " avg_element_size=" << (min_size_ + max_size_) / 2.0
                 << " reader=" << reader_type_
                 << " write_time=" << write_timer.Microseconds()
                 << " read_time=" << read_timer.Microseconds();
        }
    }

protected:
    //! number of iterations to run
    unsigned iterations_ = 1;

    //! reader type: consume or keep
    std::string reader_type_;
};

/******************************************************************************/

class MixChannelAllToAllExperiment : public DataGeneratorExperiment
{
public:
    int Run(int argc, char* argv[]) {

        common::CmdlineParser clp;

        clp.SetDescription("thrill::data benchmark for disk I/O");
        clp.SetAuthor("Tobias Sturm <mail@tobiassturm.de>");

        DataGeneratorExperiment::AddCmdline(clp);

        clp.AddUInt('n', "iterations", iterations_, "Iterations (default: 1)");

        clp.AddParamString("reader", reader_type_,
                           "reader type (consume, non-consume)");

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

        if (reader_type_ != "consume" && reader_type_ != "non-consume")
            abort();
        bool consume = reader_type_ == "consume";

        for (unsigned i = 0; i < iterations_; i++) {

            StatsTimer<true> total_timer(true);
            StatsTimer<true> read_timer;
            auto stream = ctx.GetNewMixStream();

            // start reader thread
            common::ThreadPool threads(ctx.num_workers() + 1);
            threads.Enqueue(
                [&]() {
                    read_timer.Start();
                    auto reader = stream->OpenMixReader(consume);
                    while (reader.HasNext())
                        reader.Next<Type>();
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
                 << " experiment=" << "channel_all_to_all"
                 << " workers=" << ctx.num_workers()
                 << " hosts=" << ctx.num_hosts()
                 << " datatype=" << type_as_string_
                 << " size=" << bytes_
                 << " block_size=" << block_size_
                 << " avg_element_size=" << (min_size_ + max_size_) / 2.0
                 << " total_time=" << total_timer.Microseconds()
                 << " write_time=" << write_time
                 << " read_time=" << read_timer.Microseconds();
        }
    }

protected:
    //! number of iterations to run
    unsigned iterations_ = 1;

    //! reader type: consume or keep
    std::string reader_type_;
};

/******************************************************************************/

#if SORRY_HOW_IS_THIS_DIFFERENT_FROM_ABOVE_TB

template <typename Type>
void ChannelP(uint64_t bytes, size_t min_size, size_t max_size, unsigned iterations, api::Context& ctx, const std::string& type_as_string, size_t block_size) {

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
             << " experiment=" << "channel_1p"
             << " workers=" << ctx.num_workers()
             << " hosts=" << ctx.num_hosts()
             << " datatype=" << type_as_string
             << " size=" << bytes
             << " block_size=" << block_size
             << " avg_element_size=" << (min_size + max_size) / 2.0
             << " write_time=" << write_timer.Microseconds()
             << " read_time=" << read_timer.Microseconds();
    }
}

#endif

/******************************************************************************/

#if SORRY_I_DONT_UNDERSTAND_WHAT_THIS_TESTS_TB

template <typename Type>
void ChannelAToBExperiment(api::Context& ctx) {

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
             << " experiment=" << "channel_a_to_b"
             << " workers=" << ctx.num_workers()
             << " hosts=" << ctx.num_hosts()
             << " datatype=" << type_as_string
             << " size=" << bytes
             << " block_size=" << block_size
             << " avg_element_size=" << (min_size + max_size) / 2.0
             << " write_time=" << write_timer.Microseconds()
             << " read_time=" << read_timer.Microseconds();
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
                           "reader type (consume, non-consume)");

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

        if (reader_type_ != "consume" && reader_type_ != "non-consume")
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
                 << " avg_element_size=" << (min_size_ + max_size_) / 2.0
                 << " reader=" << reader_type_
                 << " write_time=" << write_timer.Microseconds()
                 << " read_time=" << read_time
                 << " threads=" << num_threads_;
        }
    }

protected:
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
        << "    mix_channel_all2all - Full bandwidth test" << std::endl
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
    else if (benchmark == "mix_channel_all2all") {
        return MixChannelAllToAllExperiment().Run(argc - 1, argv + 1);
    }
    else if (benchmark == "blockqueue") {
        return BlockQueueExperiment().Run(argc - 1, argv + 1);
    }
    else {
        Usage(argv[0]);
        return -1;
    }
}

/******************************************************************************/
