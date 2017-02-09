/*******************************************************************************
 * thrill/core/location_detection.hpp
 *
 * Detection of element locations using a distributed bloom filter
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_LOCATION_DETECTION_HEADER
#define THRILL_CORE_LOCATION_DETECTION_HEADER

#include <thrill/common/function_traits.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/core/delta_stream.hpp>
#include <thrill/core/golomb_bit_stream.hpp>
#include <thrill/core/multiway_merge.hpp>
#include <thrill/core/reduce_bucket_hash_table.hpp>
#include <thrill/core/reduce_functional.hpp>
#include <thrill/core/reduce_old_probing_hash_table.hpp>
#include <thrill/core/reduce_probing_hash_table.hpp>
#include <thrill/core/reduce_table.hpp>
#include <thrill/data/cat_stream.hpp>

#include <x86intrin.h>

#include <algorithm>
#include <functional>
#include <unordered_map>
#include <utility>
#include <vector>

namespace thrill {

struct hash {

    inline size_t operator () (const size_t& n) const {
        size_t hash = _mm_crc32_u32((size_t)28475421, n);
        hash = hash << 32;
        hash += _mm_crc32_u32((size_t)52150599, n);
        return hash;
    }
};

namespace core {

/*!
 * Emitter for a ReduceTable, which emits all of its data into a vector of
 * hash-counter-pairs.
 */
template <typename HashCount>
class ToVectorEmitter
{
public:
    explicit ToVectorEmitter(std::vector<HashCount>& vec)
        : vec_(vec) { }

    void SetModulo(size_t modulo) {
        modulo_ = modulo;
    }

    void Emit(const size_t& /* partition_id */, const HashCount& p) {
        assert(modulo_ > 1);
        vec_.emplace_back(p);
        vec_.back().hash %= modulo_;
    }

private:
    std::vector<HashCount>& vec_;
    size_t modulo_ = 1;
};

template <typename HashCount>
class LocationDetection
{
private:
    static constexpr bool debug = false;

    using GolombBitStreamWriter =
              core::GolombBitStreamWriter<data::CatStream::Writer>;

    using GolombBitStreamReader =
              core::GolombBitStreamReader<data::CatStream::Reader>;

    using GolumbDeltaWriter =
              core::DeltaStreamWriter<GolombBitStreamWriter, size_t, /* offset */ 1>;

    using GolumbDeltaReader =
              core::DeltaStreamReader<GolombBitStreamReader, size_t, /* offset */ 1>;

    using CounterType = typename HashCount::CounterType;

    class GolombPairReader
    {
    public:
        GolombPairReader(GolombBitStreamReader& bit_reader,
                         GolumbDeltaReader& delta_reader)
            : bit_reader_(bit_reader), delta_reader_(delta_reader) { }

        bool HasNext() {
            return bit_reader_.HasNext();
        }

        template <typename Type>
        HashCount Next() {
            HashCount hc;
            hc.hash = delta_reader_.Next<size_t>();
            hc.ReadBits(bit_reader_);
            return hc;
        }

    private:
        GolombBitStreamReader& bit_reader_;
        GolumbDeltaReader& delta_reader_;
    };

    struct ExtractHash {
        size_t operator () (const HashCount& hc) const { return hc.hash; }
    };

private:
    void WriteOccurenceCounts(const data::CatStreamPtr& stream_pointer,
                              const std::vector<HashCount>& hash_occ,
                              size_t golomb_param,
                              size_t num_workers,
                              size_t max_hash) {

        std::vector<data::CatStream::Writer> writers =
            stream_pointer->GetWriters();

        for (size_t i = 0, j = 0; i < num_workers; ++i) {
            common::Range range_i =
                common::CalculateLocalRange(max_hash, num_workers, i);

            GolombBitStreamWriter golomb_writer(writers[i], golomb_param);
            GolumbDeltaWriter delta_writer(
                golomb_writer,
                /* initial */ size_t(-1) /* cancels with +1 bias */);

            while (j < hash_occ.size() && hash_occ[j].hash < range_i.end)
            {
                // accumulate counters hashing to same value
                HashCount total_hash = hash_occ[j++];
                while (j < hash_occ.size() &&
                       hash_occ[j].hash == total_hash.hash)
                {
                    total_hash += hash_occ[j++];
                }

                // write counter of all values hashing to hash_occ[j].hash
                // in next bitsize bits
                delta_writer.Put(total_hash.hash);
                total_hash.WriteBits(golomb_writer);
            }
        }
    }

public:
    using ReduceConfig = DefaultReduceConfig;
    using Emitter = ToVectorEmitter<HashCount>;

    using Table = typename ReduceTableSelect<
              ReduceConfig::table_impl_,
              HashCount, typename HashCount::HashType, HashCount,
              ExtractHash, std::plus<HashCount>, Emitter,
              /* VolatileKey */ false, ReduceConfig>::type;

    LocationDetection(Context& ctx, size_t dia_id,
                      const ReduceConfig& config = ReduceConfig())
        : emit_(hash_occ_),
          context_(ctx),
          dia_id_(dia_id),
          config_(config),
          table_(ctx, dia_id, ExtractHash(), std::plus<HashCount>(),
                 emit_, 1, config) {
        sLOG << "creating LocationDetection";
    }

    /*!
     * Initializes the table to the memory limit size.
     *
     * \param limit_memory_bytes Memory limit in bytes
     */
    void Initialize(size_t limit_memory_bytes) {
        table_.Initialize(limit_memory_bytes);
    }

    /*!
     * Inserts a pair of key and 1 to the table.
     *
     * \param key Key to insert.
     */
    void Insert(const HashCount& item) {
        table_.Insert(item);
    }

    /*!
     * Flushes the table and detects the most common location for each element.
     */
    size_t Flush(std::unordered_map<size_t, size_t>& target_processors) {

        size_t num_items = table_.num_items();
        if (table_.has_spilled_data_on_partition(0)) {
            num_items += table_.partition_files()[0].num_items();
        }

        // golomb code parameters
        size_t upper_bound_uniques = context_.net.AllReduce(num_items);
        double fpr_parameter = 8;
        size_t golomb_param = (size_t)fpr_parameter;
        size_t max_hash = golomb_param * upper_bound_uniques;

        emit_.SetModulo(max_hash);
        hash_occ_.reserve(num_items);
        table_.FlushAll();

        if (table_.has_spilled_data_on_partition(0)) {
            data::File::Reader reader =
                table_.partition_files()[0].GetReader(true);

            while (reader.HasNext()) {
                emit_.Emit(0, reader.Next<HashCount>());
            }
        }

        std::sort(hash_occ_.begin(), hash_occ_.end());

        data::CatStreamPtr golomb_data_stream =
            context_.GetNewCatStream(dia_id_);

        WriteOccurenceCounts(golomb_data_stream,
                             hash_occ_, golomb_param,
                             context_.num_workers(),
                             max_hash);

        std::vector<HashCount>().swap(hash_occ_);

        // get inbound Golomb/delta-encoded hash stream

        std::vector<data::CatStream::Reader> hash_readers =
            golomb_data_stream->GetReaders();

        std::vector<GolombBitStreamReader> golomb_readers;
        std::vector<GolumbDeltaReader> delta_readers;
        std::vector<GolombPairReader> pair_readers;

        golomb_readers.reserve(context_.num_workers());
        delta_readers.reserve(context_.num_workers());
        pair_readers.reserve(context_.num_workers());

        for (auto& reader : hash_readers) {
            golomb_readers.emplace_back(reader, golomb_param);
            delta_readers.emplace_back(
                golomb_readers.back(),
                /* initial */ size_t(-1) /* cancels with +1 bias */);
            pair_readers.emplace_back(
                golomb_readers.back(), delta_readers.back());
        }

        size_t worker_bitsize = std::max(
            common::IntegerLog2Ceil(context_.num_workers()), (unsigned int)1);

        // multi-way merge hash streams and detect hosts with most items per key

        auto puller = make_multiway_merge_tree<HashCount>(
            pair_readers.begin(), pair_readers.end());

        // create streams (delta/Golomb encoded) to notify workers of location

        data::CatStreamPtr location_stream = context_.GetNewCatStream(dia_id_);

        std::vector<data::CatStream::Writer> location_writers =
            location_stream->GetWriters();

        std::vector<GolombBitStreamWriter> location_gbsw;
        std::vector<GolumbDeltaWriter> location_dw;
        location_gbsw.reserve(context_.num_workers());
        location_dw.reserve(context_.num_workers());

        for (size_t i = 0; i < context_.num_workers(); ++i) {
            location_gbsw.emplace_back(location_writers[i], golomb_param);
            location_dw.emplace_back(
                location_gbsw.back(),
                /* initial */ size_t(-1) /* cancels with +1 bias */);
        }

        std::pair<HashCount, size_t> next;
        std::vector<size_t> workers;

        bool not_finished = puller.HasNext();

        if (not_finished)
            next = puller.NextWithSource();

        while (not_finished) {
            // set up aggregation values from first item with equal hash
            HashCount sum = next.first;
            workers.push_back(next.second);

            size_t max_worker = next.second;
            CounterType max_count = sum.count;

            // check if another item is available, and if it has the same hash
            while ((not_finished = puller.HasNext()) &&
                   (next = puller.NextWithSource(),
                    next.first.hash == sum.hash))
            {
                // summarize items (this sums dia_masks and counts)
                sum += next.first;

                // check if count is higher
                if (next.first.count > max_count) {
                    max_count = next.first.count;
                    max_worker = next.second;
                }
                // store all workers to notify
                workers.push_back(next.second);
            }

            // for dia_mask == 3 -> notify all participating workers (this is
            // for InnerJoin, since only they need the items)
            if (sum.NeedBroadcast()) {
                for (const size_t& w : workers) {
                    location_dw[w].Put(sum.hash);
                    location_gbsw[w].PutBits(max_worker, worker_bitsize);
                    LOG << "Put: " << sum.hash << " @ " << max_worker
                        << " -> " << w;
                }
            }

            workers.clear();
        }

        // close target-worker writers
        location_dw.clear();
        location_gbsw.clear();
        location_writers.clear();

        // read location notifications and store them in the unordered_map

        std::vector<data::CatStream::Reader> location_readers =
            location_stream->GetReaders();

        target_processors.reserve(num_items);

        for (data::CatStream::Reader& reader : location_readers)
        {
            GolombBitStreamReader golomb_reader(reader, golomb_param);
            GolumbDeltaReader delta_reader(
                golomb_reader, /* initial */ size_t(-1) /* cancels at +1 */);

            // Builds golomb encoded bitset from data received by the stream.
            while (delta_reader.HasNext()) {
                // Golomb code contains deltas, we want the actual values
                size_t hash = delta_reader.Next<size_t>();
                size_t worker = golomb_reader.GetBits(worker_bitsize);

                LOG << "Hash " << hash << " on worker " << worker;
                target_processors[hash] = worker;
            }
        }

        return max_hash;
    }

    //! Target vector for vector emitter
    std::vector<HashCount> hash_occ_;
    //! Emitter to vector
    Emitter emit_;
    //! Thrill context
    Context& context_;
    size_t dia_id_;
    //! Reduce configuration used
    ReduceConfig config_;
    //! Reduce table used to count keys
    Table table_;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_LOCATION_DETECTION_HEADER

/******************************************************************************/
