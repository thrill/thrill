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
#include <thrill/core/dynamic_bitset.hpp>
#include <thrill/core/golomb_reader.hpp>
#include <thrill/core/multiway_merge.hpp>
#include <thrill/core/reduce_bucket_hash_table.hpp>
#include <thrill/core/reduce_functional.hpp>
#include <thrill/core/reduce_old_probing_hash_table.hpp>
#include <thrill/core/reduce_probing_hash_table.hpp>
#include <thrill/core/reduce_table.hpp>
#include <thrill/data/cat_stream.hpp>

#include <functional>
#include <unordered_map>

#include <x86intrin.h>
namespace thrill {

struct hash {

    inline size_t operator () (const size_t& n) const {
         size_t hash = _mm_crc32_u32((size_t)28475421, n);
        hash = hash << 32;
        hash += _mm_crc32_u32((size_t)52150599, n);
        return hash;
    }
};
}

namespace thrill {
namespace core {


/*!
 * Emitter for a ReduceTable, which emits all of its data into a vector of hash-counter-pairs.
 *
 * \tparam KeyCounterPair Type of key in table and occurence counter type
 * \tparam HashFunction Hash function for golomb coder
 */


template <bool Join, typename Key, typename CounterType,
          typename LocationType, typename HashFunction, typename DataType>
class ToVectorEmitter;

template <typename Key, typename CounterType,
          typename LocationType, typename HashFunction, typename DataType>
class ToVectorEmitter<true, Key, CounterType, LocationType, HashFunction, DataType>
{
public:
    using HashResult = typename common::FunctionTraits<HashFunction>::result_type;
    using ResultTablePair = std::pair<HashResult, DataType>;
    using TableType = std::pair<Key, DataType>;

    ToVectorEmitter(HashFunction hash_function,
                    std::vector<ResultTablePair>& vec)
        : vec_(vec),
          hash_function_(hash_function) { }

    static void Put(const TableType& /*p*/,
                    data::DynBlockWriter& /*writer*/) {
        /* Should not be called */
        assert(0);
    }

    void SetModulo(size_t modulo) {
        modulo_ = modulo;
    }

    void Emit(const size_t& /*partition_id*/, const TableType& p) {
        assert(modulo_ > 1);
        vec_.emplace_back(hash_function_(p.first) % modulo_, p.second);
    }

    std::vector<ResultTablePair>& vec_;
    size_t modulo_ = 1;
    HashFunction hash_function_;
};

template <typename Key, typename CounterType,
          typename LocationType, typename HashFunction, typename DataType>
class ToVectorEmitter<false, Key, CounterType, LocationType, HashFunction, DataType>
{
public:
    using HashResult = typename common::FunctionTraits<HashFunction>::result_type;
    using CtrIdxType = std::pair<CounterType, LocationType>;
    using ResultTablePair = std::pair<HashResult, CtrIdxType>;
    using TableType = std::pair<Key, DataType>;

    ToVectorEmitter(HashFunction hash_function,
                    std::vector<ResultTablePair>& vec)
        : vec_(vec),
          hash_function_(hash_function) { }

    static void Put(const TableType& /*p*/,
                    data::DynBlockWriter& /*writer*/) {
        /* Should not be called */
        assert(0);
    }

    void SetModulo(size_t modulo) {
        modulo_ = modulo;
    }

    void Emit(const size_t& /*partition_id*/, const TableType& p) {
        assert(modulo_ > 1);
        vec_.emplace_back(hash_function_(p.first) % modulo_,
                          std::make_pair(p.second, (uint8_t) 0));
    }

    std::vector<ResultTablePair>& vec_;
    size_t modulo_ = 1;
    HashFunction hash_function_;
};

template <typename ValueType, typename Key, bool UseLocationDetection,
          typename CounterType, typename DIAIdxType, typename HashFunction,
          typename IndexFunction, typename AddFunction, bool Join>
class LocationDetection
{
    static constexpr bool debug = false;

    using HashResult = typename common::FunctionTraits<HashFunction>::result_type;
    using KeyCounterPair = std::pair<Key, CounterType>;
    using TableType = typename common::FunctionTraits<AddFunction>::result_type;
    using KeyValuePair = std::pair<Key, TableType>;
    using CtrIdxType = std::pair<CounterType, DIAIdxType>;
    using ResultTablePair = std::pair<HashResult, CtrIdxType>;
    using HashCounterPair = std::pair<HashResult, CounterType>;

private:
    void WriteOccurenceCounts(data::CatStreamPtr stream_pointer,
                              const std::vector<ResultTablePair>& occurences,
                              size_t b,
                              size_t space_bound,
                              size_t num_workers,
                              size_t max_hash) {

        std::vector<data::CatStream::Writer> writers =
            stream_pointer->GetWriters();

        // length of counter in bits
        size_t bitsize = 8;
        // total space bound of golomb code
        size_t space_bound_with_counters = space_bound + occurences.size() *
            (bitsize + 2);

        size_t j = 0;
        for (size_t i = 0; i < num_workers; ++i) {
            common::Range range_i = common::CalculateLocalRange(max_hash, num_workers, i);

            core::DynamicBitset<size_t> golomb_code(space_bound_with_counters, false, b);

            golomb_code.seek();

            size_t delta = 0;
            size_t num_elements = 0;

            uint8_t dia_indices;
            for (                        /*j is already set from previous workers*/
                ; j < occurences.size() && occurences[j].first < range_i.end; ++j) {
                //! Send hash deltas to make the encoded bitset smaller.
                num_elements++;

                if (Join) {
                    dia_indices = 0;
                    dia_indices |= occurences[j].second.second;
                }


                // accumulate counters hashing to same value
                size_t acc_occurences = occurences[j].second.first;
                size_t k = j + 1;
                while (k < occurences.size() && occurences[k].first == occurences[j].first) {
                    acc_occurences += occurences[k].second.first;
                    if (Join) {
                        dia_indices |= occurences[k].second.second;
                    }
                    k++;
                }
                //! write counter of all values hashing to occurences[j].first
                //! in next bitsize bits
                golomb_code.golomb_in(occurences[j].first - delta);
                delta = occurences[j].first;
                golomb_code.stream_in(bitsize,
                                      std::min((CounterType)((1 << bitsize) - 1),
                                               occurences[j].second.first));
                if (Join) {
                    golomb_code.stream_in(2, dia_indices);
                } else {
                    golomb_code.stream_in(2, 3);
                }

                j = k - 1;
            }

            //! Send raw data through data stream.
            writers[i].Put(golomb_code.size());
            writers[i].Put(num_elements);
            writers[i].Append(golomb_code.GetGolombData(),
                              golomb_code.size() * sizeof(size_t));
            writers[i].Close();
        }
    }

public:

    using ReduceConfig = DefaultReduceConfig;
    using Emitter = ToVectorEmitter<Join, Key, CounterType,
                                    DIAIdxType, HashFunction, TableType>;
    using Table = typename ReduceTableSelect<
        ReduceConfig::table_impl_, ValueType, Key, TableType,
        std::function<void()>, AddFunction, Emitter,
        false, ReduceConfig, IndexFunction>::type;

    // we don't need the key_extractor function here
    std::function<void()> void_fn = []() {};

    LocationDetection(Context& ctx, size_t dia_id, AddFunction add_function,
                      const HashFunction& hash_function = HashFunction(),
                      const IndexFunction& index_function = IndexFunction(),
                      const ReduceConfig& config = ReduceConfig())
        : emit_(hash_function, data_),
          context_(ctx),
          dia_id_(dia_id),
          config_(config),
          hash_function_(hash_function),
          table_(ctx, dia_id,
                 void_fn,
                 add_function,
                 emit_,
                 1, config, false, index_function, std::equal_to<Key>()) {
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
    void Insert(const Key& key, const TableType& element) {
        table_.Insert(std::make_pair(key, element));
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
        size_t b = (size_t)fpr_parameter;
        size_t upper_space_bound = upper_bound_uniques * (2 + std::log2(fpr_parameter));
        size_t max_hash = b * upper_bound_uniques;

        emit_.SetModulo(max_hash);
        data_.reserve(num_items);
        table_.FlushAll();

        if (table_.has_spilled_data_on_partition(0)) {
            data::File::Reader reader =
                table_.partition_files()[0].GetReader(true);

            while (reader.HasNext()) {
                emit_.Emit(0, reader.Next<KeyValuePair>());
            }
        }

        std::sort(data_.begin(), data_.end(), [](const ResultTablePair& hcp1,
                                                 const ResultTablePair& hcp2) {
                      return hcp1.first < hcp2.first;
                  });

        data::CatStreamPtr golomb_data_stream = context_.GetNewCatStream(dia_id_);

        WriteOccurenceCounts(golomb_data_stream,
                             data_, b,
                             upper_space_bound,
                             context_.num_workers(),
                             max_hash);

        std::vector<data::BlockReader<data::ConsumeBlockQueueSource> > readers =
            golomb_data_stream->GetReaders();

        std::vector<GolombPairReader<CounterType> > g_readers;

        std::vector<std::unique_ptr<size_t[]> > data_pointers;

        data_pointers.reserve(context_.num_workers());

        size_t total_elements = 0;

        for (auto & reader : readers) {
            assert(reader.HasNext());
            size_t data_size = reader.template Next<size_t>();
            size_t num_elements = reader.template Next<size_t>();
            data_pointers.push_back(
                std::make_unique<size_t[]>(data_size + 1));
            reader.Read(data_pointers.back().get(), data_size * sizeof(size_t));
            total_elements += num_elements;

            g_readers.push_back(
                GolombPairReader<CounterType>(data_size,
                                              data_pointers.back().get(),
                                              num_elements, b, 8));
        }


        auto puller = make_multiway_merge_tree<ResultTablePair>
                          (g_readers.begin(), g_readers.end(),
                          [](const ResultTablePair& hcp1,
                             const ResultTablePair& hcp2) {
                              return hcp1.first < hcp2.first;
                          });

        size_t processor_bitsize = std::max(common::IntegerLog2Ceil(
                                                context_.num_workers()),
                                            (unsigned int)1);
        size_t space_bound_with_processors = upper_bound_uniques * processor_bitsize
            + upper_space_bound;
        core::DynamicBitset<size_t> location_bitset(
            space_bound_with_processors * 3 / 2, false, b);

        std::pair<ResultTablePair, unsigned int> next;

        bool finished = !puller.HasNext();

        if (!finished)
            next = puller.NextWithSource();

        HashResult delta = 0;
        size_t num_elements = 0;

        while (!finished) {
            uint8_t idx = 0;
            size_t src_max = 0;
            CounterType max = 0;
            HashResult next_hr = next.first.first;
            while (next.first.first == next_hr && !finished) {
                idx |= next.first.second.second;
                assert(next.first.second.second == 1 ||
                       next.first.second.second == 2 ||
                       next.first.second.second == 3);
                if (next.first.second.first > max) {
                    src_max = next.second;
                    max = next.first.second.first;
                }
                if (puller.HasNext()) {
                    next = puller.NextWithSource();
                }
                else {
                    finished = true;
                }
            }

            if (idx == 3) {
                num_elements++;
                assert(next_hr > delta || next_hr == 0);
                location_bitset.golomb_in(next_hr - delta);
                delta = next_hr;
                location_bitset.stream_in(processor_bitsize, src_max);
            }
        }

        data::CatStreamPtr duplicates_stream = context_.GetNewCatStream(dia_id_);

        std::vector<data::CatStream::Writer> duplicate_writers =
            duplicates_stream->GetWriters();

        //! Send all duplicates to all workers (golomb encoded).
        for (size_t i = 0; i < duplicate_writers.size(); ++i) {
            duplicate_writers[i].Put(location_bitset.size());
            duplicate_writers[i].Put(num_elements);
            duplicate_writers[i].Append(location_bitset.GetGolombData(),
                                        location_bitset.size() *
                                        sizeof(size_t));
            duplicate_writers[i].Close();
        }

        size_t uniques = context_.net.AllReduce(num_elements);


        if (context_.my_rank() == 0) {
            LOG1 << "uniques: " << uniques;
        }

        auto duplicates_reader = duplicates_stream->GetCatReader(/* consume */ true);

        target_processors.reserve(uniques);

        while (duplicates_reader.HasNext()) {

            size_t data_size = duplicates_reader.template Next<size_t>();
            size_t num_elements = duplicates_reader.template Next<size_t>();
            size_t* raw_data = new size_t[data_size * 2];
            duplicates_reader.Read(raw_data, data_size * sizeof(size_t));

            //! Builds golomb encoded bitset from data recieved by the stream.
            core::DynamicBitset<size_t> golomb_code(raw_data, data_size,
                                                    b, num_elements);
            golomb_code.seek();

            size_t last = 0;
            for (size_t i = 0; i < num_elements; ++i) {
                //! Golomb code contains deltas, we want the actual values in target_vec
                size_t new_elem = golomb_code.golomb_out() + last;

                last = new_elem;

                size_t processor = golomb_code.stream_out(processor_bitsize);
                target_processors[new_elem] = processor;

            }

            delete[] raw_data;
        }

        return max_hash;
    }

    // Target vector for vector emitter
    std::vector<ResultTablePair> data_;
    // Emitter to vector
    Emitter emit_;
    // Thrill context
    Context& context_;
    size_t dia_id_;
    // Reduce configuration used
    ReduceConfig config_;
    // Hash function used in table and location detection (default: std::hash<Key>)
    HashFunction hash_function_;
    // Reduce table used to count keys
    Table table_;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_LOCATION_DETECTION_HEADER

/******************************************************************************/
