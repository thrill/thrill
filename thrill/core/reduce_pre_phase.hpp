/*******************************************************************************
 * thrill/core/reduce_pre_phase.hpp
 *
 * Hash table with support for reduce and partitions.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_REDUCE_PRE_PHASE_HEADER
#define THRILL_CORE_REDUCE_PRE_PHASE_HEADER

#include <thrill/common/logger.hpp>
#include <thrill/common/math.hpp>
#include <thrill/core/duplicate_detection.hpp>
#include <thrill/core/golomb_reader.hpp>
#include <thrill/core/reduce_bucket_hash_table.hpp>
#include <thrill/core/reduce_functional.hpp>
#include <thrill/core/reduce_old_probing_hash_table.hpp>
#include <thrill/core/reduce_probing_hash_table.hpp>
#include <thrill/data/block_reader.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <functional>
#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace core {

//! template specialization switch class to output key+value if VolatileKey and
//! only value if not VolatileKey (RobustKey).
template <typename KeyValuePair, bool VolatileKey>
class ReducePrePhaseEmitterSwitch;

template <typename KeyValuePair>
class ReducePrePhaseEmitterSwitch<KeyValuePair, false>
{
public:
    static void Put(const KeyValuePair& p, data::DynBlockWriter& writer) {
        writer.Put(p.second);
    }
};

template <typename KeyValuePair>
class ReducePrePhaseEmitterSwitch<KeyValuePair, true>
{
public:
    static void Put(const KeyValuePair& p, data::DynBlockWriter& writer) {
        writer.Put(p);
    }
};

//! Emitter implementation to plug into a reduce hash table for
//! collecting/flushing items while reducing. Items flushed in the pre-phase are
//! transmitted via a network Channel.
template <typename KeyValuePair, bool VolatileKey>
class ReducePrePhaseEmitter
{
    static constexpr bool debug = false;

public:
    explicit ReducePrePhaseEmitter(std::vector<data::DynBlockWriter>& writer)
        : writer_(writer),
          stats_(writer.size(), 0) { }

    //! output an element into a partition, template specialized for robust and
    //! non-robust keys
    void Emit(const size_t& partition_id, const KeyValuePair& p) {
        assert(partition_id < writer_.size());
        stats_[partition_id]++;
        ReducePrePhaseEmitterSwitch<KeyValuePair, VolatileKey>::Put(
            p, writer_[partition_id]);
    }

    void Flush(size_t partition_id) {
        assert(partition_id < writer_.size());
        writer_[partition_id].Flush();
    }

    void CloseAll() {
        sLOG << "emit stats:";
        size_t i = 0;
        for (data::DynBlockWriter& e : writer_) {
            e.Close();
            sLOG << "emitter" << i << "pushed" << stats_[i++];
        }
    }

public:
    //! Set of emitters, one per partition.
    std::vector<data::DynBlockWriter>& writer_;

    //! Emitter stats.
    std::vector<size_t> stats_;
};

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool VolatileKey,
          typename ReduceConfig_ = DefaultReduceConfig,
          bool UseDuplicateDetection = false,
          typename IndexFunction = ReduceByHash<Key>,
          typename EqualToFunction = std::equal_to<Key> >
class ReducePrePhase
{
    static constexpr bool debug = false;

public:
    using KeyValuePair = std::pair<Key, Value>;
    using ReduceConfig = ReduceConfig_;

    using Emitter = ReducePrePhaseEmitter<KeyValuePair, VolatileKey>;

    using Table = typename ReduceTableSelect<
              ReduceConfig::table_impl_,
              ValueType, Key, Value,
              KeyExtractor, ReduceFunction, Emitter,
              VolatileKey, ReduceConfig, IndexFunction, EqualToFunction>::type;

    /*!
     * A data structure which takes an arbitrary value and extracts a key using
     * a key extractor function from that value. Afterwards, the value is hashed
     * based on the key into some slot.
     */
    ReducePrePhase(Context& ctx, size_t dia_id,
                   size_t num_partitions,
                   KeyExtractor key_extractor,
                   ReduceFunction reduce_function,
                   std::vector<data::DynBlockWriter>& emit,
                   const ReduceConfig& config = ReduceConfig(),
                   const IndexFunction& index_function = IndexFunction(),
                   const EqualToFunction& equal_to_function = EqualToFunction())
        : emit_(emit),
          key_extractor_(key_extractor),
          table_(ctx, dia_id,
                 key_extractor, reduce_function, emit_,
                 num_partitions, config, /* immediate_flush */ !UseDuplicateDetection,
                 index_function, equal_to_function) {
        sLOG << "creating ReducePrePhase with" << emit.size() << "output emitters";

        assert(num_partitions == emit.size());
    }

    //! non-copyable: delete copy-constructor
    ReducePrePhase(const ReducePrePhase&) = delete;
    //! non-copyable: delete assignment operator
    ReducePrePhase& operator = (const ReducePrePhase&) = delete;

    void Initialize(size_t limit_memory_bytes) {
        table_.Initialize(limit_memory_bytes);
    }

    void Insert(const Value& p) {
        if (table_.Insert(p) && UseDuplicateDetection) {
            hashes_.push_back(std::hash<Key>()(key_extractor_(p)));
        }
    }

    void Insert(const KeyValuePair& kv) {
        if (table_.Insert(kv) && UseDuplicateDetection) {
            hashes_.push_back(std::hash<Key>()(kv.first));
        }
    }

    //! Flush all partitions
    void FlushAll() {
        if (UseDuplicateDetection) {
            DuplicateDetection dup_detect;
            max_hash_ = dup_detect.FindNonDuplicates(non_duplicates_,
                                                     hashes_,
                                                     table_.ctx(),
                                                     table_.dia_id());
        }

        for (size_t id = 0; id < table_.num_partitions(); ++id) {
            FlushPartition(id, /* consume */ true, /* grow */ false);
        }
    }

    //! Flushes all items of a partition.
    void FlushPartition(size_t partition_id, bool consume, bool grow) {
        if (UseDuplicateDetection) {
            table_.FlushPartitionEmit(
                partition_id, consume, grow,
                [this](const size_t& partition_id, const KeyValuePair& p) {
                    if (!std::binary_search(non_duplicates_.begin(),
                                            non_duplicates_.end(),
                                            (std::hash<Key>()(p.first) %
                                             max_hash_))) {

                        duplicated_elements_++;
                        emit_.Emit(partition_id, p);
                    }
                    else {
                        non_duplicate_elements_++;
                        emit_.Emit(table_.ctx().my_rank(), p);
                    }
                });

            if (table_.has_spilled_data_on_partition(partition_id)) {
                data::File::Reader reader =
                    table_.partition_files()[partition_id].GetReader(true);
                while (reader.HasNext()) {
                    KeyValuePair kv = reader.Next<KeyValuePair>();
                    if (!std::binary_search(non_duplicates_.begin(),
                                            non_duplicates_.end(),
                                            (std::hash<Key>()(kv.first) %
                                             max_hash_))) {

                        duplicated_elements_++;
                        emit_.Emit(partition_id, kv);
                    }
                    else {
                        non_duplicate_elements_++;
                        emit_.Emit(table_.ctx().my_rank(), kv);
                    }
                }
            }

            // flush elements pushed into emitter
            emit_.Flush(partition_id);
            emit_.Flush(table_.ctx().my_rank());
        }
        else {
            table_.FlushPartition(partition_id, consume, grow);
            // data is flushed immediately, there is no spilled data
        }
    }

    //! Closes all emitter
    void CloseAll() {
        if (UseDuplicateDetection) {
            LOG << "Reduce Pre-Stage completed."
                << " #duplicates: " << duplicated_elements_
                << " #non-duplicates: " << non_duplicate_elements_;
        }
        emit_.CloseAll();
        table_.Dispose();
    }

    //! \name Accessors
    //! \{

    //! Returns the total num of items in the table.
    size_t num_items() const { return table_.num_items(); }

    //! calculate key range for the given output partition
    common::Range key_range(size_t partition_id)
    { return table_.key_range(partition_id); }

    //! \}

private:
    //! Emitters used to parameterize hash table for output to network.
    Emitter emit_;

    //! extractor function which maps a value to it's key
    KeyExtractor key_extractor_;

    //! the first-level hash table implementation
    Table table_;

    //! \name Duplicate Detection
    //! \{

    //! Hashes of all keys.
    std::vector<size_t> hashes_;
    //! All elements occuring on more than one worker. (Elements not appearing here
    //! can be reduced locally)
    std::vector<size_t> non_duplicates_;

    //! Number of non-duplicates sent to a worker
    size_t non_duplicate_elements_ = 0;
    //! Number of duplicates reduced locally.
    size_t duplicated_elements_ = 0;
    //! Modulo for all hashes in duplicate detection to reduce hash space.
    size_t max_hash_;

    //! \}
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_PRE_PHASE_HEADER

/******************************************************************************/
