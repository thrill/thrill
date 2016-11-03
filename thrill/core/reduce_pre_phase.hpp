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

//! Emitter implementation to plug into a reduce hash table for
//! collecting/flushing items while reducing. Items flushed in the pre-phase are
//! transmitted via a network Channel.
template <typename TableItem, bool VolatileKey>
class ReducePrePhaseEmitter
{
    static constexpr bool debug = false;

public:
    explicit ReducePrePhaseEmitter(std::vector<data::DynBlockWriter>& writer)
        : writer_(writer),
          stats_(writer.size(), 0) { }

    //! output an element into a partition, template specialized for robust and
    //! non-robust keys
    void Emit(const size_t& partition_id, const TableItem& p) {
        assert(partition_id < writer_.size());
        stats_[partition_id]++;
        writer_[partition_id].Put(p);
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

template <typename TableItem, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool VolatileKey,
          typename ReduceConfig_ = DefaultReduceConfig,
          typename IndexFunction = ReduceByHash<Key>,
          typename KeyEqualFunction = std::equal_to<Key> >
class ReducePrePhase
{
    static constexpr bool debug = false;

public:
    using ReduceConfig = ReduceConfig_;
    using Emitter = ReducePrePhaseEmitter<TableItem, VolatileKey>;
    using MakeTableItem = ReduceMakeTableItem<Value, TableItem, VolatileKey>;

    using Table = typename ReduceTableSelect<
              ReduceConfig::table_impl_,
              TableItem, Key, Value,
              KeyExtractor, ReduceFunction, Emitter,
              VolatileKey, ReduceConfig, IndexFunction, KeyEqualFunction>::type;

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
                   const KeyEqualFunction& key_equal_function = KeyEqualFunction(),
                   bool duplicates = false)
        : emit_(emit),
          key_extractor_(key_extractor),
          table_(ctx, dia_id,
                 key_extractor, reduce_function, emit_,
                 num_partitions, config, !duplicates,
                 index_function, key_equal_function) {

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

    bool Insert(const Value& v) {
        // for VolatileKey this makes std::pair and extracts the key
        return table_.Insert(MakeTableItem::Make(v, table_.key_extractor()));
    }

    //! Flush all partitions
    void FlushAll() {
        for (size_t id = 0; id < table_.num_partitions(); ++id) {
            FlushPartition(id, /* consume */ true, /* grow */ false);
        }
    }

    //! Flushes a partition
    void FlushPartition(size_t partition_id, bool consume, bool grow) {
        table_.FlushPartition(partition_id, consume, grow);
        // data is flushed immediately, there is no spilled data
    }

    //! Closes all emitter
    void CloseAll() {
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

protected:
    //! Emitters used to parameterize hash table for output to network.
    Emitter emit_;

    //! extractor function which maps a value to it's key
    KeyExtractor key_extractor_;

    //! the first-level hash table implementation
    Table table_;
};

template <typename TableItem, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool VolatileKey,
          typename ReduceConfig = DefaultReduceConfig,
          typename IndexFunction = ReduceByHash<Key>,
          typename EqualToFunction = std::equal_to<Key>,
          typename HashFunction = std::hash<Key> >
class ReducePrePhaseDuplicates : public ReducePrePhase<TableItem, Key, Value,
                                                       KeyExtractor,
                                                       ReduceFunction,
                                                       VolatileKey,
                                                       ReduceConfig,
                                                       IndexFunction,
                                                       EqualToFunction>
{

public:
    using Super = ReducePrePhase<TableItem, Key, Value, KeyExtractor,
                                 ReduceFunction, VolatileKey, ReduceConfig,
                                 IndexFunction, EqualToFunction>;
    using KeyValuePair = std::pair<Key, Value>;


    ReducePrePhaseDuplicates(Context& ctx, size_t dia_id,
                             size_t num_partitions,
                             KeyExtractor key_extractor,
                             ReduceFunction reduce_function,
                             std::vector<data::DynBlockWriter>& emit,
                             const ReduceConfig& config = ReduceConfig(),
                             const IndexFunction& index_function =
                                 IndexFunction(),
                             const EqualToFunction& equal_to_function =
                                 EqualToFunction(),
                             const HashFunction hash_function = HashFunction())
        : Super(ctx, dia_id, num_partitions, key_extractor, reduce_function, emit,
                config, index_function, equal_to_function, /*duplicates*/ true),
          hash_function_(hash_function) { }

    void Insert(const Value& v) {
        if (Super::table_.Insert(
                Super::MakeTableItem::Make(v, Super::table_.key_extractor()))) {
            hashes_.push_back(hash_function_(Super::key_extractor_(v)));
        }
    }


    //! Flush all partitions
    void FlushAll() {
        DuplicateDetection dup_detect;
        max_hash_ = dup_detect.FindNonDuplicates(non_duplicates_,
                                                 hashes_,
                                                 Super::table_.ctx(),
                                                 Super::table_.dia_id());

        for (size_t id = 0; id < Super::table_.num_partitions(); ++id) {
            FlushPartition(id, /* consume */ true, /* grow */ false);
        }
    }

    void FlushPartition(size_t partition_id, bool consume, bool grow) {
        Super::table_.FlushPartitionEmit(
            partition_id, consume, grow,
            [this](const size_t& partition_id, const TableItem& ti) {
                Key key = Super::MakeTableItem::GetKey(
                    ti, Super::table_.key_extractor());
                if (!non_duplicates_[hash_function_(key) % max_hash_]) {

                    duplicated_elements_++;
                    Super::emit_.Emit(partition_id, ti);
                }
                else {
                    non_duplicate_elements_++;
                    Super::emit_.Emit(Super::table_.ctx().my_rank(), ti);
                }
            });

        if (Super::table_.has_spilled_data_on_partition(partition_id)) {
            data::File::Reader reader =
                Super::table_.partition_files()[partition_id].GetReader(true);
            while (reader.HasNext()) {
                TableItem ti = reader.Next<TableItem>();
                Key key = Super::MakeTableItem::GetKey(
                    ti, Super::table_.key_extractor());
                if (!non_duplicates_[hash_function_(key) % max_hash_]) {

                    duplicated_elements_++;
                    Super::emit_.Emit(partition_id, ti);
                }
                else {
                    non_duplicate_elements_++;
                    Super::emit_.Emit(Super::table_.ctx().my_rank(), ti);
                }
            }
        }

        // flush elements pushed into emitter
        Super::emit_.Flush(partition_id);
        Super::emit_.Flush(Super::table_.ctx().my_rank());
    }

    //! \name Duplicate Detection
    //! \{

    HashFunction hash_function_;
    //! Hashes of all keys.
    std::vector<size_t> hashes_;
    //! All elements occuring on more than one worker. (Elements not appearing here
    //! can be reduced locally)
    std::vector<bool> non_duplicates_;
    //! Modulo for all hashes in duplicate detection to reduce hash space.
    size_t max_hash_;

    size_t duplicated_elements_ = 0;
    size_t non_duplicate_elements_ = 0;

    //! \}
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_PRE_PHASE_HEADER

/******************************************************************************/
