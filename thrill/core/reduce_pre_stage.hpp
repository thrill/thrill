/*******************************************************************************
 * thrill/core/reduce_pre_stage.hpp
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
#ifndef THRILL_CORE_REDUCE_PRE_STAGE_HEADER
#define THRILL_CORE_REDUCE_PRE_STAGE_HEADER

#include <thrill/common/logger.hpp>
#include <thrill/core/reduce_bucket_hash_table.hpp>
#include <thrill/core/reduce_functional.hpp>
#include <thrill/core/reduce_probing_hash_table.hpp>
#include <thrill/data/block_writer.hpp>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <functional>
#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace core {

//! template specialization switch class to output key+value if NonRobustKey and
//! only value if RobustKey.
template <typename KeyValuePair, bool RobustKey>
class ReducePreStageEmitterSwitch;

template <typename KeyValuePair>
class ReducePreStageEmitterSwitch<KeyValuePair, false>
{
public:
    static void Put(const KeyValuePair& p, data::DynBlockWriter& writer) {
        writer.Put(p);
    }
};

template <typename KeyValuePair>
class ReducePreStageEmitterSwitch<KeyValuePair, true>
{
public:
    static void Put(const KeyValuePair& p, data::DynBlockWriter& writer) {
        writer.Put(p.second);
    }
};

//! Emitter implementation to plug into a reduce hash table for
//! collecting/flushing items while reducing. Items flushed in the pre-stage are
//! transmitted via a network Channel.
template <typename KeyValuePair, bool RobustKey>
class ReducePreStageEmitter
{
    static const bool debug = true;

public:
    explicit ReducePreStageEmitter(std::vector<data::DynBlockWriter>& writer)
        : writer_(writer),
          stats_(writer.size(), 0) { }

    //! output an element into a partition, template specialized for robust and
    //! non-robust keys
    void Emit(const size_t& partition_id, const KeyValuePair& p) {
        assert(partition_id < writer_.size());
        stats_[partition_id]++;
        ReducePreStageEmitterSwitch<KeyValuePair, RobustKey>::Put(
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
          const bool RobustKey,
          typename IndexFunction,
          typename EqualToFunction,
          template <typename _ValueType, typename _Key, typename _Value,
                    typename _KeyExtractor, typename _ReduceFunction, typename _Emitter,
                    const bool _RobustKey,
                    typename _IndexFunction,
                    typename _EqualToFunction> class HashTable>
class ReducePreStage
{
    static const bool debug = false;

public:
    using KeyValuePair = std::pair<Key, Value>;

    using Emitter = ReducePreStageEmitter<KeyValuePair, RobustKey>;

    using Table = HashTable<
              ValueType, Key, Value,
              KeyExtractor, ReduceFunction, Emitter,
              RobustKey,
              IndexFunction, EqualToFunction>;

    /**
     * A data structure which takes an arbitrary value and extracts a key using
     * a key extractor function from that value. Afterwards, the value is hashed
     * based on the key into some slot.
     */
    ReducePreStage(Context& ctx,
                   size_t num_partitions,
                   KeyExtractor key_extractor,
                   ReduceFunction reduce_function,
                   std::vector<data::DynBlockWriter>& emit,
                   const IndexFunction& index_function,
                   const Key& sentinel = ProbingTableTraits<Key>::Sentinel(),
                   const Value& neutral_element = Value(),
                   size_t limit_memory_bytes = 1024* 16,
                   double limit_partition_fill_rate = 0.6,
                   double bucket_rate = 1.0,
                   const EqualToFunction& equal_to_function = EqualToFunction())
        : emit_(emit),
          table_(ctx,
                 key_extractor, reduce_function, emit_,
                 num_partitions,
                 limit_memory_bytes,
                 limit_partition_fill_rate, bucket_rate, true,
                 sentinel,
                 index_function, equal_to_function),
          neutral_element_(neutral_element) {
        sLOG << "creating ReducePreStage with" << emit.size() << "output emitters";

        assert(num_partitions == emit.size());
    }

    ReducePreStage(Context& ctx, size_t num_partitions, KeyExtractor key_extractor,
                   ReduceFunction reduce_function,
                   std::vector<data::DynBlockWriter>& emit)
        : ReducePreStage(
              ctx, num_partitions, key_extractor, reduce_function, emit, IndexFunction()) { }

    //! non-copyable: delete copy-constructor
    ReducePreStage(const ReducePreStage&) = delete;
    //! non-copyable: delete assignment operator
    ReducePreStage& operator = (const ReducePreStage&) = delete;

    void Insert(const Value& p) {
        return table_.Insert(p);
    }

    void Insert(const KeyValuePair& kv) {
        return table_.Insert(kv);
    }

    //! Flush all partitions
    void Flush(bool consume = true) {
        for (size_t id = 0; id < table_.num_partitions(); ++id) {
            FlushPartition(id, consume);
        }
    }

    //! Flushes all items of a partition.
    void FlushPartition(size_t partition_id, bool consume) {

        table_.FlushPartition(partition_id, consume);

        // flush elements pushed into emitter
        emit_.Flush(partition_id);
    }

    //! Emits element to all children
    void EmitAll(const size_t& partition_id, const KeyValuePair& p) {
        emit_.Emit(p, partition_id);
    }

    //! Returns the neutral element.
    Value NeutralElement() const {
        return neutral_element_;
    }

    //! Closes all emitter
    void CloseAll() {
        emit_.CloseAll();
    }

    //! \name Accessors
    //! {

    //! Returns the total num of items in the table.
    size_t num_items() const { return table_.num_items(); }

    //! }

private:
    //! Emitters used to parameterize hash table for output to network.
    Emitter emit_;

    //! the first-level hash table implementation
    Table table_;

    //! Neutral element (reduce to index).
    Value neutral_element_;
};

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool RobustKey = false,
          typename IndexFunction = ReduceByHashKey<Key>,
          typename EqualToFunction = std::equal_to<Key> >
using ReducePreBucketStage = ReducePreStage<
          ValueType, Key, Value,
          KeyExtractor, ReduceFunction,
          RobustKey,
          IndexFunction, EqualToFunction,
          ReduceBucketHashTable>;

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool RobustKey = false,
          typename IndexFunction = ReduceByHashKey<Key>,
          typename EqualToFunction = std::equal_to<Key> >
using ReducePreProbingStage = ReducePreStage<
          ValueType, Key, Value,
          KeyExtractor, ReduceFunction,
          RobustKey,
          IndexFunction, EqualToFunction,
          ReduceProbingHashTable>;

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_PRE_STAGE_HEADER

/******************************************************************************/
