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
#include <thrill/common/math.hpp>
#include <thrill/core/dynamic_bitset.hpp>
#include <thrill/core/reduce_bucket_hash_table.hpp>
#include <thrill/core/reduce_functional.hpp>
#include <thrill/core/reduce_old_probing_hash_table.hpp>
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

//! template specialization switch class to output key+value if VolatileKey and
//! only value if not VolatileKey (RobustKey).
template <typename KeyValuePair, bool VolatileKey>
class ReducePreStageEmitterSwitch;

template <typename KeyValuePair>
class ReducePreStageEmitterSwitch<KeyValuePair, false>
{
public:
    static void Put(const KeyValuePair& p, data::DynBlockWriter& writer) {
        writer.Put(p.second);
    }
};

template <typename KeyValuePair>
class ReducePreStageEmitterSwitch<KeyValuePair, true>
{
public:
    static void Put(const KeyValuePair& p, data::DynBlockWriter& writer) {
        writer.Put(p);
    }
};

//! Emitter implementation to plug into a reduce hash table for
//! collecting/flushing items while reducing. Items flushed in the pre-stage are
//! transmitted via a network Channel.
template <typename KeyValuePair, bool VolatileKey>
class ReducePreStageEmitter
{
    static constexpr bool debug = false;

public:
    explicit ReducePreStageEmitter(std::vector<data::DynBlockWriter>& writer)
        : writer_(writer),
          stats_(writer.size(), 0) { }

    //! output an element into a partition, template specialized for robust and
    //! non-robust keys
    void Emit(const size_t& partition_id, const KeyValuePair& p) {
        assert(partition_id < writer_.size());
        stats_[partition_id]++;
        ReducePreStageEmitterSwitch<KeyValuePair, VolatileKey>::Put(
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
          typename IndexFunction = ReduceByHash<Key>,
          typename EqualToFunction = std::equal_to<Key> >
class ReducePreStage
{
    static constexpr bool debug = false;

public:
    using KeyValuePair = std::pair<Key, Value>;
    using ReduceConfig = ReduceConfig_;

    using Emitter = ReducePreStageEmitter<KeyValuePair, VolatileKey>;

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
    ReducePreStage(Context& ctx, size_t dia_id,
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
                 num_partitions, config, /* immediate_flush */ false,
                 index_function, equal_to_function) {
        sLOG << "creating ReducePreStage with" << emit.size() << "output emitters";

        assert(num_partitions == emit.size());
    }

    //! non-copyable: delete copy-constructor
    ReducePreStage(const ReducePreStage&) = delete;
    //! non-copyable: delete assignment operator
    ReducePreStage& operator = (const ReducePreStage&) = delete;

    void Initialize(size_t limit_memory_bytes) {
        table_.Initialize(limit_memory_bytes);
    }

	

    void Insert(const Value& p) {
		total_elements_++;
		if (table_.Insert(p)) {
			unique_elements_++;
		    hashes_.push_back(std::hash<Key>()(key_extractor_(p)));
		}
    }

    void Insert(const KeyValuePair& kv) {
		total_elements_++;
        if(table_.Insert(kv)) {
			unique_elements_++;
			hashes_.push_back(std::hash<Key>()(kv.first));
		}
    }

    //! Flush all partitions
    void FlushAll() {
		
		size_t upper_bound_uniques = table_.ctx().net.AllReduce(unique_elements_);

		double fpr_parameter = 8;
		size_t b = (size_t)(std::log(2) * fpr_parameter);
		size_t upper_space_bound = upper_bound_uniques * (2 + std::log2(fpr_parameter));

		size_t max_hash = upper_bound_uniques * fpr_parameter;

		for (size_t i = 0; i < hashes_.size(); ++i) {
			hashes_[i] = hashes_[i] % max_hash;
		}
		
		std::sort(hashes_.begin(), hashes_.end());

		size_t num_workers = table_.ctx().num_workers();

		for (size_t i = 0; i < num_workers; ++i) {
			common::Range range_i = common::CalculateLocalRange(max_hash, num_workers, i);

			//TODO: Lower bound.
			core::DynamicBitset<size_t>
				golomb_code(upper_space_bound, false, b);

			golomb_code.clear();
			golomb_code.seek(0);

			size_t delta = 0;

			for (size_t j = 0; j < hashes_.size() && hashes_[j] < range_i.end; ++j) {
				if (hashes_[j] != delta) {
					assert(hashes_[j] > delta);
					golomb_code.golomb_in(hashes_[j] - delta);
					delta = hashes_[j];
				}
			}
		}
		
        for (size_t id = 0; id < table_.num_partitions(); ++id) {
            FlushPartition(id, /* consume */ true);
        }
    }

    //! Flushes all items of a partition.
    void FlushPartition(size_t partition_id, bool consume) {

        table_.FlushPartition(partition_id, consume);

		
		if (table_.has_spilled_data_on_partition(partition_id)) {
			data::File::Reader reader = 
				table_.partition_files()[partition_id].GetReader(/* consume */ true);
			while (reader.HasNext()) {
				emit_.Emit(partition_id, reader.Next<KeyValuePair>());
			}
		}

        // flush elements pushed into emitter
        emit_.Flush(partition_id);
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

	std::vector<size_t> hashes_;

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

	size_t unique_elements_ = 0;
	size_t total_elements_ = 0;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_PRE_STAGE_HEADER

/******************************************************************************/
