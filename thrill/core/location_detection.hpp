/*******************************************************************************
 * thrill/core/location_detection.hpp
 *
 * Detection of element locations using a distributed bloom filter
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_LOCATION_DETECTION_HEADER
#define THRILL_CORE_LOCATION_DETECTION_HEADER

#include <thrill/common/function_traits.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/duplicate_detection.hpp>
#include <thrill/core/reduce_functional.hpp>
#include <thrill/core/reduce_table.hpp>
#include <thrill/core/reduce_bucket_hash_table.hpp>
#include <thrill/core/reduce_probing_hash_table.hpp>
#include <thrill/core/reduce_old_probing_hash_table.hpp>
#include <thrill/data/cat_stream.hpp>

#include <functional>

namespace thrill {
namespace core {

/*!
 * Emitter for a ReduceTable, which emits all of its data into a vector of hash-counter-pairs.
 *
 * \tparam KeyCounterPair Type of key in table and occurence counter type
 * \tparam HashFunction Hash function for golomb coder
 */ 
template <typename KeyCounterPair, typename HashFunction>
class ToVectorEmitter {
public:
	using HashResult = typename common::FunctionTraits<HashFunction>::result_type;
	using HashCounterPair = std::pair<HashResult, typename KeyCounterPair::second_type>;

	ToVectorEmitter(HashFunction hash_function,
					std::vector<HashCounterPair>& vec) 
		: vec_(vec),
		  hash_function_(hash_function) { }

    static void Put(const KeyCounterPair& p, data::DynBlockWriter& writer) {
        assert(0);
    }

	void SetModulo(size_t modulo) {
		modulo_ = modulo;
	}

	void Emit(const size_t& /*partition_id*/, const KeyCounterPair& p) {
		assert(modulo_ > 1);
		vec_.push_back(std::make_pair(hash_function_(p.first) % modulo_, p.second));
    }

	std::vector<HashCounterPair>& vec_;
	size_t modulo_ = 1;
	HashFunction hash_function_;
};

template <typename ValueType, typename Key, bool UseLocationDetection,
		  typename CounterType, typename IndexFunction,
		  typename HashFunction, typename AddFunction>
class LocationDetection 
{
	static constexpr bool debug = false;
	
	using HashResult = typename common::FunctionTraits<HashFunction>::result_type;
	using HashCounterPair = std::pair<HashResult, CounterType>;
	using KeyCounterPair = std::pair<Key, CounterType>;

private:

	void WriteOccurenceCounts(data::CatStreamPtr stream_pointer,
							  const std::vector<HashCounterPair>& occurences,
							  size_t b,
							  size_t space_bound,
							  size_t num_workers,
							  size_t max_hash) {
		
        std::vector<data::CatStream::Writer> writers =
            stream_pointer->GetWriters();

		//length of counter in bits 
		size_t bitsize = 8;
		//total space bound of golomb code
		size_t space_bound_with_counters = space_bound + occurences.size() * bitsize; 
		
		size_t j = 0;
        for (size_t i = 0; i < num_workers; ++i) {
            common::Range range_i = common::CalculateLocalRange(max_hash, num_workers, i);

            core::DynamicBitset<size_t> golomb_code(space_bound_with_counters, false, b);

            golomb_code.seek();

            size_t delta = 0;
			size_t num_elements = 0;

			for (            /*j is already set from previous workers*/
                ; j < occurences.size() && occurences[j].first < range_i.end; ++j) {
                //! Send hash deltas to make the encoded bitset smaller.
				golomb_code.golomb_in(occurences[j].first - delta);
				delta = occurences[j].first;
				num_elements++;
			    
				//! write counter in next bitsize bits
				golomb_code.stream_in(bitsize, 
									  std::min(((CounterType) 1 << bitsize) - 1,
											   occurences[j].second));
            }

            //! Send raw data through data stream.
            writers[i].Put(golomb_code.byte_size() + sizeof(size_t));
            writers[i].Put(num_elements);
            writers[i].Append(golomb_code.GetGolombData(),
                              golomb_code.byte_size() + sizeof(size_t));
            writers[i].Close();
        }
	}


public:
	
	using ReduceConfig = DefaultReduceConfig;
	using Emitter = ToVectorEmitter<KeyCounterPair, HashFunction>;
	using Table = typename ReduceTableSelect<
		ReduceConfig::table_impl_, ValueType, Key, CounterType,
	    std::function<void(void)>, AddFunction, Emitter,
		false, ReduceConfig, IndexFunction>::type;


	// we don't need the key_extractor function here
	std::function<void()> void_fn = [](){};	
	
	LocationDetection(Context& ctx, size_t dia_id, AddFunction add_function,
					  const IndexFunction& index_function = IndexFunction(),
					  const HashFunction& hash_function = HashFunction(),
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
	void Insert(const Key& key) {
		table_.Insert(std::make_pair(key, (CounterType) 1));
	}

	/*!
	 * Flushes the table and detects the most common location for each element.
	 */ 
	void Flush() {

		//golomb code parameters
		size_t upper_bound_uniques = context_.net.AllReduce(table_.num_items());
        double fpr_parameter = 8;
        size_t b = (size_t)fpr_parameter; 
        size_t upper_space_bound = upper_bound_uniques * (2 + std::log2(fpr_parameter));
        size_t max_hash = upper_bound_uniques * fpr_parameter;
		
		emit_.SetModulo(max_hash);

		table_.FlushAll();

		std::sort(data_.begin(), data_.end());

		data::CatStreamPtr golomb_data_stream = context_.GetNewCatStream(dia_id_);

		WriteOccurenceCounts(golomb_data_stream,
							 data_, b,
							 upper_space_bound,
							 context_.num_workers(),
							 max_hash);

		std::vector<data::BlockReader<data::ConsumeBlockQueueSource>> readers = golomb_data_stream->GetReaders();
		
	}

	// Target vector for vector emitter
	std::vector<HashCounterPair> data_;
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
