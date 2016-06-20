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

#include <thrill/common/logger.hpp>
#include <thrill/core/reduce_functional.hpp>
#include <thrill/core/reduce_table.hpp>
#include <thrill/core/reduce_bucket_hash_table.hpp>
#include <thrill/core/reduce_probing_hash_table.hpp>
#include <thrill/core/reduce_old_probing_hash_table.hpp>

#include <functional>

namespace thrill {
namespace core {

template <typename KeyCounterPair>
class UnusedEmitter {
public:
    static void Put(const KeyCounterPair& p, data::DynBlockWriter& writer) {
        assert(0);
    }

	void Emit(const size_t& /*partition_id*/, const KeyCounterPair& /*p*/) {
		 assert(0);
    }
};

template <typename ValueType, typename Key, bool UseLocationDetection,
		  typename CounterType, typename HashFunction, typename AddFunction>
class LocationDetection 
{
	static constexpr bool debug = false;

public:

	using KeyCounterPair = std::pair<Key, CounterType>;

	using ReduceConfig = DefaultReduceConfig;
	using Emitter = UnusedEmitter<KeyCounterPair>;


	using Table = typename ReduceTableSelect<
		ReduceConfig::table_impl_, ValueType, Key, CounterType,
	    std::function<void(void)>, AddFunction, Emitter,
		false, ReduceConfig, HashFunction>::type;

	std::function<void()> void_fn = [](){};
	
	
	LocationDetection(Context& ctx, size_t dia_id, AddFunction add_function,
					  const HashFunction& hash_function = HashFunction(),
					  const ReduceConfig& config = ReduceConfig()) 
		: context_(ctx),
		  dia_id_(dia_id),
		  table_(ctx, dia_id,
				 void_fn,
				 add_function,
				 emit_,
				 ctx.num_workers(), config, false, hash_function, std::equal_to<Key>()) {
		sLOG << "creating LocationDetection";
	}

	void Initialize(size_t limit_memory_bytes) {
		table_.Initialize(limit_memory_bytes);
	}

	void Insert(const Key& key) {
		table_.Insert(std::make_pair(key, (CounterType) 1));
	}

	void Flush() {

	}

	Emitter emit_;
	Context& context_;
	size_t dia_id_;
	ReduceConfig config;
	HashFunction hash_function;
	
			   

	Table table_;

};


} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_LOCATION_DETECTION_HEADER

/******************************************************************************/
