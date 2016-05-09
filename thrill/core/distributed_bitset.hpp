/*******************************************************************************
 * thrill/core/distributed_bitset.hpp
 *
 * A distributed bitset used for distributed bloom filtering.
 *
 * Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/


#pragma once
#ifndef THRILL_CORE_DISTRIBUTED_BITSET_HEADER
#define THRILL_CORE_DISTRIBUTED_BITSET_HEADER

#include <thrill/common/logger.hpp>

#include <array>
#include <bitset>

namespace thrill {
namespace core {



using worker_id = size_t;
/**
 * A distributed bitset, which has a set total size and one equal sized part per worker.
 * We assume that the total size divides the number of workers and all parts have equal size.
 */
template<size_t LocalBitsetSize, worker_id NumParts>
class DistributedBitset
{
public:

	DistributedBitset(worker_id my_rank,
					  size_t bitset_size) 
		: my_rank_(my_rank),
		  bitset_size_(bitset_size),
		  my_start_((bitset_size / NumParts) * my_rank),
		  my_end_(((bitset_size / NumParts) * (my_rank + 1)) - 1) {
		assert(bitset_size % NumParts == 0);
		assert(LocalBitsetSize == (my_end_ - my_start_ + 1));
	}

	size_t MyStart() const {
		return my_start_;
	}

	size_t MyEnd() const {
		return my_end_;
	}

	void Add(const size_t& element) {
	    size_t hash = std::hash<size_t>()(element) % (LocalBitsetSize * NumParts);
		worker_id table = hash / LocalBitsetSize;
		size_t position = hash % LocalBitsetSize;
		bitsets_[table].set(position);
	}

	size_t BitsSet() {
		size_t bit_sum = 0;
		for (auto bitset : bitsets_) {
			bit_sum += bitset.count();
		}
		return bit_sum;
	}

private:
	
	const worker_id my_rank_;
	const size_t bitset_size_;
	const size_t my_start_;
	const size_t my_end_;
	std::array<std::bitset<LocalBitsetSize>, NumParts> bitsets_;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_DISTRIBUTED_BITSET_HEADER

/******************************************************************************/
