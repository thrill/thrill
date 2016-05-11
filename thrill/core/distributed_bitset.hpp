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
#include <thrill/core/dynamic_bitset.hpp>

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

    DynamicBitset<size_t> Golombify(const size_t& table_id) {

		size_t m = BitsSet();
	    double fpr_parameter = (double) bitset_size_ / (double) m;
		size_t b = (size_t)(std::log(2) * fpr_parameter);
		size_t upper_space_bound = m * (2 + std::log2(fpr_parameter));

		LOG1 << upper_space_bound << " " << m << " test " << b;
		
		DynamicBitset<size_t> golomb_code(upper_space_bound, false, b);

		size_t num_hashes = 0;

		golomb_code.clear();
		golomb_code.seek(0);
		size_t delta = 0;
		for (size_t i = 0; i < bitsets_[table_id].size(); ++i) {
			if (bitsets_[table_id][i] == 1) {
				golomb_code.golomb_in(i - delta);
				delta = i;
				num_hashes++;
			}
		}

		assert(num_hashes == m);

		return golomb_code;
	}

	std::bitset<LocalBitsetSize> Degolombify(DynamicBitset<size_t>& golomb_code) {
		size_t num_hashes = BitsSet();
		golomb_code.seek(0);
		std::bitset<LocalBitsetSize> to_return;
		size_t bit_used = 0;
		for (size_t i = 0; i < num_hashes; ++i) {
			bit_used += golomb_code.golomb_out();
			to_return.set(bit_used);
		}
		return to_return;
	}

	std::bitset<LocalBitsetSize> Get(const size_t& table_id) {
		return bitsets_[table_id];
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
