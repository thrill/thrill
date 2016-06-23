/*******************************************************************************
 * thrill/core/golomb_reader.hpp
 *
 * Reader which reads from a golomb encoded dynamic_bitset
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_GOLOMB_READER_HEADER
#define THRILL_CORE_GOLOMB_READER_HEADER

#include <thrill/core/dynamic_bitset.hpp>

namespace thrill {
namespace core {

template <typename CounterType>
class GolombPairReader 
{
public:
	GolombPairReader(size_t data_size,
				 size_t* raw_data,
				 size_t num_elements,
				 size_t b,
				 size_t bitsize):
		golomb_code(raw_data,
					common::IntegerDivRoundUp(data_size,
											  sizeof(size_t)),
					b, num_elements),
		num_elements_(num_elements),
		returned_elements_(0),
		delta_(0),
		bitsize_(bitsize) { }

	bool HasNext() {
		return returned_elements_ < num_elements_;
	}

	template <typename T> 
	T Next() {
		size_t new_element = golomb_code.golomb_out() + delta_;
		delta_ = new_element;
		CounterType ctr = golomb_code.stream_out(bitsize_);
		returned_elements_++;
		return std::make_pair(new_element, ctr);
	}

private:
    DynamicBitset<size_t> golomb_code;
	size_t num_elements_;
	size_t returned_elements_;
	size_t delta_;
	size_t bitsize_;
};

class GolombReader 
{
public:
	GolombReader(size_t data_size,
				 size_t* raw_data,
				 size_t num_elements,
				 size_t b):
		golomb_code(raw_data,
					common::IntegerDivRoundUp(data_size,
											  sizeof(size_t)),
					b, num_elements),
		num_elements_(num_elements),
		returned_elements_(0),
		delta_(0) { }

	bool HasNext() {
		return returned_elements_ < num_elements_;
	}

	template <typename T> 
	T Next() {
		size_t new_element = golomb_code.golomb_out() + delta_;
		delta_ = new_element;
		returned_elements_++;
		return new_element;
	}

private:
    DynamicBitset<size_t> golomb_code;
	size_t num_elements_;
	size_t returned_elements_;
	size_t delta_;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_GOLOMB_READER_HEADER

/******************************************************************************/
