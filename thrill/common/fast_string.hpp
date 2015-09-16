/*******************************************************************************
 * thrill/common/fast_string.hpp
 *
 * (Hopefully) fast static-length string implementation.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_FAST_STRING_HEADER
#define THRILL_COMMON_FAST_STRING_HEADER

#include <cstdlib>
#include <cstring>

#include <thrill/common/logger.hpp>

namespace thrill {
namespace common {

class FastString
{
public:

	FastString() : size_(0) { };

	FastString(const FastString& in_str) 
		: FastString(in_str.Start(), in_str.Size(), true) { }

	FastString(FastString&& other) {
		start_ = std::move(other.start_);
		size_ = other.Size();
	};

	~FastString() {
		if (has_data_) free(start_);
	};

	static FastString Ref(char* start, size_t size) {
		return FastString(start, size, false);
	}

	static FastString Copy(char* start, size_t size) {
		return FastString(start, size, true);
	}

	char* Start() const {
		return start_;
	}

	size_t Size() const {
		return size_;
	}

	FastString& operator = (const FastString& other) {
		start_ = other.Start();
		size_ = other.Size();
		return *this;
	}

	FastString& operator = (FastString&& other) {
		start_ = std::move(other.start_);
		size_ = other.Size();
		return *this;
	}

	bool operator == (std::string other) const {
		return size_ == other.size() &&
			std::strncmp(start_, other.c_str(), size_) == 0;
	}

	bool operator == (const FastString& other) const {
		return size_ == other.Size() &&
			std::strncmp(start_, other.start_, size_) == 0;
	}


protected:
	FastString(char* start, size_t size, bool copy)
		: start_(start), size_(size) {
		if (copy) {
			start_ = reinterpret_cast<char*>(::malloc(size));
			std::memcpy(start_, start, size);
		}
	};

	bool has_data_ = false;

	char* start_;

	size_t size_;

};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_FAST_STRING_HEADER

/******************************************************************************/
