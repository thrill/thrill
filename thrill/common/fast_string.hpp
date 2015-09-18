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
#include <thrill/data/serialization.hpp>

namespace thrill {

namespace common {

class FastString
{
public:

	FastString() : size_(0) { };
	
	FastString(const FastString& in_str) {
		// REVIEW(an): maybe use _new char []..._ / delete -> less casts
		char* begin = new char[in_str.size_];
		// REVIEW(an): I am a big friend of std::copy instead of memcpy.
		std::copy(in_str.start_, in_str.start_ + in_str.size_, begin);
		start_ = begin;
		size_ = in_str.size_;
		has_data_ = true;
	};

    // REVIEW(an): use ": field_()" _whenever_ possible!
	FastString(FastString&& other) :
		FastString(other.start_, other.size_, other.has_data_) {
		other.has_data_ = false;
	};

	~FastString() {
		if (has_data_) delete[](start_);
	};

	static FastString Ref(const char* start, size_t size) {
		return FastString(start, size, false);
	}

	static FastString Take(const char* start, size_t size) {
		return FastString(start, size, true);
	}

	static FastString Copy(const char* start, size_t size) {
		char* mem = new char[size];
		std::copy(start, start + size, mem);
		return FastString(mem, size, true);
	}

	static FastString Copy(const std::string& in_str) {
		return Copy(in_str.c_str(), in_str.size());
	}
	
	// REVIEW(an): rename to Data(), just like std::string.
	const char* Start() const {
		return start_;
	}

	size_t Size() const {
		return size_;
	}

	FastString& operator = (const FastString& other) {
		if (has_data_) delete[] (start_);
		char* mem = new char[other.size_];
		std::copy(other.start_, other.start_ + other.size_, mem);
		start_ = mem;
		size_ = other.size_;
		has_data_ = true;
		return *this;
	}

	FastString& operator = (FastString&& other) {
		if (has_data_) delete[] (start_);
		start_ = std::move(other.start_);
		size_ = other.Size();
		has_data_ = other.has_data_;
		other.has_data_ = false;
		return *this;
	}

        bool operator == (std::string other) const {
            // REVIEW(an): use std::equal()!
		return size_ == other.size() &&
			std::strncmp(start_, other.c_str(), size_) == 0;
	}

	bool operator != (std::string other) const {
		return !(operator == (other));
	}

	bool operator == (const FastString& other) const {
		return size_ == other.Size() &&
			std::strncmp(start_, other.start_, size_) == 0;
	}

	bool operator != (const FastString& other) const {
		return !(operator == (other));
	}

	friend std::ostream& operator << (std::ostream& os, const FastString& fs) {
		return os.write(fs.Start(), fs.Size()); 
	}

	std::string ToString() const {
		return std::string(start_, size_);
	}


protected:
	FastString(const char* start, size_t size, bool copy) :
		start_(start), size_(size), has_data_(copy) { };

    // REVIEW(an): rename to data_.
	const char* start_ = 0;

	size_t size_;

	bool has_data_ = false;

};

} // namespace common

namespace data {

template<typename Archive>
struct Serialization<Archive, common::FastString> 
{
	static void Serialize(const common::FastString& fs, Archive& ar) {
		ar.PutVarint(fs.Size()).Append(fs.Start(), fs.Size());
	}

	static common::FastString Deserialize(Archive& ar) {
		uint64_t size = ar.GetVarint();
		void* outdata = ::malloc(size);
		ar.Read(outdata, size);
        return common::FastString::Take(reinterpret_cast<const char*>(outdata), size);
    }

};

} //namespace data
} // namespace thrill

namespace std { //I am very sorry.
	template <>
	struct hash<thrill::common::FastString>
	{
		size_t operator ()(const thrill::common::FastString& fs) const {
			unsigned int hash = 0xDEADC0DE;
			for (size_t ctr = 0; ctr < fs.Size(); ctr++) {
				hash = ((hash << 5) + hash) + *(fs.Start() + ctr); /* hash * 33 + c */
			}
			return hash;
		}
	};
}

#endif // !THRILL_COMMON_FAST_STRING_HEADER

/******************************************************************************/
