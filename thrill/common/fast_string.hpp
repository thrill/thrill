/*******************************************************************************
 * thrill/common/fast_string.hpp
 *
 * (Hopefully) fast static-length string implementation.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
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
		char* begin = new char[in_str.size_];
		std::copy(in_str.data_, in_str.data_ + in_str.size_, begin);
		data_ = begin;
		size_ = in_str.size_;
		has_data_ = true;
	};

	FastString(FastString&& other) :
		FastString(other.data_, other.size_, other.has_data_) {
		other.has_data_ = false;
	};

	~FastString() {
		if (has_data_) {
			delete[](data_);
		}
	};

	static FastString Ref(const char* data, size_t size) {
		return FastString(data, size, false);
	}

	static FastString Ref(std::string::const_iterator data, size_t size) {
		return FastString(&(*data), size, false);
	}

	static FastString Take(const char* data, size_t size) {
		return FastString(data, size, true);
	}

	static FastString Copy(const char* data, size_t size) {
		char* mem = new char[size];
		std::copy(data, data + size, mem);
		return FastString(mem, size, true);
	}

	static FastString Copy(const std::string& in_str) {
		return Copy(in_str.c_str(), in_str.size());
	}
	
	const char* Data() const {
		return data_;
	}

	size_t Size() const {
		return size_;
	}

	FastString& operator = (const FastString& other) {
		if (has_data_) delete[] (data_);
		char* mem = new char[other.size_];
		std::copy(other.data_, other.data_ + other.size_, mem);
		data_ = mem;
		size_ = other.size_;
		has_data_ = true;
		return *this;
	}

	FastString& operator = (FastString&& other) {
		if (has_data_) delete[] (data_);
		data_ = std::move(other.data_);
		size_ = other.Size();
		has_data_ = other.has_data_;
		other.has_data_ = false;
		return *this;
	}

	bool operator == (std::string other) const {
            // REVIEW(an): use std::equal()!
		return std::equal(data_, data_ + size_, other.c_str(), other.c_str() + other.size());
	}

	bool operator != (std::string other) const {
		return !(operator == (other));
	}

	bool operator == (const FastString& other) const {
		return std::equal(data_, data_ + size_, other.data_, other.data_ + other.size_); 
	}

	bool operator != (const FastString& other) const {
		return !(operator == (other));
	}

	friend std::ostream& operator << (std::ostream& os, const FastString& fs) {
		return os.write(fs.Data(), fs.Size()); 
	}

	std::string ToString() const {
		return std::string(data_, size_);
	}


protected:
	FastString(const char* data, size_t size, bool copy) :
		data_(data), size_(size), has_data_(copy) { };

	const char* data_ = 0;

	size_t size_;

	bool has_data_ = false;

};

} // namespace common

namespace data {

template<typename Archive>
struct Serialization<Archive, common::FastString> 
{
	static void Serialize(const common::FastString& fs, Archive& ar) {
		ar.PutVarint(fs.Size()).Append(fs.Data(), fs.Size());
	}

	static common::FastString Deserialize(Archive& ar) {
		uint64_t size = ar.GetVarint();
		char* outdata = new char[size];
		ar.Read(outdata, size);
        return common::FastString::Take(outdata, size);
    }
	
    static const bool   is_fixed_size = false;
    static const size_t fixed_size = 0;

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
				hash = ((hash << 5) + hash) + *(fs.Data() + ctr); /* hash * 33 + c */
			}
			return hash;
		}
	};
}

#endif // !THRILL_COMMON_FAST_STRING_HEADER

/******************************************************************************/
