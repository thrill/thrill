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

#include <thrill/common/logger.hpp>
#include <thrill/data/serialization.hpp>

#include <algorithm>
#include <string>

namespace thrill {
namespace common {

/*!
 * FastString is a fast implementation of a string, which is basically only a
 * char pointer and a length. The FastString is defined by the char array given
 * by those parameters. A copy assignment or copy constructor actually allocates
 * memory for the data. This allows both non-allocating quick references and
 * persistent storage of strings.
 */
class FastString
{
public:
    using iterator = const char*;

    //! Default constructor for a FastString. Doesn't do anything.
    FastString() : size_(0) { }

    /*!
     * Copy constructor for a new FastString. Actually allocates memory.
     * \param input Input FastString
     * \return Copy of input
     */
    FastString(const FastString& input) {
        char* begin = new char[input.size_];
        std::copy(input.data_, input.data_ + input.size_, begin);
        data_ = begin;
        size_ = input.size_;
        owns_data_ = true;
    }

    //! Move constructor for a new FastString. Steals data ownership.
    FastString(FastString&& other) noexcept
        : FastString(other.data_, other.size_, other.owns_data_) {
        other.owns_data_ = false;
    }

    //! Destructor for a FastString. If it holds data, this data gets freed.
    ~FastString() {
        if (owns_data_) {
            delete[] (data_);
        }
    }

    /*!
     * Creates a new reference FastString, given a const char* and the size of
     * the FastString.
     *
     * \param data Pointer to start of data
     * \param size Size of data in bytes.
     * \return New FastString object.
     */
    static FastString
    Ref(const char* data, size_t size) noexcept {
        return FastString(data, size, false);
    }

    /*!
     * Creates a new reference FastString, given a const iterator to a
     * std::string and the size of the FastString.
     *
     * \param data Pointer to start of data
     * \param size Size of data in bytes.
     * \return New FastString object.
     */
    static FastString
    Ref(const std::string::const_iterator& data, size_t size) {
        return FastString(&(*data), size, false);
    }

    /*!
     * Creates a new FastString and takes data ownership, given a const char*
     * and the size of the FastString.
     *
     * \param data Pointer to start of data
     * \param size Size of data in bytes.
     * \return New FastString object.
     */
    static FastString Take(const char* data, size_t size) {
        return FastString(data, size, true);
    }

    /*!
     * Creates a new FastString and copies it's data.
     * \param data Pointer to start of data
     * \param size Size of data in bytes.
     * \return New FastString object.
     */
    static FastString Copy(const char* data, size_t size) {
        char* mem = new char[size];
        std::copy(data, data + size, mem);
        return FastString(mem, size, true);
    }

    /*!
     * Creates a new FastString and copies it's data.
     * \param input Input string, which the new FastString is a copy of
     * \return New FastString object.
     */
    static FastString Copy(const std::string& input) {
        return Copy(input.c_str(), input.size());
    }

    //! Returns a pointer to the start of the data.
    const char * Data() const {
        return data_;
    }

    //! Returns a pointer to the beginning of the data.
    iterator begin() const {
        return data_;
    }

    //! Returns a pointer beyond the end of the data.
    iterator end() const {
        return data_ + size_;
    }

    //! Returns the size of this FastString
    size_t Size() const {
        return size_;
    }

    /*!
     * Copy assignment operator.
     * \param other copied FastString
     * \return reference to this FastString
     */
    FastString& operator = (const FastString& other) {
        if (owns_data_) delete[] (data_);
        char* mem = new char[other.size_];
        std::copy(other.data_, other.data_ + other.size_, mem);
        data_ = mem;
        size_ = other.size_;
        owns_data_ = true;
        return *this;
    }

    /*!
     * Move assignment operator
     * \param other moved FastString
     * \return reference to this FastString
     */
    FastString& operator = (FastString&& other) noexcept {
        if (owns_data_) delete[] (data_);
        data_ = other.data_;
        size_ = other.Size();
        owns_data_ = other.owns_data_;
        other.owns_data_ = false;
        return *this;
    }

    /*!
     * Equality operator to compare a FastString with an std::string
     * \param other Comparison string
     * \return true, if data is equal
     */
    bool operator == (const std::string& other) const noexcept {
        return size_ == other.size() &&
               std::equal(data_, data_ + size_, other.c_str());
    }

    /*!
     * Inequality operator to compare a FastString with an std::string
     * \param other Comparison string
     * \return false, if data is equal
     */
    bool operator != (const std::string& other) const noexcept {
        return !(operator == (other));
    }

    /*!
     * Equality operator to compare a FastString with another FastString
     * \param other Comparison FastString
     * \return true, if data is equal
     */
    bool operator == (const FastString& other) const noexcept {
        return size_ == other.size_ &&
               std::equal(data_, data_ + size_, other.data_);
    }

    /*!
     * Inequality operator to compare a FastString with another FastString
     * \param other Comparison FastString
     * \return false, if data is equal
     */
    bool operator != (const FastString& other) const noexcept {
        return !(operator == (other));
    }

    /*!
     * Make FastString ostreamable
     * \param os ostream
     * \param fs FastString to stream
     */
    friend std::ostream& operator << (std::ostream& os, const FastString& fs) {
        return os.write(fs.Data(), fs.Size());
    }

    /*!
     * Returns the data of this FastString as an std::string
     * \return This FastString as an std::string
     */
    std::string ToString() const {
        return std::string(data_, size_);
    }

private:
    /*!
     * Internal constructor, which creates a new FastString and sets parameters
     * \param data Pointer to data
     * \param size Size of data in bytes
     * \param owns_data True, if this FastString has ownership of data
     */
    FastString(const char* data, size_t size, bool owns_data) noexcept
        : data_(data), size_(size), owns_data_(owns_data) { }

    //! Pointer to data
    const char* data_ = 0;
    //! Size of data
    size_t size_;
    //! True, if this FastString has ownership of data
    bool owns_data_ = false;
};

} // namespace common

namespace data {

template <typename Archive>
struct Serialization<Archive, common::FastString>
{
    static void Serialize(const common::FastString& fs, Archive& ar) {
        ar.PutVarint(fs.Size()).Append(fs.Data(), fs.Size());
    }

    static common::FastString Deserialize(Archive& ar) {
        size_t size = ar.GetVarint();
        char* outdata = new char[size];
        ar.Read(outdata, size);
        return common::FastString::Take(outdata, size);
    }

    static const bool   is_fixed_size = false;
    static const size_t fixed_size = 0;
};

} // namespace data
} // namespace thrill

namespace std {
template <>
struct hash<thrill::common::FastString>
{
    size_t operator () (const thrill::common::FastString& fs) const {
        // simple string hash taken from: http://www.cse.yorku.ca/~oz/hash.html
        size_t hash = 5381;
        for (const char* ctr = fs.begin(); ctr != fs.end(); ++ctr) {
            // hash * 33 + c
            hash = ((hash << 5) + hash) + *ctr;
        }
        return hash;
    }
};

} // namespace std

#endif // !THRILL_COMMON_FAST_STRING_HEADER

/******************************************************************************/
