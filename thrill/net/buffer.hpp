/*******************************************************************************
 * thrill/net/buffer.hpp
 *
 * Contains binary byte buffer used by most network classes.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_BUFFER_HEADER
#define THRILL_NET_BUFFER_HEADER

#include <thrill/common/logger.hpp>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <string>

namespace thrill {
namespace net {

//! \addtogroup net Network Communication
//! \{

/*!
 * Simple buffer of characters without initialization or growing functionality.
 *
 * We use Buffer instead of std::string for handling untyped byte arrays. The
 * advantage of Buffer is that we have guaranteed direct byte access, and that
 * it does not initialize memory (faster). A Buffer object is also non-copyable,
 * which makes sure that we use zero-copy overhead as much as possible.
 *
 * This Buffer allows memory using malloc()/realloc()/free().
 */
class Buffer
{
public:
    //! value type stored in the buffer
    using value_type = unsigned char;
    //! size and offset type of buffer
    using size_type = size_t;

    //! simple pointer iterators
    using iterator = value_type *;
    //! simple pointer iterators
    using const_iterator = const value_type *;
    //! simple pointer references
    using reference = value_type &;
    //! simple pointer references
    using const_reference = const value_type &;

protected:
    //! protected constructor used to acquire ownership of a buffer
    Buffer(bool /* acquire_tag */, void* data, size_type size)
        : data_(reinterpret_cast<value_type*>(data)), size_(size)
    { }

public:
    //! \name Construction, Moving, Destruction
    //! \{

    //! allocate empty buffer
    Buffer()
        : data_(nullptr), size_(0)
    { }

    //! allocate buffer containing n bytes
    explicit Buffer(size_type n)
        : Buffer(true, malloc(n), n)
    { }

    //! allocate buffer and COPY data into it.
    explicit Buffer(const void* data, size_type size)
        : Buffer(true, malloc(size), size) {
        const value_type* cdata = reinterpret_cast<const value_type*>(data);
        std::copy(cdata, cdata + size, data_);
    }

    //! non-copyable: delete copy-constructor
    Buffer(const Buffer&) = delete;
    //! non-copyable: delete assignment operator
    Buffer& operator = (const Buffer&) = delete;

    //! move-construct other buffer into this one
    Buffer(Buffer&& other)
        : data_(other.data_), size_(other.size_) {
        other.data_ = nullptr;
        other.size_ = 0;
    }

    //! move-assignment of other buffer into this one
    Buffer& operator = (Buffer&& other) {
        assert(this != &other);
        if (data_) free(data_);
        data_ = other.data_;
        size_ = other.size_;
        other.data_ = nullptr;
        other.size_ = 0;
        return *this;
    }

    //! construct Buffer by acquiring ownership of a memory buffer. The memory
    //! buffer will thee FREE()ed (not delete[]-ed).
    static Buffer Acquire(void* data, size_type size)
    { return Buffer(true, data, size); }

    //! delete buffer
    ~Buffer() {
        if (data_) free(data_);
    }

    //! swap buffer with another one
    friend void swap(Buffer& a, Buffer& b) {
        using std::swap;
        swap(a.data_, b.data_);
        swap(a.size_, b.size_);
    }

    //! Check for Buffer contents is valid.
    bool IsValid() const { return (data_ != nullptr); }

    //! \}

    //! \name Data Access
    //! \{

    //! return iterator to beginning of vector
    iterator data()
    { return data_; }
    //! return iterator to beginning of vector
    const_iterator data() const
    { return data_; }

    //! return number of items in vector
    size_type size() const
    { return size_; }

    //! return the i-th position of the vector
    reference operator [] (size_type i) {
        assert(i < size_);
        return *(begin() + i);
    }
    //! return constant reference to the i-th position of the vector
    const_reference operator [] (size_type i) const {
        assert(i < size_);
        return *(begin() + i);
    }

    //! \}

    //! \name Iterator Access

    //! return mutable iterator to first element
    iterator begin()
    { return data_; }
    //! return constant iterator to first element
    const_iterator begin() const
    { return data_; }
    //! return constant iterator to first element
    const_iterator cbegin() const
    { return begin(); }

    //! return mutable iterator beyond last element
    iterator end()
    { return data_ + size_; }
    //! return constant iterator beyond last element
    const_iterator end() const
    { return data_ + size_; }
    //! return constant iterator beyond last element
    const_iterator cend() const
    { return end(); }

    //! \}

    //! \name Resize and Filling
    //! \{

    //! Zero the whole array content.
    void Zero() {
        std::fill(data_, data_ + size_, value_type(0));
    }

    //! resize the array to contain exactly new_size items. This should only be
    //! used if the Buffer was default constructed containing an empty array. It
    //! should NOT be used to resizing it, since this requires it to copy data.
    void Resize(size_type new_size) {
        if (data_)
        {
            LOG1 << "Warning: resizing non-empty simple_vector";
            data_ = reinterpret_cast<value_type*>(::realloc(data_, new_size));
            size_ = new_size;
        }
        else
        {
            data_ = reinterpret_cast<value_type*>(::malloc(new_size));
            size_ = new_size;
        }
    }

    //! \}

    //! \name Output
    //! \{

    //! copy contents into std::string
    std::string ToString() const {
        if (!data_) return std::string();
        return std::string(reinterpret_cast<const char*>(data_), size_);
    }

    //! copy part of contents into std::string
    std::string PartialToString(size_t begin, size_t length) const {
        assert(size_ >= begin + length);
        return std::string(reinterpret_cast<const char*>(data_ + begin), length);
    }

    //! make ostream-able
    friend std::ostream& operator << (std::ostream& os, const Buffer& b) {
        return os << "[Buffer size=" << b.size() << "]";
    }

    //! \}

protected:
    //! the buffer, typed as character data
    value_type* data_;

    //! size of the buffer.
    size_type size_;
};

//! \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_BUFFER_HEADER

/******************************************************************************/
