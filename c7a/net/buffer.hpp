/*******************************************************************************
 * c7a/net/buffer.hpp
 *
 * Contains binary byte buffer used by most network classes.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_BUFFER_HEADER
#define C7A_NET_BUFFER_HEADER

#include <c7a/common/logger.hpp>

#include <algorithm>
#include <cassert>
#include <cstddef>

namespace c7a {
namespace net {

/*!
 * Simple buffer of characters without initialization or growing functionality.
 *
 * We use Buffer instead of std::string for handling untyped byte arrays. The
 * advantage of Buffer is that we have guaranteed direct byte access, and that
 * it does not initialize memory (faster). A Buffer object is also non-copyable,
 * which makes sure that we use zero-copy overhead as much as possible.
 */
class Buffer
{
public:
    //! value type stored in the buffer
    typedef unsigned char value_type;
    //! size and offset type of buffer
    typedef size_t size_type;

    //! simple pointer iterators
    typedef value_type* iterator;
    //! simple pointer iterators
    typedef const value_type* const_iterator;
    //! simple pointer references
    typedef value_type& reference;
    //! simple pointer references
    typedef const value_type& const_reference;

public:
    //! \name Construction, Moving, Destruction
    //! \{

    //! allocate empty buffer
    Buffer()
        : data_(nullptr), size_(0)
    { }

    //! allocate buffer containing n bytes
    explicit Buffer(size_type n)
        : data_(new value_type[n]), size_(n)
    { }

    //! non-copyable: delete copy-constructor
    Buffer(const Buffer&) = delete;
    //! non-copyable: delete assignment operator
    Buffer& operator = (const Buffer&) = delete;

    //! move-construct other buffer into this one
    Buffer(Buffer&& other)
        : data_(other.data_), size_(other.size_)
    {
        other.data_ = nullptr;
        other.size_ = 0;
    }

    //! move-assignment of other buffer into this one
    Buffer& operator = (Buffer&& other)
    {
        if (this != &other)
        {
            if (data_) delete[] data_;
            data_ = other.data_;
            size_ = other.size_;
            other.data_ = nullptr;
            other.size_ = 0;
        }
        return *this;
    }

    //! delete buffer
    ~Buffer()
    {
        delete[] data_;
    }

    //! swap buffer with another one
    friend void swap(Buffer& a, Buffer& b)
    {
        using std::swap;
        swap(a.data_, b.data_);
        swap(a.size_, b.size_);
    }

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
    reference operator [] (size_type i)
    {
        assert(i < size_);
        return *(begin() + i);
    }
    //! return constant reference to the i-th position of the vector
    const_reference operator [] (size_type i) const
    {
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
    void zero()
    {
        std::fill(data_, data_ + size_, value_type(0));
    }

    //! resize the array to contain exactly new_size items. This should only be
    //! used if the Buffer was default constructed containing an empty array. It
    //! should NOT be used to resizing it, since this requires it to copy data.
    void resize(size_type new_size)
    {
        if (data_)
        {
            LOG1 << "Warning: resizing non-empty simple_vector";
            value_type* old = data_;
            data_ = new value_type[new_size];
            std::copy(old, old + std::min(size_, new_size), data_);
            delete[] old;
            size_ = new_size;
        }
        else
        {
            data_ = new value_type[new_size];
            size_ = new_size;
        }
    }

    //! \}

    //! \name Output
    //! \{

    friend std::ostream& operator << (std::ostream& os, const Buffer& b)
    {
        return os << "[Buffer size=" << b.size() << "]";
    }

    //! \}

protected:
    //! the buffer, typed as character data
    value_type* data_;

    //! size of the buffer.
    size_type size_;
};

} // namespace net
} // namespace c7a

#endif // !C7A_NET_BUFFER_HEADER

/******************************************************************************/
