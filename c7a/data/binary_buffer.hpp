/*******************************************************************************
 * c7a/data/binary_buffer.hpp
 *
 * Look at the Doxygen below....
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_BINARY_BUFFER_HEADER
#define C7A_DATA_BINARY_BUFFER_HEADER

#include <c7a/net/buffer.hpp>
#include <c7a/data/binary_buffer_builder.hpp>

namespace c7a {
namespace data {

/*!
 * BinaryBuffer represents a memory area as pointer and valid length. It
 * is not deallocated or otherwise managed. This class can be used to pass
 * around references to BinaryBufferBuilder objects.
 */
class BinaryBuffer
{
protected:
    //! type used to store the bytes
    typedef unsigned char Byte;

    //! Allocated buffer pointer.
    const Byte* data_;

    //! Size of valid data.
    size_t size_;

public:
    //! Constructor, assign memory area from BinaryBufferBuilder.
    explicit BinaryBuffer(const BinaryBufferBuilder& bb)
        : data_(bb.data()), size_(bb.size())
    { }

    //! Constructor, assign memory area from pointer and length.
    BinaryBuffer(const void* data, size_t n)
        : data_(reinterpret_cast<const Byte*>(data)), size_(n)
    { }

    //! Constructor, assign memory area from string, does NOT copy.
    explicit BinaryBuffer(const std::string& str)
        : data_(reinterpret_cast<const Byte*>(str.data())),
          size_(str.size())
    { }

    //! Return a pointer to the currently kept memory area.
    const void * data() const
    { return data_; }

    //! Return the currently valid length in bytes.
    size_t size() const
    { return size_; }

    //! Explicit conversion to std::string (copies memory of course).
    std::string ToString() const
    { return std::string(reinterpret_cast<const char*>(data_), size_); }

    //! Explicit conversion to Buffer MOVING the memory ownership.
    net::Buffer ToBuffer() {
        void* addr = (void*)data_;
        net::Buffer b = net::Buffer::Acquire(addr, size_);
        data_ = nullptr;
        size_ = 0;
        return std::move(b);
    }

    //! Compare contents of two BinaryBuffers.
    bool operator == (const BinaryBuffer& br) const {
        if (size_ != br.size_) return false;
        return std::equal(data_, data_ + size_, br.data_);
    }

    //! Compare contents of two BinaryBuffers.
    bool operator != (const BinaryBuffer& br) const {
        if (size_ != br.size_) return true;
        return !std::equal(data_, data_ + size_, br.data_);
    }

    void Delete() {
        if (data_)
            free((void*)data_);
    }
};

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BINARY_BUFFER_HEADER

/******************************************************************************/
