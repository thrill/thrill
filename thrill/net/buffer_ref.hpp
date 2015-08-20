/*******************************************************************************
 * thrill/net/buffer_ref.hpp
 *
 * Look at the Doxygen below....
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_BUFFER_REF_HEADER
#define THRILL_NET_BUFFER_REF_HEADER

#include <thrill/net/buffer.hpp>
#include <thrill/net/buffer_builder.hpp>

#include <string>

namespace thrill {
namespace net {

//! \addtogroup net Network Communication
//! \{

/*!
 * BufferRef represents a reference to a memory area as pointer and valid
 * length. It is not deallocated or otherwise managed. This class can be used to
 * pass around references to BufferBuilder and BufferReader objects.
 */
class BufferRef
{
protected:
    //! type used to store the bytes
    using Byte = unsigned char;

    //! Allocated buffer pointer.
    const Byte* data_;

    //! Size of valid data.
    size_t size_;

public:
    //! Constructor, assign memory area from BufferBuilder.
    explicit BufferRef(const BufferBuilder& bb)
        : data_(bb.data()), size_(bb.size())
    { }

    //! Constructor, assign memory area from pointer and length.
    BufferRef(const void* data, size_t n)
        : data_(reinterpret_cast<const Byte*>(data)), size_(n)
    { }

    //! Constructor, assign memory area from string, does NOT copy.
    explicit BufferRef(const std::string& str)
        : data_(reinterpret_cast<const Byte*>(str.data())),
          size_(str.size())
    { }

    //! Constructor, assign memory area from net::Buffer, does NOT copy!
    BufferRef(const net::Buffer& b) // NOLINT
        : data_(b.data()), size_(b.size())
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
        void* addr = reinterpret_cast<void*>(const_cast<Byte*>(data_));
        net::Buffer b = net::Buffer::Acquire(addr, size_);
        data_ = nullptr;
        size_ = 0;
        return std::move(b);
    }

    //! Compare contents of two BufferRefs.
    bool operator == (const BufferRef& br) const {
        if (size_ != br.size_) return false;
        return std::equal(data_, data_ + size_, br.data_);
    }

    //! Compare contents of two BufferRefs.
    bool operator != (const BufferRef& br) const {
        if (size_ != br.size_) return true;
        return !std::equal(data_, data_ + size_, br.data_);
    }

    void Delete() {
        if (data_)
            free(reinterpret_cast<void*>(const_cast<Byte*>(data_)));
    }
};

//! \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_BUFFER_REF_HEADER

/******************************************************************************/
