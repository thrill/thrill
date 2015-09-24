/*******************************************************************************
 * thrill/net/fixed_buffer_builder.hpp
 *
 * FixedBufferBuilder is like BufferBuilder except that constructs data blocks
 * with FIXED length content.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_FIXED_BUFFER_BUILDER_HEADER
#define THRILL_NET_FIXED_BUFFER_BUILDER_HEADER

#include <thrill/common/item_serialization_tools.hpp>

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdlib>
#include <stdexcept>
#include <string>

namespace thrill {
namespace net {

//! \addtogroup net Network Communication
//! \{

/*!
 * Represents a FIXED length area of memory, which can be modified by appending
 * integral data types via Put() and other basic operations.
 */
template <size_t Capacity>
class FixedBufferBuilder
    : public common::ItemWriterToolsBase<FixedBufferBuilder<Capacity> >
{
private:
    //! type used to store the bytes
    using Byte = unsigned char;

    //! simple pointer iterators
    using iterator = Byte *;
    //! simple pointer iterators
    using const_iterator = const Byte *;
    //! simple pointer references
    using reference = Byte &;
    //! simple pointer references
    using const_reference = const Byte &;

    //! Allocated buffer.
    std::array<Byte, Capacity> data_;

    //! Size of _valid_ data.
    size_t size_ = 0;

public:
    //! \name Data, Size, and Capacity Accessors
    //! \{

    //! Return a pointer to the currently kept memory area.
    const Byte * data() const {
        return data_.data();
    }

    //! Return a writeable pointer to the currently kept memory area.
    Byte * data() {
        return data_.data();
    }

    //! Return the currently used length in bytes.
    size_t size() const {
        return size_;
    }

    //! Return the currently allocated buffer capacity.
    size_t capacity() const {
        return Capacity;
    }

    //! \} //do not append empty end-of-stream buffer

    //! \name Buffer Growing, Clearing, and other Management
    //! \{

    //! Clears the memory contents, does not deallocate the memory.
    FixedBufferBuilder & Clear() {
        size_ = 0;
        return *this;
    }

    //! Set the valid bytes in the buffer, use if the buffer is filled
    //! directly.
    FixedBufferBuilder & set_size(size_t n) {
        assert(n <= Capacity);
        size_ = n;

        return *this;
    }

    //! Explicit conversion to std::string (copies memory of course).
    std::string ToString() const {
        return std::string(reinterpret_cast<const char*>(data_.data()), size_);
    }

    //! \}

    //! \name Appending Write Functions
    //! \{

    //! Append a memory range to the buffer
    FixedBufferBuilder & Append(const void* data, size_t len) {
        assert(size_ + len <= Capacity);

        const Byte* cdata = reinterpret_cast<const Byte*>(data);
        std::copy(cdata, cdata + len, data_.data() + size_);
        size_ += len;

        return *this;
    }

    //! Append to contents of a std::string, excluding the null (which isn't
    //! contained in the string size anyway).
    FixedBufferBuilder & AppendString(const std::string& s) {
        return Append(s.data(), s.size());
    }

    //! Put (append) a single item of the template type T to the buffer. Be
    //! careful with implicit type conversions!
    template <typename Type>
    FixedBufferBuilder & Put(const Type item) {
        static_assert(std::is_pod<Type>::value,
                      "You only want to Put() POD types as raw values.");

        assert(size_ + sizeof(Type) <= Capacity);

        *reinterpret_cast<Type*>(data_.data() + size_) = item;
        size_ += sizeof(Type);

        return *this;
    }

    //! Put a single byte to the buffer (used via CRTP from ItemWriterToolsBase)
    FixedBufferBuilder & PutByte(Byte data) {
        return Put<uint8_t>(data);
    }

    //! \}

    //! \name Access
    //! \{

    //! return mutable iterator to first element
    iterator begin()
    { return data_.data(); }
    //! return constant iterator to first element
    const_iterator begin() const
    { return data_.data(); }

    //! return mutable iterator beyond last element
    iterator end()
    { return data_.data() + size_; }
    //! return constant iterator beyond last element
    const_iterator end() const
    { return data_.data() + size_; }

    //! return the i-th position of the vector
    reference operator [] (size_t i) {
        assert(i < size_);
        return *(begin() + i);
    }

    //! \}
};

//! \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_FIXED_BUFFER_BUILDER_HEADER

/******************************************************************************/
