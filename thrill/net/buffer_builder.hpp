/*******************************************************************************
 * thrill/net/buffer_builder.hpp
 *
 * Classes BufferBuilder and BinaryBufferReader to construct data blocks with variable
 * length content. Programs construct blocks using BufferBuilder::Put<type>()
 * and read them using BufferReader::Get<type>(). The operation sequences must
 * match. See test-binary-builder.cpp for an example.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_BUFFER_BUILDER_HEADER
#define THRILL_NET_BUFFER_BUILDER_HEADER

#include <thrill/common/item_serialization_tools.hpp>
#include <thrill/net/buffer.hpp>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <stdexcept>
#include <string>

namespace thrill {
namespace net {

//! \addtogroup net Network Communication
//! \{

/*!
 * BufferBuilder represents a dynamically growable area of memory, which
 * can be modified by appending integral data types via Put() and other basic
 * operations.
 */
class BufferBuilder
    : public common::ItemWriterToolsBase<BufferBuilder>
{
protected:
    //! type used to store the bytes
    using Byte = unsigned char;

    //! Allocated buffer pointer.
    Byte* data_;

    //! Size of valid data.
    size_t size_;

    //! Total capacity of buffer.
    size_t capacity_;

public:
    //! \name Construction, Movement, Destruction
    //! \{

    //! Create a new empty object
    BufferBuilder()
        : data_(nullptr), size_(0), capacity_(0)
    { }

    //! Copy-Constructor, duplicates memory content.
    BufferBuilder(const BufferBuilder& other)
        : data_(nullptr), size_(0), capacity_(0) {
        Assign(other);
    }

    //! Move-Constructor, moves memory area.
    BufferBuilder(BufferBuilder&& other)
        : data_(other.data_), size_(other.size_), capacity_(other.capacity_) {
        other.data_ = nullptr;
        other.size_ = 0;
        other.capacity_ = 0;
    }

    //! Constructor, copy memory area.
    BufferBuilder(const void* data, size_t n)
        : data_(nullptr), size_(0), capacity_(0) {
        Assign(data, n);
    }

    //! Constructor, create object with n bytes pre-allocated.
    explicit BufferBuilder(size_t n)
        : data_(nullptr), size_(0), capacity_(0) {
        Reserve(n);
    }

    //! Constructor from std::string, COPIES string content.
    explicit BufferBuilder(const std::string& str)
        : data_(nullptr), size_(0), capacity_(0) {
        Assign(str.data(), str.size());
    }

    //! Assignment operator: copy other's memory range into buffer.
    BufferBuilder& operator = (const BufferBuilder& other) {
        if (&other != this)
            Assign(other.data(), other.size());

        return *this;
    }

    //! Move-Assignment operator: move other's memory area into buffer.
    BufferBuilder& operator = (BufferBuilder&& other) {
        if (this != &other)
        {
            if (data_) free(data_);
            data_ = other.data_;
            size_ = other.size_;
            capacity_ = other.capacity_;
            other.data_ = nullptr;
            other.size_ = 0;
            other.capacity_ = 0;
        }
        return *this;
    }

    //! Destroys the memory space.
    ~BufferBuilder() {
        Deallocate();
    }

    //! Deallocates the kept memory space (we use dealloc() instead of free()
    //! as a name, because sometimes "free" is replaced by the preprocessor)
    BufferBuilder & Deallocate() {
        if (data_) free(data_);
        data_ = nullptr;
        size_ = capacity_ = 0;

        return *this;
    }

    //! \}

    //! \name Data, Size, and Capacity Accessors
    //! \{

    //! Return a pointer to the currently kept memory area.
    const Byte * data() const {
        return data_;
    }

    //! Return a writeable pointer to the currently kept memory area.
    Byte * data() {
        return data_;
    }

    //! Return the currently used length in bytes.
    size_t size() const {
        return size_;
    }

    //! Return the currently allocated buffer capacity.
    size_t capacity() const {
        return capacity_;
    }

    //! \} //do not append empty end-of-stream buffer

    //! \name Buffer Growing, Clearing, and other Management
    //! \{

    //! Clears the memory contents, does not deallocate the memory.
    BufferBuilder & Clear() {
        size_ = 0;
        return *this;
    }

    //! Set the valid bytes in the buffer, use if the buffer is filled
    //! directly.
    BufferBuilder & set_size(size_t n) {
        assert(n <= capacity_);
        size_ = n;

        return *this;
    }

    //! Make sure that at least n bytes are allocated.
    BufferBuilder & Reserve(size_t n) {
        if (capacity_ < n)
        {
            capacity_ = n;
            data_ = static_cast<Byte*>(realloc(data_, capacity_));
        }

        return *this;
    }

    //! Dynamically allocate more memory. At least n bytes will be available,
    //! probably more to compensate future growth.
    BufferBuilder & DynReserve(size_t n) {
        if (capacity_ < n)
        {
            // place to adapt the buffer growing algorithm as need.
            size_t newsize = capacity_;

            while (newsize < n) {
                if (newsize < 256) newsize = 512;
                else if (newsize < 1024 * 1024) newsize = 2 * newsize;
                else newsize += 1024 * 1024;
            }

            Reserve(newsize);
        }

        return *this;
    }

    //! Detach the memory from the object, returns the memory pointer.
    const Byte * Detach() {
        const Byte* data = data_;
        data_ = nullptr;
        size_ = capacity_ = 0;
        return data;
    }

    //! Explicit conversion to std::string (copies memory of course).
    std::string ToString() const {
        return std::string(reinterpret_cast<const char*>(data_), size_);
    }

    //! Explicit conversion to Buffer MOVING the memory ownership.
    net::Buffer ToBuffer() {
        net::Buffer b = net::Buffer::Acquire(data_, size_);
        Detach();
        return std::move(b);
    }

    //! \}

    //! \name Assignment or Alignment
    //! \{

    //! Copy a memory range into the buffer, overwrites all current
    //! data. Roughly equivalent to clear() followed by append().
    BufferBuilder & Assign(const void* data, size_t len) {
        if (len > capacity_) Reserve(len);

        const Byte* cdata = reinterpret_cast<const Byte*>(data);
        std::copy(cdata, cdata + len, data_);
        size_ = len;

        return *this;
    }

    //! Copy the contents of another buffer object into this buffer, overwrites
    //! all current data. Roughly equivalent to clear() followed by append().
    BufferBuilder & Assign(const BufferBuilder& other) {
        if (&other != this)
            Assign(other.data(), other.size());

        return *this;
    }

    //! Align the size of the buffer to a multiple of n. Fills up with 0s.
    BufferBuilder & Align(size_t n) {
        assert(n > 0);
        size_t rem = size_ % n;
        if (rem != 0)
        {
            size_t add = n - rem;
            if (size_ + add > capacity_) DynReserve(size_ + add);
            std::fill(data_ + size_, data_ + size_ + add, 0);
            size_ += add;
        }
        assert((size_ % n) == 0);

        return *this;
    }

    //! \}

    //! \name Appending Write Functions
    //! \{

    //! Append a memory range to the buffer
    BufferBuilder & Append(const void* data, size_t len) {
        if (size_ + len > capacity_) DynReserve(size_ + len);

        const Byte* cdata = reinterpret_cast<const Byte*>(data);
        std::copy(cdata, cdata + len, data_ + size_);
        size_ += len;

        return *this;
    }

    //! Append the contents of a different buffer object to this one.
    BufferBuilder & Append(const class BufferBuilder& bb) {
        return Append(bb.data(), bb.size());
    }

    //! Append to contents of a std::string, excluding the null (which isn't
    //! contained in the string size anyway).
    BufferBuilder & AppendString(const std::string& s) {
        return Append(s.data(), s.size());
    }

    //! Put (append) a single item of the template type T to the buffer. Be
    //! careful with implicit type conversions!
    template <typename Type>
    BufferBuilder & Put(const Type item) {
        static_assert(std::is_pod<Type>::value,
                      "You only want to Put() POD types as raw values.");

        if (size_ + sizeof(Type) > capacity_) DynReserve(size_ + sizeof(Type));

        *reinterpret_cast<Type*>(data_ + size_) = item;
        size_ += sizeof(Type);

        return *this;
    }

    //! Put a single byte to the buffer (used via CRTP from ItemWriterToolsBase)
    BufferBuilder & PutByte(Byte data) {
        return Put<uint8_t>(data);
    }

    //! \}
};

//! \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_BUFFER_BUILDER_HEADER

/******************************************************************************/
