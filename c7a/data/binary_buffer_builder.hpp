/*******************************************************************************
 * c7a/data/binary_buffer_builder.hpp
 *
 * Classes BufferBuilder and BinaryBufferReader to construct data blocks with variable
 * length content. Programs construct blocks using BufferBuilder::Put<type>()
 * and read them using BufferReader::Get<type>(). The operation sequences must
 * match. See test-binary-builder.cpp for an example.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_BINARY_BUFFER_BUILDER_HEADER
#define C7A_DATA_BINARY_BUFFER_BUILDER_HEADER

#include <c7a/net/buffer.hpp>

#include <cassert>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <algorithm>

namespace c7a {
namespace data {

/*!
 * BinaryBufferBuilder represents a dynamically growable area of memory, which can be
 * modified by appending integral data types via Put() and other basic
 * operations.
 */
class BinaryBufferBuilder
{
protected:
    //! type used to store the bytes
    typedef unsigned char Byte;

    //! Allocated buffer pointer.
    Byte* data_;

    //! Size of valid data.
    size_t size_;

    //! Total capacity of buffer.
    size_t capacity_;

    //! Number of elements
    size_t num_elements_;

public:
    //! \name Construction, Movement, Destruction
    //! \{

    //! Create a new empty object
    BinaryBufferBuilder()
        : data_(nullptr), size_(0), capacity_(0), num_elements_(0)
    { }

    //! Copy-Constructor, duplicates memory content.
    BinaryBufferBuilder(const BinaryBufferBuilder& other)
        : data_(nullptr), size_(0), capacity_(0), num_elements_(0) {
        Assign(other);
    }

    //! Move-Constructor, moves memory area.
    BinaryBufferBuilder(BinaryBufferBuilder&& other)
        : data_(other.data_), size_(other.size_), capacity_(other.capacity_), num_elements_(other.num_elements_) {
        other.data_ = nullptr;
        other.size_ = 0;
        other.capacity_ = 0;
        other.num_elements_ = 0;
    }

    //! Constructor, copy memory area.
    BinaryBufferBuilder(const void* data, size_t n, size_t elements = 0)
        : data_(nullptr), size_(0), capacity_(0) {
        Assign(data, n, elements);
    }

    //! Constructor, create object with n bytes pre-allocated.
    explicit BinaryBufferBuilder(size_t n)
        : data_(nullptr), size_(0), capacity_(0), num_elements_(0) {
        Reserve(n);
    }

    //! Constructor from std::string, COPIES string content.
    explicit BinaryBufferBuilder(const std::string& str, const size_t elements = 0)
        : data_(nullptr), size_(0), capacity_(0) {
        Assign(str.data(), str.size(), elements);
    }

    //! Assignment operator: copy other's memory range into buffer.
    BinaryBufferBuilder& operator = (const BinaryBufferBuilder& other) {
        if (&other != this)
            Assign(other.data(), other.size(), other.num_elements_);

        return *this;
    }

    //! Move-Assignment operator: move other's memory area into buffer.
    BinaryBufferBuilder& operator = (BinaryBufferBuilder&& other) {
        if (this != &other)
        {
            if (data_) free(data_);
            data_ = other.data_;
            size_ = other.size_;
            capacity_ = other.capacity_;
            num_elements_ = other.num_elements_;
            other.data_ = nullptr;
            other.size_ = 0;
            other.capacity_ = 0;
            other.num_elements_ = 0;
        }
        return *this;
    }

    //! Destroys the memory space.
    ~BinaryBufferBuilder() {
        Deallocate();
    }

    //! Deallocates the kept memory space (we use dealloc() instead of free()
    //! as a name, because sometimes "free" is replaced by the preprocessor)
    BinaryBufferBuilder & Deallocate() {
        if (data_) free(data_);
        data_ = nullptr;
        size_ = capacity_ = num_elements_ = 0;

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

    //! Returns the currently held number of elements.
    size_t elements() const {
        return num_elements_;
    }

    //! Return the currently allocated buffer capacity.
    size_t capacity() const {
        return capacity_;
    }

    //! \}

    //! \name Buffer Growing, Clearing, and other Management
    //! \{

    //! Clears the memory contents, does not deallocate the memory.
    BinaryBufferBuilder & Clear() {
        size_ = 0;
        num_elements_ = 0;
        return *this;
    }

    //! Set the valid bytes in the buffer, use if the buffer is filled
    //! directly.
    BinaryBufferBuilder & set_size(size_t n) {
        assert(n <= capacity_);
        size_ = n;

        return *this;
    }

    //! Set the number of element sin the buffer, use if buffer is filled
    //! directly
    BinaryBufferBuilder & set_elements(size_t n) {
        num_elements_ = n;

        return *this;
    }

    //! Make sure that at least n bytes are allocated.
    BinaryBufferBuilder & Reserve(size_t n) {
        if (capacity_ < n)
        {
            capacity_ = n;
            data_ = static_cast<Byte*>(realloc(data_, capacity_));
        }

        return *this;
    }

    //! Dynamically allocate more memory. At least n bytes will be available,
    //! probably more to compensate future growth.
    BinaryBufferBuilder & DynReserve(size_t n) {
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
    BinaryBufferBuilder & Assign(const void* data, size_t len, size_t elements) {
        if (len > capacity_) Reserve(len);

        const Byte* cdata = reinterpret_cast<const Byte*>(data);
        std::copy(cdata, cdata + len, data_);
        size_ = len;
        num_elements_ = elements;

        return *this;
    }

    //! Copy the contents of another buffer object into this buffer, overwrites
    //! all current data. Roughly equivalent to clear() followed by append().
    BinaryBufferBuilder & Assign(const BinaryBufferBuilder& other) {
        if (&other != this)
            Assign(other.data(), other.size(), other.elements());

        return *this;
    }

    //! Align the size of the buffer to a multiple of n. Fills up with 0s.
    BinaryBufferBuilder & Align(size_t n) {
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
    BinaryBufferBuilder & Append(const void* data, size_t len, size_t elements = 0) {
        if (size_ + len > capacity_) DynReserve(size_ + len);

        const Byte* cdata = reinterpret_cast<const Byte*>(data);
        std::copy(cdata, cdata + len, data_ + size_);
        size_ += len;
        num_elements_ += elements;

        return *this;
    }

    //! Append the contents of a different buffer object to this one.
    BinaryBufferBuilder & Append(const class BinaryBufferBuilder& bb) {
        return Append(bb.data(), bb.size());
    }

    //! Append to contents of a std::string, excluding the null (which isn't
    //! contained in the string size anyway).
    BinaryBufferBuilder & AppendString(const std::string& s, size_t elements = 0) {
        return Append(s.data(), s.size(), elements);
    }

    //! Put (append) a single item of the template type T to the buffer. Be
    //! careful with implicit type conversions!
    template <typename Type, bool internal = false>
    BinaryBufferBuilder & Put(const Type item) {
        static_assert(std::is_pod<Type>::value,
                      "You only want to Put() POD types as raw values.");

        if (size_ + sizeof(Type) > capacity_) DynReserve(size_ + sizeof(Type));

        *reinterpret_cast<Type*>(data_ + size_) = item;
        size_ += sizeof(Type);
        if (!internal)
            num_elements_++;

        return *this;
    }

    //! Append a varint to the buffer.
    BinaryBufferBuilder & PutVarint(uint32_t v) {
        if (v < 128) {
            Put<uint8_t, true>(uint8_t(v));
        }
        else if (v < 128 * 128) {
            Put<uint8_t, true>((uint8_t)(((v >> 0) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)((v >> 7) & 0x7F));
        }
        else if (v < 128 * 128 * 128) {
            Put<uint8_t, true>((uint8_t)(((v >> 0) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 7) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)((v >> 14) & 0x7F));
        }
        else if (v < 128 * 128 * 128 * 128) {
            Put<uint8_t, true>((uint8_t)(((v >> 0) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 7) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)((v >> 21) & 0x7F));
        }
        else {
            Put<uint8_t, true>((uint8_t)(((v >> 0) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 7) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 21) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)((v >> 28) & 0x7F));
        }

        num_elements_++;
        return *this;
    }

    //! Append a varint to the buffer.
    BinaryBufferBuilder & PutVarint(int v) {
        return PutVarint((uint32_t)v);
    }

    //! Append a varint to the buffer.
    BinaryBufferBuilder & PutVarint(uint64_t v) {
        if (v < 128) {
            Put<uint8_t, true>(uint8_t(v));
        }
        else if (v < 128 * 128) {
            Put<uint8_t, true>((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)((v >> 07) & 0x7F));
        }
        else if (v < 128 * 128 * 128) {
            Put<uint8_t, true>((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)((v >> 14) & 0x7F));
        }
        else if (v < 128 * 128 * 128 * 128) {
            Put<uint8_t, true>((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)((v >> 21) & 0x7F));
        }
        else if (v < ((uint64_t)128) * 128 * 128 * 128 * 128) {
            Put<uint8_t, true>((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 21) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)((v >> 28) & 0x7F));
        }
        else if (v < ((uint64_t)128) * 128 * 128 * 128 * 128 * 128) {
            Put<uint8_t, true>((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 21) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 28) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)((v >> 35) & 0x7F));
        }
        else if (v < ((uint64_t)128) * 128 * 128 * 128 * 128 * 128 * 128) {
            Put<uint8_t, true>((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 21) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 28) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 35) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)((v >> 42) & 0x7F));
        }
        else if (v < ((uint64_t)128) * 128 * 128 * 128
                 * 128 * 128 * 128 * 128) {
            Put<uint8_t, true>((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 21) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 28) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 35) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 42) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)((v >> 49) & 0x7F));
        }
        else if (v < ((uint64_t)128) * 128 * 128 * 128
                 * 128 * 128 * 128 * 128 * 128) {
            Put<uint8_t, true>((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 21) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 28) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 35) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 42) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 49) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)((v >> 56) & 0x7F));
        }
        else {
            Put<uint8_t, true>((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 21) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 28) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 35) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 42) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 49) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)(((v >> 56) & 0x7F) | 0x80));
            Put<uint8_t, true>((uint8_t)((v >> 63) & 0x7F));
        }

        num_elements_++;
        return *this;
    }

    //! Put a string by saving it's length followed by the data itself.
    BinaryBufferBuilder & PutString(const char* data, size_t len) {
        //append with elements = 0 since PutVarint increases the element count
        return PutVarint((uint32_t)len).Append(data, len, 0);
    }

    //! Put a string by saving it's length followed by the data itself.
    BinaryBufferBuilder & PutString(const Byte* data, size_t len) {
        //append with elements = 0 since PutVarint increases the element count
        return PutVarint((uint32_t)len).Append(data, len, 0);
    }

    //! Put a string by saving it's length followed by the data itself.
    BinaryBufferBuilder & PutString(const std::string& str) {
        return PutString(str.data(), str.size());
    }

    //! Put a BinaryBufferBuilder by saving it's length followed by the data itself.
    BinaryBufferBuilder & PutString(const BinaryBufferBuilder& bb) {
        return PutString(bb.data(), bb.size());
    }

    //! \}
};

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BINARY_BUFFER_BUILDER_HEADER

/******************************************************************************/
