/*******************************************************************************
 * c7a/net/binary-builder.hpp
 *
 * Classes BufferBuilder and BinaryReader to construct data blocks with variable
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
#ifndef C7A_NET_BINARY_BUILDER_HEADER
#define C7A_NET_BINARY_BUILDER_HEADER

#include <c7a/net/buffer.hpp>

#include <cassert>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <algorithm>

namespace c7a {
namespace net {
/*!
 * BinaryBuilder represents a dynamically growable area of memory, which can be
 * modified by appending integral data types via Put() and other basic
 * operations.
 */
class BinaryBuilder
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

public:
    //! \name Construction, Movement, Destruction
    //! \{

    //! Create a new empty object
    BinaryBuilder()
        : data_(nullptr), size_(0), capacity_(0)
    { }

    //! Copy-Constructor, duplicates memory content.
    BinaryBuilder(const BinaryBuilder& other)
        : data_(nullptr), size_(0), capacity_(0)
    {
        Assign(other);
    }

    //! Move-Constructor, moves memory area.
    BinaryBuilder(BinaryBuilder && other)
        : data_(other.data_), size_(other.size_), capacity_(other.capacity_)
    {
        other.data_ = nullptr;
        other.size_ = 0;
        other.capacity_ = 0;
    }

    //! Constructor, copy memory area.
    BinaryBuilder(const void* data, size_t n)
        : data_(nullptr), size_(0), capacity_(0)
    {
        Assign(data, n);
    }

    //! Constructor, create object with n bytes pre-allocated.
    explicit BinaryBuilder(size_t n)
        : data_(nullptr), size_(0), capacity_(0)
    {
        Reserve(n);
    }

    //! Constructor from std::string, COPIES string content.
    explicit BinaryBuilder(const std::string& str)
        : data_(nullptr), size_(0), capacity_(0)
    {
        Assign(str.data(), str.size());
    }

    //! Assignment operator: copy other's memory range into buffer.
    BinaryBuilder& operator = (const BinaryBuilder& other)
    {
        if (&other != this)
            Assign(other.data(), other.size());

        return *this;
    }

    //! Move-Assignment operator: move other's memory area into buffer.
    BinaryBuilder& operator = (BinaryBuilder && other)
    {
        if (this != &other)
        {
            if (data_) delete[] data_;
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
    ~BinaryBuilder()
    {
        Deallocate();
    }

    //! Deallocates the kept memory space (we use dealloc() instead of free()
    //! as a name, because sometimes "free" is replaced by the preprocessor)
    BinaryBuilder & Deallocate()
    {
        if (data_) free(data_);
        data_ = nullptr;
        size_ = capacity_ = 0;

        return *this;
    }

    //! \}

    //! \name Data, Size, and Capacity Accessors
    //! \{

    //! Return a pointer to the currently kept memory area.
    const Byte * data() const
    {
        return data_;
    }

    //! Return a writeable pointer to the currently kept memory area.
    Byte * data()
    {
        return data_;
    }

    //! Return the currently used length in bytes.
    size_t size() const
    {
        return size_;
    }

    //! Return the currently allocated buffer capacity.
    size_t capacity() const
    {
        return capacity_;
    }

    //! \}

    //! \name Buffer Growing, Clearing, and other Management
    //! \{

    //! Clears the memory contents, does not deallocate the memory.
    BinaryBuilder & Clear()
    {
        size_ = 0;
        return *this;
    }

    //! Set the valid bytes in the buffer, use if the buffer is filled
    //! directly.
    BinaryBuilder & set_size(size_t n)
    {
        assert(n <= capacity_);
        size_ = n;

        return *this;
    }

    //! Make sure that at least n bytes are allocated.
    BinaryBuilder & Reserve(size_t n)
    {
        if (capacity_ < n)
        {
            capacity_ = n;
            data_ = static_cast<Byte*>(realloc(data_, capacity_));
        }

        return *this;
    }

    //! Dynamically allocate more memory. At least n bytes will be available,
    //! probably more to compensate future growth.
    BinaryBuilder & DynReserve(size_t n)
    {
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
    const Byte * Detach()
    {
        const Byte* data = data_;
        data_ = nullptr;
        size_ = capacity_ = 0;
        return data;
    }

    //! Explicit conversion to std::string (copies memory of course).
    std::string ToString() const
    {
        return std::string(reinterpret_cast<const char*>(data_), size_);
    }

    //! Explicit conversion to Buffer MOVING the memory ownership.
    Buffer ToBuffer()
    {
        Buffer b = Buffer::Acquire(data_, size_);
        data_ = nullptr;
        size_ = capacity_ = 0;
        return std::move(b);
    }

    //! \}

    //! \name Assignment or Alignment
    //! \{

    //! Copy a memory range into the buffer, overwrites all current
    //! data. Roughly equivalent to clear() followed by append().
    BinaryBuilder & Assign(const void* data, size_t len)
    {
        if (len > capacity_) Reserve(len);

        const Byte* cdata = reinterpret_cast<const Byte*>(data);
        std::copy(cdata, cdata + len, data_);
        size_ = len;

        return *this;
    }

    //! Copy the contents of another buffer object into this buffer, overwrites
    //! all current data. Roughly equivalent to clear() followed by append().
    BinaryBuilder & Assign(const BinaryBuilder& other)
    {
        if (&other != this)
            Assign(other.data(), other.size());

        return *this;
    }

    //! Align the size of the buffer to a multiple of n. Fills up with 0s.
    BinaryBuilder & Align(size_t n)
    {
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
    BinaryBuilder & Append(const void* data, size_t len)
    {
        if (size_ + len > capacity_) DynReserve(size_ + len);

        const Byte* cdata = reinterpret_cast<const Byte*>(data);
        std::copy(cdata, cdata + len, data_ + size_);
        size_ += len;

        return *this;
    }

    //! Append the contents of a different buffer object to this one.
    BinaryBuilder & Append(const class BinaryBuilder& bb)
    {
        return Append(bb.data(), bb.size());
    }

    //! Append to contents of a std::string, excluding the null (which isn't
    //! contained in the string size anyway).
    BinaryBuilder & Append(const std::string& s)
    {
        return Append(s.data(), s.size());
    }

    //! Put (append) a single item of the template type T to the buffer. Be
    //! careful with implicit type conversions!
    template <typename Type>
    BinaryBuilder & Put(const Type item)
    {
        static_assert(std::is_integral<Type>::value,
                      "You only want to Put() integral types as raw values.");

        if (size_ + sizeof(Type) > capacity_) DynReserve(size_ + sizeof(Type));

        *reinterpret_cast<Type*>(data_ + size_) = item;
        size_ += sizeof(Type);

        return *this;
    }

    //! Append a varint to the buffer.
    BinaryBuilder & PutVarint(uint32_t v)
    {
        if (v < 128) {
            Put<uint8_t>(uint8_t(v));
        }
        else if (v < 128 * 128) {
            Put<uint8_t>((uint8_t)(((v >> 0) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)((v >> 7) & 0x7F));
        }
        else if (v < 128 * 128 * 128) {
            Put<uint8_t>((uint8_t)(((v >> 0) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 7) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)((v >> 14) & 0x7F));
        }
        else if (v < 128 * 128 * 128 * 128) {
            Put<uint8_t>((uint8_t)(((v >> 0) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 7) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)((v >> 21) & 0x7F));
        }
        else {
            Put<uint8_t>((uint8_t)(((v >> 0) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 7) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 21) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)((v >> 28) & 0x7F));
        }

        return *this;
    }

    //! Append a varint to the buffer.
    BinaryBuilder & PutVarint(int v)
    {
        return PutVarint((uint32_t)v);
    }

    //! Append a varint to the buffer.
    BinaryBuilder & PutVarint(uint64_t v)
    {
        if (v < 128) {
            Put<uint8_t>(uint8_t(v));
        }
        else if (v < 128 * 128) {
            Put<uint8_t>((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)((v >> 07) & 0x7F));
        }
        else if (v < 128 * 128 * 128) {
            Put<uint8_t>((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)((v >> 14) & 0x7F));
        }
        else if (v < 128 * 128 * 128 * 128) {
            Put<uint8_t>((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)((v >> 21) & 0x7F));
        }
        else if (v < ((uint64_t)128) * 128 * 128 * 128 * 128) {
            Put<uint8_t>((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 21) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)((v >> 28) & 0x7F));
        }
        else if (v < ((uint64_t)128) * 128 * 128 * 128 * 128 * 128) {
            Put<uint8_t>((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 21) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 28) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)((v >> 35) & 0x7F));
        }
        else if (v < ((uint64_t)128) * 128 * 128 * 128 * 128 * 128 * 128) {
            Put<uint8_t>((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 21) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 28) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 35) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)((v >> 42) & 0x7F));
        }
        else if (v < ((uint64_t)128) * 128 * 128 * 128
                 * 128 * 128 * 128 * 128) {
            Put<uint8_t>((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 21) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 28) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 35) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 42) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)((v >> 49) & 0x7F));
        }
        else if (v < ((uint64_t)128) * 128 * 128 * 128
                 * 128 * 128 * 128 * 128 * 128) {
            Put<uint8_t>((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 21) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 28) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 35) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 42) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 49) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)((v >> 56) & 0x7F));
        }
        else {
            Put<uint8_t>((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 21) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 28) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 35) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 42) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 49) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)(((v >> 56) & 0x7F) | 0x80));
            Put<uint8_t>((uint8_t)((v >> 63) & 0x7F));
        }

        return *this;
    }

    //! Put a string by saving it's length followed by the data itself.
    BinaryBuilder & PutString(const char* data, size_t len)
    {
        return PutVarint((uint32_t)len).Append(data, len);
    }

    //! Put a string by saving it's length followed by the data itself.
    BinaryBuilder & PutString(const Byte* data, size_t len)
    {
        return PutVarint((uint32_t)len).Append(data, len);
    }

    //! Put a string by saving it's length followed by the data itself.
    BinaryBuilder & PutString(const std::string& str)
    {
        return PutString(str.data(), str.size());
    }

    //! Put a BinaryBuilder by saving it's length followed by the data itself.
    BinaryBuilder & PutString(const BinaryBuilder& bb)
    {
        return PutString(bb.data(), bb.size());
    }

    //! \}
};

/*!
 * BinaryBuffer represents a memory area as pointer and valid length. It
 * is not deallocated or otherwise managed. This class can be used to pass
 * around references to BinaryBuilder objects.
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
    //! Constructor, assign memory area from BinaryBuilder.
    explicit BinaryBuffer(const BinaryBuilder& bb)
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

    //! Compare contents of two BinaryBuffers.
    bool operator == (const BinaryBuffer& br) const
    {
        if (size_ != br.size_) return false;
        return std::equal(data_, data_ + size_, br.data_);
    }

    //! Compare contents of two BinaryBuffers.
    bool operator != (const BinaryBuffer& br) const
    {
        if (size_ != br.size_) return true;
        return !std::equal(data_, data_ + size_, br.data_);
    }
};

/*!
 * BinaryReader represents a BinaryBuffer with an additional cursor with which
 * the memory can be read incrementally.
 */
class BinaryReader : public BinaryBuffer
{
protected:
    //! Current read cursor
    size_t cursor_ = 0;

public:
    //! \name Construction
    //! \{

    //! Constructor, assign memory area from BinaryBuilder.
    BinaryReader(const BinaryBuffer& br) // NOLINT
        : BinaryBuffer(br)
    { }

    //! Constructor, assign memory area from pointer and length.
    BinaryReader(const void* data, size_t n)
        : BinaryBuffer(data, n)
    { }

    //! Constructor, assign memory area from string, does NOT copy.
    explicit BinaryReader(const std::string& str)
        : BinaryBuffer(str)
    { }

    //! \}

    //! \name Size Accessors
    //! \{

    //! Return the current read cursor.
    size_t cursor() const
    {
        return cursor_;
    }

    //! Return the number of bytes still available at the cursor.
    bool available(size_t n) const
    {
        return (cursor_ + n <= size_);
    }

    //! Return true if the cursor is at the end of the buffer.
    bool empty() const
    {
        return (cursor_ == size_);
    }

    //! \}

    //! \name Cursor Movement and Checks
    //! \{

    //! Reset the read cursor.
    BinaryReader & Rewind()
    {
        cursor_ = 0;
        return *this;
    }

    //! Throws a std::underflow_error unless n bytes are available at the
    //! cursor.
    void CheckAvailable(size_t n) const
    {
        if (!available(n))
            throw std::underflow_error("BinaryReader underrun");
    }

    //! Advance the cursor given number of bytes without reading them.
    BinaryReader & Skip(size_t n)
    {
        CheckAvailable(n);
        cursor_ += n;

        return *this;
    }

    //! \}

    //! \name Cursor Reading Methods
    //! \{

    //! Fetch a number of unstructured bytes from the buffer, advancing the
    //! cursor.
    BinaryReader & Read(void* outdata, size_t datalen)
    {
        CheckAvailable(datalen);

        Byte* coutdata = reinterpret_cast<Byte*>(outdata);
        std::copy(data_ + cursor_, data_ + cursor_ + datalen, coutdata);
        cursor_ += datalen;

        return *this;
    }

    //! Fetch a number of unstructured bytes from the buffer as std::string,
    //! advancing the cursor.
    std::string Read(size_t datalen)
    {
        CheckAvailable(datalen);
        std::string out(
            reinterpret_cast<const char*>(data_ + cursor_), datalen);
        cursor_ += datalen;
        return out;
    }

    //! Fetch a single item of the template type Type from the buffer,
    //! advancing the cursor. Be careful with implicit type conversions!
    template <typename Type>
    Type Get()
    {
        static_assert(std::is_integral<Type>::value,
                      "You only want to Get() integral types as raw values.");

        CheckAvailable(sizeof(Type));

        Type ret = *reinterpret_cast<const Type*>(data_ + cursor_);
        cursor_ += sizeof(Type);

        return ret;
    }

    //! Fetch a varint with up to 32-bit from the buffer at the cursor.
    uint32_t GetVarint()
    {
        uint32_t u, v = Get<uint8_t>();
        if (!(v & 0x80)) return v;
        v &= 0x7F;
        u = Get<uint8_t>(), v |= (u & 0x7F) << 7;
        if (!(u & 0x80)) return v;
        u = Get<uint8_t>(), v |= (u & 0x7F) << 14;
        if (!(u & 0x80)) return v;
        u = Get<uint8_t>(), v |= (u & 0x7F) << 21;
        if (!(u & 0x80)) return v;
        u = Get<uint8_t>();
        if (u & 0xF0)
            throw std::overflow_error("Overflow during varint decoding.");
        v |= (u & 0x7F) << 28;
        return v;
    }

    //! Fetch a 64-bit varint from the buffer at the cursor.
    uint64_t GetVarint64()
    {
        uint64_t u, v = Get<uint8_t>();
        if (!(v & 0x80)) return v;
        v &= 0x7F;
        u = Get<uint8_t>(), v |= (u & 0x7F) << 7;
        if (!(u & 0x80)) return v;
        u = Get<uint8_t>(), v |= (u & 0x7F) << 14;
        if (!(u & 0x80)) return v;
        u = Get<uint8_t>(), v |= (u & 0x7F) << 21;
        if (!(u & 0x80)) return v;
        u = Get<uint8_t>(), v |= (u & 0x7F) << 28;
        if (!(u & 0x80)) return v;
        u = Get<uint8_t>(), v |= (u & 0x7F) << 35;
        if (!(u & 0x80)) return v;
        u = Get<uint8_t>(), v |= (u & 0x7F) << 42;
        if (!(u & 0x80)) return v;
        u = Get<uint8_t>(), v |= (u & 0x7F) << 49;
        if (!(u & 0x80)) return v;
        u = Get<uint8_t>(), v |= (u & 0x7F) << 56;
        if (!(u & 0x80)) return v;
        u = Get<uint8_t>();
        if (u & 0xFE)
            throw std::overflow_error("Overflow during varint64 decoding.");
        v |= (u & 0x7F) << 63;
        return v;
    }

    //! Fetch a string which was Put via Put_string().
    std::string GetString()
    {
        uint32_t len = GetVarint();
        return Read(len);
    }

    //! Fetch a BinaryBuffer to a binary string or blob which was Put via
    //! Put_string(). Does NOT copy the data.
    BinaryBuffer GetBinaryBuffer()
    {
        uint32_t len = GetVarint();
        // save object
        BinaryBuffer br(data_ + cursor_, len);
        // skip over sub block data
        Skip(len);
        return br;
    }

    //! \}
};
} // namespace net
} // namespace c7a

#endif // !C7A_NET_BINARY_BUILDER_HEADER

/******************************************************************************/
