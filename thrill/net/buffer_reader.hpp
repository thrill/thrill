/*******************************************************************************
 * thrill/net/buffer_reader.hpp
 *
 * Look at the Doxygen below....
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_BUFFER_READER_HEADER
#define THRILL_NET_BUFFER_READER_HEADER

#include <thrill/common/item_serialization_tools.hpp>
#include <thrill/net/buffer_ref.hpp>

#include <algorithm>
#include <string>

namespace thrill {
namespace net {

//! \addtogroup net Network Communication
//! \{

/*!
 * BufferReader represents a BufferRef with an additional cursor with
 * which the memory can be read incrementally.
 */
class BufferReader
    : public BufferRef,
      public common::ItemReaderToolsBase<BufferReader>
{
private:
    //! Current read cursor
    size_t cursor_ = 0;

public:
    //! \name Construction
    //! \{

    //! Constructor, assign memory area from BinaryBuilder.
    BufferReader(const BufferRef& br) // NOLINT
        : BufferRef(br)
    { }

    //! Constructor, assign memory area from pointer and length.
    BufferReader(const void* data, size_t n)
        : BufferRef(data, n)
    { }

    //! Constructor, assign memory area from string, does NOT copy.
    explicit BufferReader(const std::string& str)
        : BufferRef(str)
    { }

    //! \}

    //! \name Size Accessors
    //! \{

    //! Return the current read cursor.
    size_t cursor() const {
        return cursor_;
    }

    //! Return the number of bytes still available at the cursor.
    bool available(size_t n) const {
        return (cursor_ + n <= size_);
    }

    //! Return true if the cursor is at the end of the buffer.
    bool empty() const {
        return (cursor_ == size_);
    }

    size_t Size() const {
        return size_;
    }

    //! Indicates if the reader was initialized with a nullptr and size 0
    bool IsNull() const {
        return data_ == nullptr;
    }

    //! \}

    //! \name Cursor Movement and Checks
    //! \{

    //! Reset the read cursor.
    BufferReader& Rewind() {
        cursor_ = 0;
        return *this;
    }

    //! Throws a std::underflow_error unless n bytes are available at the
    //! cursor.
    void CheckAvailable(size_t n) const {
        if (!available(n))
            throw std::underflow_error("BufferReader underrun");
    }

    //! Advance the cursor given number of bytes without reading them.
    BufferReader& Skip(size_t n) {
        CheckAvailable(n);
        cursor_ += n;

        return *this;
    }

    //! \}

    //! \name Cursor Reading Methods
    //! \{

    //! Fetch a number of unstructured bytes from the buffer, advancing the
    //! cursor.
    BufferReader& Read(void* outdata, size_t datalen) {
        CheckAvailable(datalen);

        Byte* coutdata = reinterpret_cast<Byte*>(outdata);
        std::copy(data_ + cursor_, data_ + cursor_ + datalen, coutdata);
        cursor_ += datalen;

        return *this;
    }

    //! Fetch a number of unstructured bytes from the buffer as std::string,
    //! advancing the cursor.
    std::string Read(size_t datalen) {
        CheckAvailable(datalen);
        std::string out(
            reinterpret_cast<const char*>(data_ + cursor_), datalen);
        cursor_ += datalen;
        return out;
    }

    //! Fetch a single item of the template type Type from the buffer,
    //! advancing the cursor. Be careful with implicit type conversions!
    template <typename Type>
    Type Get() {
        static_assert(std::is_pod<Type>::value,
                      "You only want to Get() POD types as raw values.");

        CheckAvailable(sizeof(Type));

        Type ret = *reinterpret_cast<const Type*>(data_ + cursor_);
        cursor_ += sizeof(Type);

        return ret;
    }

    //! Fetch a single byte from the buffer, advancing the cursor.
    Byte GetByte() {
        return Get<uint8_t>();
    }

    //! Fetch a single item of the template type Type from the buffer,
    //! advancing the cursor. Be careful with implicit type conversions!
    template <typename Type>
    Type GetRaw() {
        return Get<Type>();
    }

    //! Fetch a BufferRef to a binary string or blob which was Put via
    //! Put_string(). Does NOT copy the data.
    BufferRef GetBufferRef() {
        uint64_t len = GetVarint();
        // save object
        BufferRef br(data_ + cursor_, len);
        // skip over sub block data
        Skip(len);
        return br;
    }

    //! \}
};

//! \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_BUFFER_READER_HEADER

/******************************************************************************/
