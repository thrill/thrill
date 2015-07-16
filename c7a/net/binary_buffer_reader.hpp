/*******************************************************************************
 * c7a/data/binary_buffer_reader.hpp
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
#ifndef C7A_DATA_BINARY_BUFFER_READER_HEADER
#define C7A_DATA_BINARY_BUFFER_READER_HEADER

#include "binary_buffer.hpp"
#include <c7a/common/item_serializer_tools.hpp>

namespace c7a {
namespace net {

/*!
 * BinaryBufferReader represents a BinaryBuffer with an additional cursor with
 * which the memory can be read incrementally.
 */
class BinaryBufferReader
    : public BinaryBuffer,
      public common::ItemReaderToolsBase<BinaryBufferReader>
{
protected:
    //! Current read cursor
    size_t cursor_ = 0;

public:
    //! \name Construction
    //! \{

    //! Constructor, assign memory area from BinaryBuilder.
    BinaryBufferReader(const BinaryBuffer& br) // NOLINT
        : BinaryBuffer(br)
    { }

    //! Constructor, assign memory area from pointer and length.
    BinaryBufferReader(const void* data, size_t n)
        : BinaryBuffer(data, n)
    { }

    //! Constructor, assign memory area from string, does NOT copy.
    explicit BinaryBufferReader(const std::string& str)
        : BinaryBuffer(str)
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
    BinaryBufferReader & Rewind() {
        cursor_ = 0;
        return *this;
    }

    //! Throws a std::underflow_error unless n bytes are available at the
    //! cursor.
    void CheckAvailable(size_t n) const {
        if (!available(n))
            throw std::underflow_error("BinaryBufferReader underrun");
    }

    //! Advance the cursor given number of bytes without reading them.
    BinaryBufferReader & Skip(size_t n) {
        CheckAvailable(n);
        cursor_ += n;

        return *this;
    }

    //! \}

    //! \name Cursor Reading Methods
    //! \{

    //! Fetch a number of unstructured bytes from the buffer, advancing the
    //! cursor.
    BinaryBufferReader & Read(void* outdata, size_t datalen) {
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

    //! Fetch a BinaryBuffer to a binary string or blob which was Put via
    //! Put_string(). Does NOT copy the data.
    BinaryBuffer GetBinaryBuffer() {
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

#endif // !C7A_DATA_BINARY_BUFFER_READER_HEADER

/******************************************************************************/
