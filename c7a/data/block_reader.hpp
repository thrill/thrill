/*******************************************************************************
 * c7a/data/block_reader.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_BLOCK_READER_HEADER
#define C7A_DATA_BLOCK_READER_HEADER

#include <c7a/data/block.hpp>
#include <c7a/data/serializer.hpp>

namespace c7a {
namespace data {

template <typename BlockSource>
class BlockReader
{
public:
    using Byte = unsigned char;

    using BlockType = typename BlockSource::BlockType;
    using BlockCPtr = std::shared_ptr<const BlockType>;

    //! Start reading a File
    BlockReader(BlockSource&& source)
        : source_(std::move(source)) {
        source_.Initialize(&current_, &end_);
    }

    //! \name Reading (Generic) Items
    //! \{

    //! Next() reads a complete item T
    template <typename T>
    T Next() {
        return Serializer<BlockReader, T>::deserialize(*this);
    }

    //! HasNext() returns true if at least one more byte is available.
    bool HasNext() {
        while (current_ == end_) {
            if (!NextBlock())
                return false;
        }
        return true;
    }

    //! \}

    //! \name Cursor Reading Methods
    //! \{

    //! Fetch a number of unstructured bytes from the current block, advancing
    //! the cursor.
    BlockReader & Read(void* outdata, size_t size) {

        Byte* cdata = reinterpret_cast<Byte*>(outdata);

        while (current_ + size > end_) {
            // partial copy of remainder of block
            size_t partial_size = end_ - current_;
            std::copy(current_, current_ + partial_size, cdata);

            cdata += partial_size;
            size -= partial_size;

            if (!NextBlock())
                throw std::runtime_error("Data underflow in BlockReader.");
        }

        // copy rest from current block
        std::copy(current_, current_ + size, cdata);
        current_ += size;

        return *this;
    }

    //! Fetch a number of unstructured bytes from the buffer as std::string,
    //! advancing the cursor.
    std::string Read(size_t datalen) {
        std::string out(datalen, 0);
        Read(const_cast<char*>(out.data()), out.size());
        return out;
    }

    //! Fetch a single byte from the current block, advancing the cursor.
    Byte ReadByte() {
        if (current_ < end_) {
            return *current_++;
        }
        else {
            // loop, since blocks can actually be empty.
            while (current_ < end_) {
                if (!NextBlock())
                    throw std::runtime_error("Data underflow in BlockReader.");
            }
            return *current_++;
        }
    }

    //! Fetch a single item of the template type Type from the buffer,
    //! advancing the cursor. Be careful with implicit type conversions!
    template <typename Type>
    Type Get() {
        static_assert(std::is_pod<Type>::value,
                      "You only want to Get() POD types as raw values.");

        Type ret;
        Read(&ret, sizeof(ret));
        return ret;
    }

    //! Fetch a varint with up to 32-bit from the buffer at the cursor.
    uint32_t GetVarint() {
        uint32_t u, v = ReadByte();
        if (!(v & 0x80)) return v;
        v &= 0x7F;
        u = ReadByte(), v |= (u & 0x7F) << 7;
        if (!(u & 0x80)) return v;
        u = ReadByte(), v |= (u & 0x7F) << 14;
        if (!(u & 0x80)) return v;
        u = ReadByte(), v |= (u & 0x7F) << 21;
        if (!(u & 0x80)) return v;
        u = ReadByte();
        if (u & 0xF0)
            throw std::overflow_error("Overflow during varint decoding.");
        v |= (u & 0x7F) << 28;
        return v;
    }

    //! Fetch a 64-bit varint from the buffer at the cursor.
    uint64_t GetVarint64() {
        uint64_t u, v = ReadByte();
        if (!(v & 0x80)) return v;
        v &= 0x7F;
        u = ReadByte(), v |= (u & 0x7F) << 7;
        if (!(u & 0x80)) return v;
        u = ReadByte(), v |= (u & 0x7F) << 14;
        if (!(u & 0x80)) return v;
        u = ReadByte(), v |= (u & 0x7F) << 21;
        if (!(u & 0x80)) return v;
        u = ReadByte(), v |= (u & 0x7F) << 28;
        if (!(u & 0x80)) return v;
        u = ReadByte(), v |= (u & 0x7F) << 35;
        if (!(u & 0x80)) return v;
        u = ReadByte(), v |= (u & 0x7F) << 42;
        if (!(u & 0x80)) return v;
        u = ReadByte(), v |= (u & 0x7F) << 49;
        if (!(u & 0x80)) return v;
        u = ReadByte(), v |= (u & 0x7F) << 56;
        if (!(u & 0x80)) return v;
        u = ReadByte();
        if (u & 0xFE)
            throw std::overflow_error("Overflow during varint64 decoding.");
        v |= (u & 0x7F) << 63;
        return v;
    }

    //! Fetch a string which was Put via Put_string().
    std::string GetString() {
        uint32_t len = GetVarint();
        return Read(len);
    }

    //! \}

protected:
    //! Instance of BlockSource. This is NOT a reference, as to enable embedding
    //! of FileBlockSource to compose classes into File::Reader.
    BlockSource source_;

    //! current read pointer into current block of file.
    const Byte* current_;

    //! pointer to end of current block.
    const Byte* end_;

    //! Call source_.NextBlock with appropriate parameters
    bool NextBlock() {
        return source_.NextBlock(&current_, &end_);
    }
};

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BLOCK_READER_HEADER

/******************************************************************************/
