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

    using Block = typename BlockSource::Block;
    using BlockCPtr = std::shared_ptr<const Block>;

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

#if THIS_DOES_NOT_WORK_AS_BLOCKSOURCE_IS_PURE_VIRTUAL

//! Pure virtual class which can be derived to implement sources for the
//! PolyBlockReader. It has the same methods as needed for a BlockSource
//! concept.
template <typename _Block>
class PolyBlockSource
{
public:
    using Byte = unsigned char;

    using Block = _Block;
    using BlockPtr = std::shared_ptr<Block>;

    //! Initialize the first block to be read by BlockReader
    virtual void Initialize(const Byte** out_current, const Byte** out_end) = 0;

    //! Advance to next block of file, delivers current_ and end_ for
    //! BlockReader. Returns false if the source is empty.
    virtual bool NextBlock(const Byte** out_current, const Byte** out_end) = 0;
};

//! Typedef of a polymorphic block reader, reading from a PolyBlockSource.
//! One uglyness is that the different Block sources (like File, BlockQueue, or
//! Channel) some hold state for the FileReader (e.g. when BlockQueue pops
//! items), and some are immutable (like File, where one iterates over Blocks).
//!
//! The only solution I saw, was a shared_ptr to the iteration information -tb.
template <typename Block>
using PolyBlockReader = BlockReader<PolyBlockSource<Block> >;

//! Generic polymorphism adapter for wrapping a class comforming to BlockSource
//! concept to class polymorphic class deriving from PolyBlockSource, to attach
//! it to a PolyBlockReader.
template <typename _Block, typename Source>
class PolyBlockSourceAdapter : public PolyBlockSource<_Block>
{
public:
    using Byte = unsigned char;

    using Block = _Block;
    using BlockPtr = typename Source::BlockPtr;

    PolyBlockSourceAdapter(Source& source)
        : source_(source)
    { }

    //! Initialize the first block to be read by BlockReader
    void Initialize(const Byte** out_current, const Byte** out_end) override {
        return source_->Initialize(out_current, out_end);
    }

    //! Advance to next block of file, delivers current_ and end_ for
    //! BlockReader. Returns false if the source is empty.
    bool NextBlock(const Byte** out_current, const Byte** out_end) override {
        return source_->NextBlock(out_current, out_end);
    }

protected:
    //! reference to the BlockSource.
    Source& source_;
};

#endif

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BLOCK_READER_HEADER

/******************************************************************************/
