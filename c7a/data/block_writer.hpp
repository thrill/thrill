/*******************************************************************************
 * c7a/data/block_writer.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_BLOCK_WRITER_HEADER
#define C7A_DATA_BLOCK_WRITER_HEADER

#include <c7a/data/block.hpp>
#include <c7a/data/serializer.hpp>
#include <c7a/common/item_serializer_tools.hpp>

namespace c7a {
namespace data {

template <typename BlockSink>
class BlockWriter : public common::ItemWriterToolsBase<BlockWriter<BlockSink> >
{
public:
    using Byte = unsigned char;
    using Block = typename std::decay<BlockSink>::type::Block;
    using BlockPtr = std::shared_ptr<Block>;

    //! Start build (appending blocks) to a File
    explicit BlockWriter(BlockSink sink)
        : sink_(std::forward<BlockSink>(sink)) {
        AllocateBlock();
    }

    //! non-copyable: delete copy-constructor
    BlockWriter(const BlockWriter&) = delete;
    //! non-copyable: delete assignment operator
    BlockWriter& operator = (const BlockWriter&) = delete;

    //! move-constructor
    BlockWriter(BlockWriter&&) = default;
    //! move-assignment
    BlockWriter& operator = (BlockWriter&&) = delete;

    //! On destruction, the last partial block is flushed.
    ~BlockWriter() {
        if (block_)
            Close();
    }

    //! Explicitly close the writer
    void Close() {
        if (!closed_) { //potential race condition
            closed_ = true;
            if (current_ != block_->begin() || nitems_) {
                FlushBlock();
                nitems_ = 0;
                block_ = BlockPtr();
                current_ = nullptr;
            }
            sink_.Close();
        }
    }

    //! Flush the current block (only really meaningful for a network sink).
    void Flush() {
        FlushBlock();
        AllocateBlock();
    }

    //! \name Appending (Generic) Items
    //! \{

    //! Mark beginning of an item.
    BlockWriter & MarkItem() {
        if (nitems_ == 0)
            first_offset_ = current_ - block_->begin();

        ++nitems_;

        return *this;
    }

    //! operator() appends a complete item
    template <typename T>
    BlockWriter& operator () (const T& x) {
        MarkItem();
        Serialize<BlockWriter, T>(x, *this);
        return *this;
    }

    //! \}

    //! \name Appending Write Functions
    //! \{

    //! Append a memory range to the block
    BlockWriter & Append(const void* data, size_t size) {

        const Byte* cdata = reinterpret_cast<const Byte*>(data);

        while (current_ + size > end_) {
            // partial copy of beginning of buffer
            size_t partial_size = end_ - current_;
            std::copy(cdata, cdata + partial_size, current_);

            cdata += partial_size;
            size -= partial_size;
            current_ += partial_size;

            FlushBlock();
            AllocateBlock();
        }

        // copy remaining bytes.
        std::copy(cdata, cdata + size, current_);
        current_ += size;

        return *this;
    }

    //! Append a single byte to the block
    BlockWriter & PutByte(Byte data) {
        if (current_ < end_) {
            *current_++ = data;
        }
        else {
            FlushBlock();
            AllocateBlock();
            *current_++ = data;
        }
        return *this;
    }

    //! Append to contents of a std::string, excluding the null (which isn't
    //! contained in the string size anyway).
    BlockWriter & Append(const std::string& str) {
        return Append(str.data(), str.size());
    }

    //! Put (append) a single item of the template type T to the buffer. Be
    //! careful with implicit type conversions!
    template <typename Type>
    BlockWriter & Put(const Type& item) {
        static_assert(std::is_pod<Type>::value,
                      "You only want to Put() POD types as raw values.");

        return Append(&item, sizeof(item));
    }

    //! \}

protected:
    //! Allocate a new block (overwriting the existing one).
    void AllocateBlock() {
        block_ = std::make_shared<Block>();
        current_ = block_->begin();
        end_ = block_->end();
        nitems_ = 0;
        first_offset_ = 0;
    }

    //! Flush the currently created block into the underlying File.
    void FlushBlock() {
        sink_.Append(block_, current_ - block_->begin(),
                     nitems_, first_offset_);
    }

    //! current block, already allocated as shared ptr, since we want to use
    //! make_shared.
    BlockPtr block_;

    //! current write pointer into block.
    Byte* current_;

    //! current end of block pointer. this is == block_.end(), just one
    //! indirection less.
    Byte* end_;

    //! number of items in current block
    size_t nitems_;

    //! offset of first item
    size_t first_offset_;

    //! file or stream sink to output blocks to.
    BlockSink sink_;

    //! Flag if Close was called explicitly
    bool closed_ = false;
};
} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BLOCK_WRITER_HEADER

/******************************************************************************/
