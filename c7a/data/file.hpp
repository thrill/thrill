/*******************************************************************************
 * c7a/data/file.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_FILE_HEADER
#define C7A_DATA_FILE_HEADER

#include <memory>
#include <vector>
#include <string>
#include <cassert>
#include <c7a/data/serializer.hpp>

namespace c7a {
namespace data {

//! default block size of files.
static const size_t default_block_size = 2 * 1024 * 1024;

template <size_t BlockSize>
class Block
{
public:
    //! type of underlying memory area
    using Byte = unsigned char;

protected:
    //! constant size of memory block
    static const size_t size_ = BlockSize;

    //! the memory block itself
    Byte data_[size_];

public:
    //! mutable data accessor to memory block
    Byte * data() { return data_; }
    //! const data accessor to memory block
    const Byte * data() const { return data_; }

    //! mutable data accessor to beginning of memory block
    Byte * begin() { return data_; }
    //! const data accessor to beginning of memory block
    const Byte * begin() const { return data_; }

    //! mutable data accessor beyond end of memory block
    Byte * end() { return data_ + size_; }
    //! const data accessor beyond end of memory block
    const Byte * end() const { return data_ + size_; }

    //! the block size
    size_t size() const { return size_; }
};

/**
 * VirtualBlocks combine a reference to a \ref Block and book-keeping
 * information.
 *
 * Multiple virtual blocks can point to the same block but have different
 * book-keeping information!
 */
template <size_t BlockSize = default_block_size>
struct VirtualBlock
{
    using BlockType = Block<BlockSize>;
    using BlockCPtr = std::shared_ptr<const BlockType>;

    VirtualBlock()
        : block_used(0), nitems(0), first(0)
    { }

    VirtualBlock(const BlockCPtr& block,
                 size_t block_used, size_t nitems, size_t first)
        : block(block),
          block_used(block_used),
          nitems(nitems),
          first(first) { }

    //! referenced block
    BlockCPtr block;

    //! number of valid bytes in the block (can be used to virtually shorten
    //! a block)
    size_t    block_used;

    //! number of valid items in this block (includes cut-off element at the end)
    size_t    nitems;

    //! offset of first element in the block
    size_t    first;

    //! Releases the reference to the block and resets book-keeping info
    void      Release() {
        block = BlockCPtr();
        block_used = 0;
        nitems = 0;
        first = 0;
    }
};

template <typename Block, typename Target>
class BlockWriter;

template <size_t BlockSize>
class BlockReader;

template <size_t BlockSize>
class FileBase
{
public:
    enum { block_size = BlockSize };

    using BlockType = Block<BlockSize>;
    using BlockCPtr = std::shared_ptr<const BlockType>;
    using Writer = BlockWriter<BlockType, FileBase>;
    using Reader = BlockReader<BlockSize>;

    //! Append a block to this file, the block must contain given number of
    //! items after the offset first.
    void Append(const BlockCPtr& block, size_t block_used,
                size_t nitems, size_t first) {
        assert(!closed_);
        blocks_.push_back(block);
        nitems_sum_.push_back(NumItems() + nitems);
        used_.push_back(block_used);
        offset_of_first_.push_back(first);
    }

    void Close() {
        assert(!closed_);
        closed_ = true;
    }

    //! Return the number of blocks
    size_t NumBlocks() const { return blocks_.size(); }

    //! Return the number of items in the file
    size_t NumItems() const { return nitems_sum_.back(); }

    //! Return the number of bytes used by the underlying blocks
    size_t TotalBytes() const { return NumBlocks() * block_size; }

    //! Return shared pointer to a block
    const BlockCPtr & block(size_t i) const {
        assert(i < blocks_.size());
        return blocks_[i];
    }

    //! Return number of items starting in block i
    size_t ItemsStartIn(size_t i) const {
        assert(i < blocks_.size());
        return nitems_sum_[i + 1] - nitems_sum_[i];
    }

    //! Return offset of first item in block i
    size_t offset_of_first(size_t i) const {
        assert(i < offset_of_first_.size());
        return offset_of_first_[i];
    }

    //! Return number of bytes actually used in block i
    size_t used(size_t i) const {
        assert(i < used_.size());
        return used_[i];
    }

    //! Return block i as a std::string (for debugging)
    std::string BlockAsString(size_t i) const {
        assert(i < blocks_.size());
        return std::string(reinterpret_cast<const char*>(blocks_[i]->data()),
                           used_[i]);
    }

    //! Get BlockReader for beginning of File
    Reader GetReader() const;

protected:
    //! the container holding shared pointers to all blocks.
    std::vector<BlockCPtr> blocks_;

    //! plain size of valid bytes in the corresponding block.
    std::vector<size_t> used_;

    //! exclusive prefixsum of number of elements of blocks. The last item of
    //! the vector contains the current total number of items; hence
    //! nitems_sum_.size() == blocks_.size() + 1, always.
    std::vector<size_t> nitems_sum_ = { 0 };

    //! offset to the first element in the Block. The cut-off element before
    //! that offset is not included in the element_count
    std::vector<size_t> offset_of_first_;

    //! for access to blocks_ and used_
    friend class BlockReader<BlockSize>;

    //! Closed files can not be altered
    bool closed_ = false;
};

using File = FileBase<default_block_size>;

template <typename Block, typename Target>
class BlockWriter
{
public:
    using Byte = unsigned char;

    using BlockPtr = std::shared_ptr<Block>;

    //! Start build (appending blocks) to a File
    BlockWriter(Target& target)
        : target_(target) {
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
            target_.Close();
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
        Serializer<BlockWriter, T>::serialize(x, *this);
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
    BlockWriter & AppendByte(Byte data) {
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

    //! Append a varint to the buffer.
    BlockWriter & PutVarint(uint32_t v) {
        if (v < 128) {
            AppendByte(uint8_t(v));
        }
        else if (v < 128 * 128) {
            AppendByte((uint8_t)(((v >> 0) & 0x7F) | 0x80));
            AppendByte((uint8_t)((v >> 7) & 0x7F));
        }
        else if (v < 128 * 128 * 128) {
            AppendByte((uint8_t)(((v >> 0) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 7) & 0x7F) | 0x80));
            AppendByte((uint8_t)((v >> 14) & 0x7F));
        }
        else if (v < 128 * 128 * 128 * 128) {
            AppendByte((uint8_t)(((v >> 0) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 7) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            AppendByte((uint8_t)((v >> 21) & 0x7F));
        }
        else {
            AppendByte((uint8_t)(((v >> 0) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 7) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 21) & 0x7F) | 0x80));
            AppendByte((uint8_t)((v >> 28) & 0x7F));
        }

        return *this;
    }

    //! Append a varint to the buffer.
    BlockWriter & PutVarint(int v) {
        return PutVarint((uint32_t)v);
    }

    //! Append a varint to the buffer.
    BlockWriter & PutVarint(uint64_t v) {
        if (v < 128) {
            AppendByte(uint8_t(v));
        }
        else if (v < 128 * 128) {
            AppendByte((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            AppendByte((uint8_t)((v >> 07) & 0x7F));
        }
        else if (v < 128 * 128 * 128) {
            AppendByte((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            AppendByte((uint8_t)((v >> 14) & 0x7F));
        }
        else if (v < 128 * 128 * 128 * 128) {
            AppendByte((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            AppendByte((uint8_t)((v >> 21) & 0x7F));
        }
        else if (v < ((uint64_t)128) * 128 * 128 * 128 * 128) {
            AppendByte((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 21) & 0x7F) | 0x80));
            AppendByte((uint8_t)((v >> 28) & 0x7F));
        }
        else if (v < ((uint64_t)128) * 128 * 128 * 128 * 128 * 128) {
            AppendByte((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 21) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 28) & 0x7F) | 0x80));
            AppendByte((uint8_t)((v >> 35) & 0x7F));
        }
        else if (v < ((uint64_t)128) * 128 * 128 * 128 * 128 * 128 * 128) {
            AppendByte((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 21) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 28) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 35) & 0x7F) | 0x80));
            AppendByte((uint8_t)((v >> 42) & 0x7F));
        }
        else if (v < ((uint64_t)128) * 128 * 128 * 128
                 * 128 * 128 * 128 * 128) {
            AppendByte((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 21) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 28) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 35) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 42) & 0x7F) | 0x80));
            AppendByte((uint8_t)((v >> 49) & 0x7F));
        }
        else if (v < ((uint64_t)128) * 128 * 128 * 128
                 * 128 * 128 * 128 * 128 * 128) {
            AppendByte((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 21) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 28) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 35) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 42) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 49) & 0x7F) | 0x80));
            AppendByte((uint8_t)((v >> 56) & 0x7F));
        }
        else {
            AppendByte((uint8_t)(((v >> 00) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 07) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 14) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 21) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 28) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 35) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 42) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 49) & 0x7F) | 0x80));
            AppendByte((uint8_t)(((v >> 56) & 0x7F) | 0x80));
            AppendByte((uint8_t)((v >> 63) & 0x7F));
        }

        return *this;
    }

    //! Put a string by saving its length followed by the data itself.
    BlockWriter & PutString(const char* data, size_t len) {
        return PutVarint((uint32_t)len).Append(data, len);
    }

    //! Put a string by saving its length followed by the data itself.
    BlockWriter & PutString(const Byte* data, size_t len) {
        return PutVarint((uint32_t)len).Append(data, len);
    }

    //! Put a string by saving its length followed by the data itself.
    BlockWriter & PutString(const std::string& str) {
        return PutString(str.data(), str.size());
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
        target_.Append(block_, current_ - block_->begin(),
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

    //! file or stream target to output blocks to
    Target& target_;

    //! Flag if Close was called explicilty
    bool closed_ = false;
};

template <size_t BlockSize>
class BlockReader
{
public:
    using BaseFile = FileBase<BlockSize>;

    using BlockType = Block<BlockSize>;
    using BlockCPtr = std::shared_ptr<const BlockType>;

    using Byte = unsigned char;

    //! Start reading a File
    BlockReader(const BaseFile& file,
                size_t current_block = 0, size_t offset = 0)
        : file_(file), current_block_(current_block) {
        // set up reader for the block+offset pair
        if (current_block_ >= file_.NumBlocks()) {
            current_ = end_ = nullptr;
        }
        else {
            const BlockCPtr& block = file_.blocks_[current_block_];
            current_ = block->begin() + offset;
            end_ = block->begin() + file_.used_[current_block_];
            assert(current_ < end_);
        }
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
    //! Advance to next block of file.
    bool NextBlock() {
        ++current_block_;

        if (current_block_ >= file_.NumBlocks())
            return false;

        const BlockCPtr& block = file_.blocks_[current_block_];
        current_ = block->begin();
        end_ = block->begin() + file_.used_[current_block_];

        return true;
    }

    //! file to read blocks from
    const BaseFile& file_;

    //! index of current block.
    size_t current_block_;

    //! current read pointer into current block of file.
    const Byte* current_;

    //! pointer to end of current block.
    const Byte* end_;
};

//! Get BlockReader for beginning of File
template <size_t BlockSize>
typename FileBase<BlockSize>::Reader FileBase<BlockSize>::GetReader() const {
    return Reader(*this, 0, 0);
}
} // namespace data
} // namespace c7a

#endif // !C7A_DATA_FILE_HEADER

/******************************************************************************/
