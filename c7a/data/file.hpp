/*******************************************************************************
 * c7a/data/file.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_FILE_HEADER
#define C7A_DATA_FILE_HEADER

#include <memory>
#include <vector>
#include <string>
#include <cassert>
#include <c7a/data/block.hpp>
#include <c7a/data/block_reader.hpp>
#include <c7a/data/block_writer.hpp>
#include <c7a/data/serializer.hpp>

namespace c7a {
namespace data {

template <size_t BlockSize>
class FileBlockSource;

template <size_t BlockSize>
class FileBase
{
public:
    enum { block_size = BlockSize };

    using BlockType = Block<BlockSize>;
    using BlockCPtr = std::shared_ptr<const BlockType>;
    using Writer = BlockWriter<BlockType, FileBase>;
    using Reader = BlockReader<FileBlockSource<BlockSize> >;

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
    friend class FileBlockSource<BlockSize>;

    //! Closed files can not be altered
    bool closed_ = false;
};

using File = FileBase<default_block_size>;

//! A BlockSource to read Blocks from a File.
template <size_t BlockSize>
class FileBlockSource
{
public:
    using Byte = unsigned char;

    using BaseFile = FileBase<BlockSize>;

    using BlockType = Block<BlockSize>;
    using BlockCPtr = std::shared_ptr<const BlockType>;

    //! Start reading a File
    FileBlockSource(const BaseFile& file,
                    size_t current_block = 0, size_t first_offset = 0)
        : file_(file), current_block_(current_block),
          first_offset_(first_offset)
    { }

    //! Initialize the first block to be read by BlockReader
    void Initialize(const Byte** out_current, const Byte** out_end) {
        // set up reader for the (block,offset) pair
        if (current_block_ >= file_.NumBlocks()) {
            *out_current = *out_end = nullptr;
        }
        else {
            const BlockCPtr& block = file_.blocks_[current_block_];
            *out_current = block->begin() + first_offset_;
            *out_end = block->begin() + file_.used_[current_block_];
            assert(*out_current < *out_end);
        }
    }

    //! Advance to next block of file, delivers current_ and end_ for
    //! BlockReader
    bool NextBlock(const Byte** out_current, const Byte** out_end) {
        ++current_block_;

        if (current_block_ >= file_.NumBlocks())
            return false;

        const BlockCPtr& block = file_.blocks_[current_block_];
        *out_current = block->begin();
        *out_end = block->begin() + file_.used_[current_block_];

        return true;
    }

protected:
    //! file to read blocks from
    const BaseFile& file_;

    //! index of current block.
    size_t current_block_;

    //! offset of first item in first block read
    size_t first_offset_;
};

//! Get BlockReader for beginning of File
template <size_t BlockSize>
typename FileBase<BlockSize>::Reader FileBase<BlockSize>::GetReader() const {
    return Reader(FileBlockSource<BlockSize>(*this, 0, 0));
}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_FILE_HEADER

/******************************************************************************/
