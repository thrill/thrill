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

#include <c7a/data/block.hpp>
#include <c7a/data/block_reader.hpp>
#include <c7a/data/block_sink.hpp>
#include <c7a/data/block_writer.hpp>

#include <cassert>
#include <memory>
#include <string>
#include <vector>

namespace c7a {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

template <size_t BlockSize>
class FileBlockSource;

/*!
 * A File or generally FileBase<BlockSize> is an ordered sequence of
 * VirtualBlock objects for storing items. By using the VirtualBlock
 * indirection, the File can be composed using existing Block objects (via
 * reference counting), but only contain a subset of the items in those
 * Blocks. This may be used for Zip() and Repartition().
 *
 * A File can be written using a BlockWriter instance, which is delivered by
 * GetWriter(). Thereafter it can be read (multiple times) using a BlockReader,
 * delivered by GetReader().
 *
 * Using a prefixsum over the number of items in a Block, one can seek to the
 * block contained any item offset in log_2(Blocks) time, though seeking within
 * the Block goes sequentially.
 */
template <size_t BlockSize>
class FileBase : public BlockSink<BlockSize>
{
public:
    enum { block_size = BlockSize };

    using Block = data::Block<BlockSize>;
    using BlockCPtr = std::shared_ptr<const Block>;
    using VirtualBlock = data::VirtualBlock<BlockSize>;

    using Writer = BlockWriterBase<BlockSize>;
    using Reader = BlockReader<FileBlockSource<BlockSize> >;

    //! Append a block to this file, the block must contain given number of
    //! items after the offset first.
    void Append(VirtualBlock&& vb) {
        assert(!closed_);
        if (vb.bytes_used == 0) return;
        blocks_.push_back(vb.block);
        nitems_sum_.push_back(NumItems() + vb.nitems);
        used_.push_back(vb.bytes_used);
        offset_of_first_.push_back(vb.first);
    }

    void Close() {
        assert(!closed_);
        closed_ = true;
    }

    // returns a string that identifies this string instance
    std::string ToString() {
        return "File@" + std::to_string((size_t) this);
    }

    bool closed() const {
        return closed_;
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

    //! Get BlockWriter.
    Writer GetWriter() {
        return Writer(this);
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

//! Default File class, using the default block size.
using File = FileBase<default_block_size>;

/*!
 * A BlockSource to read Blocks from a File. The FileBlockSource mainly contains
 * an index to the current block, which is incremented when the NextBlock() must
 * be delivered.
 */
template <size_t BlockSize>
class FileBlockSource
{
public:
    using Byte = unsigned char;

    using FileBase = data::FileBase<BlockSize>;

    using Block = data::Block<BlockSize>;
    using BlockCPtr = std::shared_ptr<const Block>;

    //! Advance to next block of file, delivers current_ and end_ for
    //! BlockReader
    bool NextBlock(const Byte** out_current, const Byte** out_end) {
        ++current_block_;

        if (current_block_ >= file_.NumBlocks())
            return false;

        const BlockCPtr& block = file_.blocks_[current_block_];
        if (current_block_ == first_block_) {
            *out_current = block->begin() + first_offset_;
            *out_end = block->begin() + file_.used_[current_block_];
        }
        else {
            *out_current = block->begin();
            *out_end = block->begin() + file_.used_[current_block_];
        }

        return true;
    }

    bool closed() const {
        return file_.closed();
    }

protected:
    //! Start reading a File
    FileBlockSource(const FileBase& file,
                    size_t first_block = 0, size_t first_offset = 0)
        : file_(file), first_block_(first_block), first_offset_(first_offset) {
        current_block_ = first_block_ - 1;
    }

    //! for calling the protected constructor
    friend class data::FileBase<BlockSize>;

    //! file to read blocks from
    const FileBase& file_;

    //! index of current block.
    size_t current_block_ = -1;

    //! number of the first block
    size_t first_block_;

    //! offset of first item in first block read
    size_t first_offset_;
};

//! Get BlockReader for beginning of File
template <size_t BlockSize>
typename FileBase<BlockSize>::Reader FileBase<BlockSize>::GetReader() const {
    return Reader(FileBlockSource<BlockSize>(*this, 0, 0));
}

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_FILE_HEADER

/******************************************************************************/
