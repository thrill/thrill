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

#include <c7a/common/logger.hpp>
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

class FileBlockSource;
class CachingBlockQueueSource;

/*!
 * A File or generally File<> is an ordered sequence of
 * Block objects for storing items. By using the Block
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
class File : public BlockSink
{
public:
    using BlockSource = FileBlockSource;
    using Writer = BlockWriter;
    using Reader = BlockReader<FileBlockSource>;

    //! Append a block to this file, the block must contain given number of
    //! items after the offset first.
    void AppendBlock(const Block& b) override {
        assert(!closed_);
        if (b.size() == 0) return;
        blocks_.push_back(b);
        nitems_sum_.push_back(NumItems() + b.nitems());
    }

    void Close() override {
        assert(!closed_);
        closed_ = true;
    }

    // returns a string that identifies this string instance
    std::string ToString() {
        return "File@" + std::to_string(reinterpret_cast<uintptr_t>(this));
    }

    bool closed() const {
        return closed_;
    }

    //! Return the number of blocks
    size_t NumBlocks() const { return blocks_.size(); }

    //! Return the number of items in the file
    size_t NumItems() const {
        return nitems_sum_.size() ? nitems_sum_.back() : 0;
    }

    //! Return the number of bytes used by the underlying blocks
    //size_t TotalBytes() const { return NumBlocks() * block_size; }

    //! Return shared pointer to a block
    const Block & block(size_t i) const {
        assert(i < blocks_.size());
        return blocks_[i];
    }

    //! Return number of items starting in block i
    size_t ItemsStartIn(size_t i) const {
        assert(i < blocks_.size());
        return nitems_sum_[i] - (i == 0 ? 0 : nitems_sum_[i - 1]);
    }

    //! Get BlockWriter.
    Writer GetWriter(size_t block_size = default_block_size) {
        return Writer(this, block_size);
    }

    //! Get BlockReader for beginning of File
    Reader GetReader() const;

    //! Get BlockReader seeked to the corresponding item index
    template <typename ItemType>
    Reader GetReaderAt(size_t index) const;

    //! Seek in File: return a Block range containing items begin, end of
    //! given type.
    template <typename ItemType>
    std::vector<Block> GetItemRange(size_t begin, size_t end) const;

    //! Read complete File into a std::string, obviously, this should only be
    //! used for debugging!
    std::string ReadComplete() const {
        std::string output;
        for (const Block& b : blocks_)
            output += b.ToString();
        return output;
    }

    //! Output the Block objects contained in this File.
    friend std::ostream& operator << (std::ostream& os, const File& f) {
        os << "[File " << std::hex << &f << std::dec
           << " Blocks=[";
        for (const Block& b : f.blocks_)
            os << "\n    " << b;
        return os << "]]";
    }

protected:
    //! the container holding blocks and thus shared pointers to all byte
    //! blocks.
    std::vector<Block> blocks_;

    //! inclusive prefixsum of number of elements of blocks, hence
    //! nitems_sum_[i] is the number of items starting in all blocks preceding
    //! and including the i-th block.
    std::vector<size_t> nitems_sum_;

    //! for access to blocks_ and used_
    friend class data::FileBlockSource;

    //! Closed files can not be altered
    bool closed_ = false;
};

/*!
 * A BlockSource to read Blocks from a File. The FileBlockSource mainly contains
 * an index to the current block, which is incremented when the NextBlock() must
 * be delivered.
 */
class FileBlockSource
{
public:
    //! Advance to next block of file, delivers current_ and end_ for
    //! BlockReader
    Block NextBlock() {
        ++current_block_;

        if (current_block_ >= file_.NumBlocks())
            return Block();

        if (current_block_ == first_block_) {
            // construct first block differently, in case we want to shorten it.
            Block b = file_.block(current_block_);
            if (first_item_ != keep_first_item)
                b.set_begin(first_item_);
            return b;
        }
        else {
            return file_.block(current_block_);
        }
    }

    bool closed() const {
        return file_.closed();
    }

protected:
    //! Start reading a File
    FileBlockSource(const File& file,
                    size_t first_block = 0, size_t first_item = keep_first_item)
        : file_(file), first_block_(first_block), first_item_(first_item) {
        current_block_ = first_block_ - 1;
    }

    //! for calling the protected constructor
    friend class data::File;
    friend class data::CachingBlockQueueSource;

    //! sentinel value for not changing the first_item item
    static const size_t keep_first_item = size_t(-1);

    //! file to read blocks from
    const File& file_;

    //! index of current block.
    size_t current_block_ = -1;

    //! number of the first block
    size_t first_block_;

    //! offset of first item in first block read
    size_t first_item_;
};

//! Get BlockReader for beginning of File
inline typename File::Reader File::GetReader() const {
    return Reader(FileBlockSource(*this, 0, 0));
}

//! Get BlockReader seeked to the corresponding item index
template <typename ItemType>
typename File::Reader
File::GetReaderAt(size_t index) const {
    static const bool debug = false;

    // perform binary search for item block with largest exclusive size
    // prefixsum less or equal to index.
    auto it =
        std::lower_bound(nitems_sum_.begin(), nitems_sum_.end(), index);

    if (it == nitems_sum_.end())
        die("Access beyond end of File?");

    size_t begin_block = it - nitems_sum_.begin();

    sLOG << "item" << index << "in block" << begin_block
         << "psum" << nitems_sum_[begin_block]
         << "first_item" << blocks_[begin_block].first_item();

    // start Reader at given first valid item in located block
    Reader fr(
        FileBlockSource(*this, begin_block,
                        blocks_[begin_block].first_item()));

    // skip over extra items in beginning of block
    size_t items_before = it == nitems_sum_.begin() ? 0 : *(it - 1);

    sLOG << "items_before" << items_before << "index" << index
         << "delta" << (index - items_before);
    assert(items_before <= index);

    // use fixed_size information to accelerate jump.
    if (Serialization<Reader, ItemType>::is_fixed_size)
    {
        const size_t skip_items = index - items_before;
        fr.Skip(skip_items,
                skip_items * ((Reader::self_verify ? sizeof(size_t) : 0) +
                              Serialization<Reader, ItemType>::fixed_size));
    }
    else
    {
        for (size_t i = items_before; i < index; ++i) {
            if (!fr.HasNext())
                die("Underflow in GetItemRange()");
            fr.template Next<ItemType>();
        }
    }

    return fr;
}

//! Seek in File: return a Block range containing items begin, end of
//! given type.
template <typename ItemType>
std::vector<Block>
File::GetItemRange(size_t begin, size_t end) const {
    assert(begin <= end);
    // deliver array of remaining blocks
    return GetReaderAt<ItemType>(begin)
           .template GetItemBatch<ItemType>(end - begin);
}

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_FILE_HEADER

/******************************************************************************/
