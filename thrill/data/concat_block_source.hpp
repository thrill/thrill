/*******************************************************************************
 * thrill/data/concat_block_source.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_CONCAT_BLOCK_SOURCE_HEADER
#define THRILL_DATA_CONCAT_BLOCK_SOURCE_HEADER

#include <thrill/data/block.hpp>

#include <utility>
#include <vector>

namespace thrill {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

/*!
 * ConcatBlockSource is a BlockSource which concatenates all Blocks available
 * from a vector of BlockSources. They are concatenated in order: first all
 * Blocks from source zero, then from source one, etc.
 */
template <typename BlockSource>
class ConcatBlockSource
{
public:
    //! default constructor
    ConcatBlockSource() = default;

    //! Construct a BlockSource which concatenates many other BlockSources.
    explicit ConcatBlockSource(std::vector<BlockSource>&& sources)
        : sources_(std::move(sources)) { }

    //! non-copyable: delete copy-constructor
    ConcatBlockSource(const ConcatBlockSource&) = delete;
    //! non-copyable: delete assignment operator
    ConcatBlockSource& operator = (const ConcatBlockSource&) = delete;
    //! move-constructor: default
    ConcatBlockSource(ConcatBlockSource&&) = default;
    //! move-assignment operator: default
    ConcatBlockSource& operator = (ConcatBlockSource&&) = default;

    //! Advance to next block of file, delivers current_ and end_ for
    //! BlockReader. Returns false if the source is empty.
    Block NextBlock() {
        for ( ; current_ < sources_.size(); ++current_) {
            Block b = sources_[current_].NextBlock();
            if (b.IsValid()) return b;
        }
        return Block();
    }

protected:
    //! vector containing block sources
    std::vector<BlockSource> sources_;

    //! current source, all sources < current_ are empty.
    size_t current_ = 0;
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_CONCAT_BLOCK_SOURCE_HEADER

/******************************************************************************/
