/*******************************************************************************
 * thrill/data/concat_block_source.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_CONCAT_BLOCK_SOURCE_HEADER
#define C7A_DATA_CONCAT_BLOCK_SOURCE_HEADER

#include <thrill/data/block.hpp>

#include <utility>
#include <vector>

namespace c7a {
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
    //! Construct a BlockSource which concatenates many other BlockSources.
    explicit ConcatBlockSource(const std::vector<BlockSource>& sources)
        : sources_(sources) { }

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
} // namespace c7a

#endif // !C7A_DATA_CONCAT_BLOCK_SOURCE_HEADER

/******************************************************************************/
