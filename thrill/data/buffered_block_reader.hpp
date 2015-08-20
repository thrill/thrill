/*******************************************************************************
 * thrill/data/buffered_block_reader.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_BUFFERED_BLOCK_READER_HEADER
#define C7A_DATA_BUFFERED_BLOCK_READER_HEADER

#include <thrill/data/block_reader.hpp>

namespace c7a {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

/*!
 * Simple block reader that allows reading the first value without advancing.
 */
template <typename ItemType, typename BlockSource>
class BufferedBlockReader
{
private:
    BlockReader<BlockSource> reader_;
    ItemType current_;
    bool hasCurrent_;

public:
    //! Creates a new instance of this class, based on the given file reader.
    explicit BufferedBlockReader(BlockSource&& source)
        : reader_(std::move(source)) {
        Next();
    }

    //! Returns true if this reader holds a value at the current position.
    bool HasValue() {
        return hasCurrent_;
    }

    //! Gets the value at the current position of this reader.
    ItemType Value() {
        assert(HasValue());

        return current_;
    }

    //! Advances this reader to the next value.
    void Next() {
        hasCurrent_ = reader_.HasNext();
        if (hasCurrent_)
            current_ = reader_.template Next<ItemType>();
    }
};

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BUFFERED_BLOCK_READER_HEADER

/******************************************************************************/
