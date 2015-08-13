/*******************************************************************************
 * c7a/data/dyn_block_reader.hpp
 *
 * Dynamized instantiation of BlockReader which can polymorphically read from
 * different block sources using the same object type.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_DYN_BLOCK_READER_HEADER
#define C7A_DATA_DYN_BLOCK_READER_HEADER

#include <c7a/data/block_reader.hpp>

namespace c7a {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

/*!
 * This is a pure virtual base which will be used to fetch Blocks for the
 * BlockReader from different sources.
 */
class DynBlockSourceInterface
{
public:
    virtual Block NextBlock() = 0;
};

/*!
 * This is the actual BlockSource used to instantiate BlockReader. The
 * BlockSource holds a shared pointer to the polymorphic block source, which is
 * derived from the above virtual base class.
 *
 * Think of this class being embedded into the BlockReader and delivering Blocks
 * via the virtual function class from whatever is attached.
 */
class DynBlockSourcePtr
{
public:
    explicit DynBlockSourcePtr(
        std::shared_ptr<DynBlockSourceInterface>&& block_source_ptr)
        : block_source_ptr_(std::move(block_source_ptr))
    { }

    Block NextBlock() {
        return block_source_ptr_->NextBlock();
    }

protected:
    std::shared_ptr<DynBlockSourceInterface> block_source_ptr_;
};

//! Instantiation of BlockReader for reading from the polymorphic source.
using DynBlockReader = BlockReader<DynBlockSourcePtr>;

/*!
 * Adapter class to wrap any existing BlockSource concept class into a
 * DynBlockSourceInterface.
 */
template <typename BlockSource>
class DynBlockSourceAdapter : public DynBlockSourceInterface
{
public:
    explicit DynBlockSourceAdapter(BlockSource&& block_source)
        : block_source_(std::move(block_source))
    { }

    Block NextBlock() final {
        return block_source_.NextBlock();
    }

protected:
    BlockSource block_source_;
};

/*!
 * Method to construct a DynBlockReader from a non-polymorphic BlockSource. The
 * variadic parameters are passed to the constructor of the existing
 * BlockSource.
 */
template <typename BlockSource, typename ... Params>
DynBlockReader ConstructDynBlockReader(Params&& ... params) {
    return DynBlockReader(
        DynBlockSourcePtr(
            std::move(std::make_shared<DynBlockSourceAdapter<BlockSource> >(
                          BlockSource(std::forward<Params>(params) ...)))));
}

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_DYN_BLOCK_READER_HEADER

/******************************************************************************/
