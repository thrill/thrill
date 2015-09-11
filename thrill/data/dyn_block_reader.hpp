/*******************************************************************************
 * thrill/data/dyn_block_reader.hpp
 *
 * Dynamized instantiation of BlockReader which can polymorphically read from
 * different block sources using the same object type.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_DYN_BLOCK_READER_HEADER
#define THRILL_DATA_DYN_BLOCK_READER_HEADER

#include <thrill/data/block_reader.hpp>

namespace thrill {
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
class DynBlockSource
{
public:
    explicit DynBlockSource(
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
using DynBlockReader = BlockReader<DynBlockSource>;

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

    //! non-copyable: delete copy-constructor
    DynBlockSourceAdapter(const DynBlockSourceAdapter&) = delete;
    //! non-copyable: delete assignment operator
    DynBlockSourceAdapter& operator = (const DynBlockSourceAdapter&) = delete;
    //! move-constructor: default
    DynBlockSourceAdapter(DynBlockSourceAdapter&&) = default;
    //! move-assignment operator: default
    DynBlockSourceAdapter& operator = (DynBlockSourceAdapter&&) = default;

    Block NextBlock() final {
        return block_source_.NextBlock();
    }

protected:
    BlockSource block_source_;
};

/*!
 * Method to construct a DynBlockSource from a non-polymorphic BlockSource. The
 * variadic parameters are passed to the constructor of the existing
 * BlockSource.
 */
template <typename BlockSource, typename ... Params>
DynBlockSource ConstructDynBlockSource(Params&& ... params) {
    return DynBlockSource(
        std::move(std::make_shared<DynBlockSourceAdapter<BlockSource> >(
                      BlockSource(std::forward<Params>(params) ...))));
}

/*!
 * Method to construct a DynBlockReader from a non-polymorphic BlockSource. The
 * variadic parameters are passed to the constructor of the existing
 * BlockSource.
 */
template <typename BlockSource, typename ... Params>
DynBlockReader ConstructDynBlockReader(Params&& ... params) {
    return DynBlockReader(
        ConstructDynBlockSource<BlockSource>(std::forward<Params>(params) ...));
}

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_DYN_BLOCK_READER_HEADER

/******************************************************************************/
