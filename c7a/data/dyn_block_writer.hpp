/*******************************************************************************
 * c7a/data/dyn_block_writer.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_DYN_BLOCK_WRITER_HEADER
#define C7A_DATA_DYN_BLOCK_WRITER_HEADER

#include <c7a/data/block_writer.hpp>
#include <c7a/data/file.hpp>
#include <c7a/data/block_queue.hpp>

namespace c7a {
namespace data {

//! Class injected into BlockWriter which can be switch between different
//! BlockSink implementations. This is quite hacky. In clean object-oriented
//! code, this would be done by making all BlockSinks have virtual functions and
//! a common super class BlockSink. But this increases overhead for simple
//! operations, hence we do these switchs and union tricks to bypass type
//! casts. -tb
template <typename _Block>
class DynBlockSink
{
public:
    using Block = _Block;
    using BlockPtr = std::shared_ptr<Block>;

    static const size_t block_size = Block::block_size;

    using FileSink = FileBase<block_size>;
    using BlockQueueSink = BlockQueue<block_size>;

    explicit DynBlockSink(FileSink* file)
        : type_(FILE) {
        sink_.file_ = file;
    }

    explicit DynBlockSink(BlockQueueSink* block_queue)
        : type_(BLOCK_QUEUE) {
        sink_.block_queue_ = block_queue;
    }

    //! Closes the sink. Must not be called multiple times
    void Close() {
        switch (type_) {
        case FILE: return sink_.file_->Close();
        case BLOCK_QUEUE: return sink_.block_queue_->Close();
        default: abort();
        }
    }

    //! Appends the VirtualBlock and detaches it afterwards.
    void Append(const BlockPtr& block, size_t block_used,
                size_t nitems, size_t first) {
        switch (type_) {
        case FILE:
            return sink_.file_->Append(block, block_used, nitems, first);
        case BLOCK_QUEUE:
            return sink_.block_queue_->Append(block, block_used, nitems, first);
        default: abort();
        }
    }

protected:
    //! enumeration switch defining the attached sink type.
    enum  {
        FILE, BLOCK_QUEUE
    } type_;

    //! union containing a pointer to the attached sink type.
    union {
        FileSink      * file_;
        BlockQueueSink* block_queue_;
    } sink_;
};

//! Typedef of a dynamic block writer, writing to a DynBlockSink.
template <typename Block>
using DynBlockWriter = BlockWriter<Block, DynBlockSink<Block> >;

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_DYN_BLOCK_WRITER_HEADER

/******************************************************************************/
