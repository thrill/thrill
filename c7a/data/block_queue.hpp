/*******************************************************************************
 * c7a/data/block_queue.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Tobias Sturm <tobias.sturm@student.kit.edu>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_BLOCK_QUEUE_HEADER
#define C7A_DATA_BLOCK_QUEUE_HEADER

#include <condition_variable>
#include <mutex>
#include <atomic>
#include <memory> //shared_ptr
#include <c7a/data/file.hpp>
#include <c7a/common/concurrent_bounded_queue.hpp>

namespace c7a {
namespace data {

//! A BlockQueue is used to hand-over blocks between threads. It fulfills the
//same interface as \ref c7a::data::Stream and \ref c7a::data::File
template <size_t BlockSize = default_block_size>
class BlockQueue
{
public:
    using BlockPtr = std::shared_ptr<Block<BlockSize> >;

    void Append(const BlockPtr& block, size_t block_used, size_t nitems, size_t first) {
        queue_.emplace(block, block_used, nitems, first);
    }

    void Close() {
        assert(!closed_); //racing condition tolerated
        closed_ = true;
    }

    VirtualBlock<BlockSize> Pop() {
        return queue_.pop();
    }

    bool closed() const { return closed_; }
    bool empty() const { return queue_.empty(); }

private:
    common::ConcurrentBoundedQueue<VirtualBlock<BlockSize> > queue_;
    std::atomic<bool> closed_ = { false };
};

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BLOCK_QUEUE_HEADER

/******************************************************************************/
