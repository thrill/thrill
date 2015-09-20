/*******************************************************************************
 * thrill/data/fetching_block_queue.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_PREFETCHING_BLOCK_QUEUE_HEADER
#define THRILL_DATA_PREFETCHING_BLOCK_QUEUE_HEADER

#include <thrill/common/concurrent_bounded_queue.hpp>

namespace thrill {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

class PrefetchingBlockQueue final
{
protected:
    mutable std::mutex mutex_;
    std::queue<Block> unpinned_queue_;
    std::queue<Block> pinned_queue_;
    std::condition_variable cv_;
    std::atomic<size_t> currently_fetching_ = { 0 };
    const size_t desired_;
public:

    PrefetchingBlockQueue(size_t desired_prefetched) : desired_(desired_prefetched) {}

    //we cannod move since callbacks would break (?)
    PrefetchingBlockQueue(PrefetchingBlockQueue&& other) = delete;

    //! If pinned_queue_ holds less than the desired_ number of blocks
    //! MaybePrefetch starts the pin process for the required amount of
    //! blocks (or unpinned_queue_ is empty). currently_fetching_
    //! is used to prevent too many pin calls by two consecutove calls to
    //! MaybePrefetch. MaybePrefetch notifies the condition variable cv_
    void MaybePrefetch() {
        while(currently_fetching_ + pinned_queue_.size() < desired_ && !unpinned_queue_.empty()) {
            //block is valid (not end-of-x block)
            if (unpinned_queue_.front().byte_block_) {
                currently_fetching_++;
                // we have a block which is not swapped in
                //copy block since http://stackoverflow.com/a/14763579/359326
                unpinned_queue_.front().byte_block_->Prefetch([&, block = unpinned_queue_.pop()](){
                    currently_fetching_--;
                    pinned_queue_.push(block);
                    cv_.notify_one();
                });
            } else { // for end-of-x blocks or already in memory
                pinned_queue_.push_back(unpinned_queue_.front());
                unpinned_queue_.pop();
                cv_.notify_one();
            }
        }
    }

    //! Pushes a copy of source onto back of the queue.
    void push(const Block& source) {
        std::unique_lock<std::mutex> lock(other.mutex_);
        unpinned_queue_.push(source);
        MaybePrefetch();
    }

    //! Pushes given element into the queue by utilizing element's move
    //! constructor
    void push(Block&& source) {
        std::unique_lock<std::mutex> lock(other.mutex_);
        unpinned_queue_.push(std::move(source));
        MaybePrefetch();
    }

    //! Pushes a new element into the queue. The element is constructed with
    //! given arguments.
    template <typename ... Arguments>
    void emplace(Arguments&& ... args) {
        std::unique_lock<std::mutex> lock(mutex_);
        unpinned_queue_.emplace(args ...);
        MaybePrefetch();
    }

    //! Returns: true if queue has no items; false otherwise.
    bool empty() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return unpinned_queue_.empty() && pinned_queue_.empty();
    }

    //! Clears the queue.
    void clear() {
        std::unique_lock<std::mutex> lock(mutex_);
        unpinned_queue_.clear();
        pinned_queue_.clear();
    }

    //! If a fetched Block is available, pops it from the queue, move it to destination,
    //! destroying the original position. Otherwise does nothing.
    bool try_pop(Block& destination) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (pinned_queue_.empty())
            return false;

        destination = std::move(pinned_queue_.front());
        pinned_queue_.pop();
        return true;
    }

    //! If value is available, pops it from the queue, move it to
    //! destination. If no item is in the queue, wait until there is one.
    void pop(Block& destination) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [=]() { return !queue_.empty(); });
        destination = std::move(queue_.front());
        queue_.pop();
    }

    //! return number of items available in the queue (tbb says "can return
    //! negative size", due to pending pop()s, but we ignore that here).
    //! \returns number of blocks (includes pinned, unpinned and currently fetching)
    size_t size() {
        std::unique_lock<std::mutex> lock(mutex_);
        return pinned_queue_.size() + unpinned_queue_.size() + currently_fetching_;
    }
};

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_PREFETCHING_BLOCK_QUEUE_HEADER

/******************************************************************************/
