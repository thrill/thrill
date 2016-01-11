/*******************************************************************************
 * thrill/data/block_pool.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/logger.hpp>
#include <thrill/data/block.hpp>
#include <thrill/data/block_pool.hpp>
#include <thrill/io/file_base.hpp>
#include <thrill/mem/aligned_alloc.hpp>

#include <limits>

namespace thrill {
namespace data {

//! debug block life cycle output: create, destroy
static const bool debug_blc = false;

//! debug block pinning:
static const bool debug_pin = false;

//! debug block eviction: evict, write complete, read complete
static const bool debug_em = true;

size_t BlockPool::soft_ram_limit_ = 300000000lu;
size_t BlockPool::hard_ram_limit_ = 500000000lu;

BlockPool::~BlockPool() {
    pin_count_.AssertZero();

    LOG1 << "~BlockPool():"
         << " max_pins=" << pin_count_.max_pins
         << " max_pinned_bytes=" << pin_count_.max_pinned_bytes;
}

PinnedByteBlockPtr BlockPool::AllocateByteBlock(size_t size, size_t local_worker_id) {
    assert(local_worker_id < workers_per_host_);
    std::unique_lock<std::mutex> lock(mutex_);

    RequestInternalMemory(lock, size);

    // allocate block memory.
    Byte* data = static_cast<Byte*>(mem::aligned_alloc(size));

    // create counting ptr, no need for special make_shared()-equivalent
    PinnedByteBlockPtr result(new ByteBlock(data, size, this), local_worker_id);
    IncBlockPinCountNoLock(result.get(), local_worker_id);

    pin_count_.Increment(local_worker_id, size);

    LOGC(debug_blc)
        << "BlockPool::AllocateBlock() size=" << size
        << " local_worker_id=" << local_worker_id
        << " total_count=" << block_count()
        << " total_size=" << mem_manager_.total()
        << pin_count_;

    return result;
}

//! Pins a block by swapping it in if required.
std::future<PinnedBlock> BlockPool::PinBlock(const Block& block, size_t local_worker_id) {
    assert(local_worker_id < workers_per_host_);
    std::unique_lock<std::mutex> lock(mutex_);

    ByteBlock* byte_block = block.byte_block();

    if (byte_block->pin_count_[local_worker_id] > 0) {
        // We may get a Block who's underlying is already pinned, since
        // PinnedBlock become Blocks when transfered between Files or delivered
        // via GetItemRange() or Scatter().

        IncBlockPinCountNoLock(byte_block, local_worker_id);

        LOGC(debug_pin)
            << "BlockPool::PinBlock block=" << &block
            << " already pinned by thread";

        std::promise<PinnedBlock> result;
        result.set_value(PinnedBlock(block, local_worker_id));
        return result.get_future();
    }

    if (byte_block->total_pins_ > 0) {
        // This block was already pinned by another thread, hence we only need
        // to get a pin for the new thread.

        IncBlockPinCountNoLock(byte_block, local_worker_id);
        pin_count_.Increment(local_worker_id, byte_block->size());

        LOGC(debug_pin)
            << "BlockPool::PinBlock block=" << &block
            << " already pinned by another thread"
            << pin_count_;

        std::promise<PinnedBlock> result;
        result.set_value(PinnedBlock(block, local_worker_id));
        return result.get_future();
    }

    if (byte_block->in_memory())
    {
        // unpinned block in memory, no need to load from EM.

        // remove from unpinned list
        assert(unpinned_blocks_.exists(byte_block));
        unpinned_blocks_.erase(byte_block);

        IncBlockPinCountNoLock(byte_block, local_worker_id);
        pin_count_.Increment(local_worker_id, byte_block->size());

        LOGC(debug_pin)
            << "BlockPool::PinBlock block=" << &block
            << " pinned from internal memory"
            << pin_count_;

        std::promise<PinnedBlock> result;
        result.set_value(PinnedBlock(block, local_worker_id));
        return result.get_future();
    }

    die_unless(reading_.find(byte_block) == reading_.end());

    // else need to initiate an async read to get the data.

    die_unless(byte_block->em_bid_.storage);

    // maybe blocking call until memory is available, this also swaps out other
    // blocks.
    RequestInternalMemory(lock, byte_block->size());

    // the requested memory is already counted as a pin.
    pin_count_.Increment(local_worker_id, byte_block->size());

    // initiate reading from EM.
    assert(reading_.find(byte_block) == reading_.end());
    ReadRequest* read = &reading_[byte_block];

    // allocate block memory.
    read->data = static_cast<Byte*>(mem::aligned_alloc(byte_block->size()));

    swapped_.erase(byte_block);
    --num_swapped_blocks_;

    LOGC(debug_em)
        << "BlockPool::PinBlock block=" << &block
        << " requested from external memory"
        << pin_count_;

    read->req =
        byte_block->em_bid_.storage->aread(
            read->data, byte_block->em_bid_.offset, byte_block->size(),
            [this, block, local_worker_id, read](
                io::Request* req, bool success) {
                return OnReadComplete(block, local_worker_id, read,
                                      req, success);
            });

    return read->result.get_future();
}

void BlockPool::OnReadComplete(
    const Block& block, size_t local_worker_id, ReadRequest* read,
    io::Request* req, bool success) {
    std::unique_lock<std::mutex> lock(mutex_);

    LOGC(debug_em)
        << "OnReadComplete(): " << req << " done, from "
        << block.byte_block()->em_bid_ << " success = " << success;
    die_unless(success);
    req->check_error();

    // assign data
    block.byte_block()->data_ = read->data;

    // set pin on ByteBlock
    IncBlockPinCountNoLock(block.byte_block(), local_worker_id);

    bm_->delete_block(block.byte_block()->em_bid_);
    block.byte_block()->em_bid_ = io::BID<0>();

    // deliver future
    read->result.set_value(PinnedBlock(block, local_worker_id));
}

//! Increment a ByteBlock's pin count
void BlockPool::IncBlockPinCount(ByteBlock* block_ptr, size_t local_worker_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    assert(local_worker_id < workers_per_host_);
    assert(block_ptr->pin_count_[local_worker_id] > 0);
    return IncBlockPinCountNoLock(block_ptr, local_worker_id);
}

//! Increment a ByteBlock's pin count
void BlockPool::IncBlockPinCountNoLock(ByteBlock* block_ptr, size_t local_worker_id) {
    assert(local_worker_id < workers_per_host_);

    ++block_ptr->pin_count_[local_worker_id];
    ++block_ptr->total_pins_;

    LOGC(debug_pin)
        << "BlockPool::IncBlockPinCount()"
        << " ++block.pin_count[" << local_worker_id << "]="
        << block_ptr->pin_count_[local_worker_id]
        << " ++block.total_pins_=" << block_ptr->total_pins_
        << pin_count_;
}

void BlockPool::DecBlockPinCount(ByteBlock* block_ptr, size_t local_worker_id) {
    std::unique_lock<std::mutex> lock(mutex_);

    assert(local_worker_id < workers_per_host_);
    assert(block_ptr->pin_count_[local_worker_id] > 0);
    assert(block_ptr->total_pins_ > 0);

    size_t p = --block_ptr->pin_count_[local_worker_id];
    size_t tp = --block_ptr->total_pins_;

    LOGC(debug_pin)
        << "BlockPool::DecBlockPinCount()"
        << " --block.pin_count[" << local_worker_id << "]=" << p
        << " --block.total_pins_=" << tp
        << " local_worker_id=" << local_worker_id;

    if (p == 0)
        UnpinBlock(block_ptr, local_worker_id);
}

void BlockPool::UnpinBlock(ByteBlock* block_ptr, size_t local_worker_id) {
    assert(local_worker_id < workers_per_host_);

    // decrease per-thread total pin count (memory locked by thread)
    assert(block_ptr->pin_count(local_worker_id) == 0);

    pin_count_.Decrement(local_worker_id, block_ptr->size());

    if (block_ptr->total_pins_ != 0) {
        LOGC(debug_pin)
            << "BlockPool::UnpinBlock()"
            << " --block.total_pins_=" << block_ptr->total_pins_;
        return;
    }

    // if all per-thread pins are zero, allow this Block to be swapped out.
    assert(!unpinned_blocks_.exists(block_ptr));
    unpinned_blocks_.put(block_ptr);

    LOGC(debug_pin)
        << "BlockPool::UnpinBlock()"
        << " --total_pins_=" << block_ptr->total_pins_
        << " allow swap out.";
}

size_t BlockPool::block_count() const noexcept {
    LOG0 << "BlockPool::block_count()"
         << pin_count_
         << " unpinned_blocks_" << unpinned_blocks_.size()
         << " num_swapped_blocks_" << num_swapped_blocks_;
    return pin_count_.total_pins_ + unpinned_blocks_.size() + num_swapped_blocks_;
}

void BlockPool::DestroyBlock(ByteBlock* block) {
    std::unique_lock<std::mutex> lock(mutex_);
    // this method is called by ByteBlockPtr's deleter when the reference
    // counter reaches zero to deallocate the block.

    // pinned blocks cannot be destroyed since they are always unpinned first
    assert(block->total_pins_ == 0);

    LOGC(debug_blc)
        << "BlockPool::DestroyBlock() block=" << block;

    if (block->in_memory())
    {
        // unpinned block in memory, remove from list
        assert(unpinned_blocks_.exists(block));
        unpinned_blocks_.erase(block);

        // release memory
        mem::aligned_dealloc(block->data_);
        block->data_ = nullptr;

        // page_mapper_.SwapOut(block->data_, false);
        // page_mapper_.ReleaseToken(block->swap_token_);
        ReleaseInternalMemory(block->size());
    }
    else
    {
        // block was evicted, may still be writing to EM.
        auto req = writing_.find(block);
        if (req != writing_.end()) {
            // TODO(tb): maybe cancel instead someday.
            req->second->wait();
            assert(writing_.find(block) == writing_.end());
        }

        auto it = swapped_.find(block);
        die_unless(it != swapped_.end());

        swapped_.erase(it);
        --num_swapped_blocks_;
    }
}

void BlockPool::RequestInternalMemory(
    std::unique_lock<std::mutex>& lock, size_t size) {

    requested_bytes_ += size;

    LOGC(debug_em)
        << "BlockPool::RequestInternalMemory()"
        << " size=" << size
        << " total_ram_use_=" << total_ram_use_
        << " writing_bytes_=" << writing_bytes_
        << " requested_bytes_=" << requested_bytes_
        << " soft_ram_limit_=" << soft_ram_limit_
        << " hard_ram_limit_=" << hard_ram_limit_
        << pin_count_;

    while (total_ram_use_ - writing_bytes_ + requested_bytes_ > soft_ram_limit_)
    {
        // evict blocks: schedule async writing which increases writing_bytes_.
        EvictBlock();
    }

    // wait for memory change due to blocks begin written and deallocated.
    memory_change_.wait(
        lock, [&]() {
            return total_ram_use_ + size <= hard_ram_limit_;
        });

    requested_bytes_ -= size;
    total_ram_use_ += size;
}

void BlockPool::ReleaseInternalMemory(size_t size) {

    assert(total_ram_use_ >= size);
    total_ram_use_ -= size;

    memory_change_.notify_all();
}

void BlockPool::EvictBlock() {

    assert(unpinned_blocks_.size());
    ByteBlock* block_ptr = unpinned_blocks_.pop();
    assert(block_ptr);

    // allocate EM block
    block_ptr->em_bid_.size = block_ptr->size();
    bm_->new_block(io::Striping(), block_ptr->em_bid_);

    LOGC(debug_em)
        << "EvictBlock(): " << block_ptr << " - " << *block_ptr
        << " to em_bid " << block_ptr->em_bid_;

    writing_bytes_ += block_ptr->size();

    // initiate writing to EM.
    writing_[block_ptr] =
        block_ptr->em_bid_.storage->awrite(
            block_ptr->data_, block_ptr->em_bid_.offset, block_ptr->size(),
            [this, block_ptr](io::Request* req, bool success) {
                return OnWriteComplete(block_ptr, req, success);
            });
}

void BlockPool::OnWriteComplete(
    ByteBlock* block_ptr, io::Request* req, bool success) {
    std::unique_lock<std::mutex> lock(mutex_);

    LOGC(debug_em)
        << "OnWriteComplete(): " << req
        << " done, to " << block_ptr->em_bid_ << " success = " << success;
    req->check_error();
    die_unless(success);

    writing_bytes_ -= block_ptr->size();
    writing_.erase(block_ptr);

    swapped_.insert(block_ptr);
    ++num_swapped_blocks_;

    // release memory
    mem::aligned_dealloc(block_ptr->data_);
    block_ptr->data_ = nullptr;

    ReleaseInternalMemory(block_ptr->size());
}

/******************************************************************************/
// BlockPool::PinCount

//! ctor: initializes vectors to correct size.
BlockPool::PinCount::PinCount(size_t workers_per_host)
    : pin_count_(workers_per_host),
      pinned_bytes_(workers_per_host) { }

void BlockPool::PinCount::Increment(size_t local_worker_id, size_t size) {
    ++pin_count_[local_worker_id];
    pinned_bytes_[local_worker_id] += size;
    ++total_pins_;
    total_pinned_bytes_ += size;
    max_pins = std::max(max_pins, total_pins_);
    max_pinned_bytes = std::max(max_pinned_bytes, total_pinned_bytes_);
}

void BlockPool::PinCount::Decrement(size_t local_worker_id, size_t size) {
    assert(pin_count_[local_worker_id] > 0);
    assert(pinned_bytes_[local_worker_id] >= size);
    assert(total_pins_ > 0);
    assert(total_pinned_bytes_ >= size);

    --pin_count_[local_worker_id];
    pinned_bytes_[local_worker_id] -= size;
    --total_pins_;
    total_pinned_bytes_ -= size;
}

void BlockPool::PinCount::AssertZero() const {
    assert(total_pins_ == 0);
    assert(total_pinned_bytes_ == 0);
    for (const size_t& pc : pin_count_)
        assert(pc == 0);
    for (const size_t& pb : pinned_bytes_)
        assert(pb == 0);
}

std::ostream& operator << (std::ostream& os, const BlockPool::PinCount& p) {
    os << " total_pins_=" << p.total_pins_
       << " total_pinned_bytes_=" << p.total_pinned_bytes_
       << " pin_count_=[" << common::Join(',', p.pin_count_) << "]"
       << " pinned_bytes_=[" << common::Join(',', p.pinned_bytes_) << "]";
    return os;
}

} // namespace data
} // namespace thrill

/******************************************************************************/
