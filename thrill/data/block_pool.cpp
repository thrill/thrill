/*******************************************************************************
 * thrill/data/block_pool.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/logger.hpp>
#include <thrill/common/math.hpp>
#include <thrill/data/block.hpp>
#include <thrill/data/block_pool.hpp>
#include <thrill/io/file_base.hpp>
#include <thrill/io/iostats.hpp>
#include <thrill/mem/aligned_allocator.hpp>
#include <thrill/mem/pool.hpp>

#include <tlx/die.hpp>
#include <tlx/lru_cache.hpp>
#include <tlx/math/is_power_of_two.hpp>
#include <tlx/string/join_generic.hpp>

#include <algorithm>
#include <functional>
#include <iostream>
#include <limits>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace thrill {
namespace data {

//! debug block life cycle output: create, destroy
static constexpr bool debug_blc = false;

//! debug block pinning:
static constexpr bool debug_pin = false;

//! debug memory requests
static constexpr bool debug_mem = false;

//! debug block eviction: evict, write complete, read complete
static constexpr bool debug_em = false;

/******************************************************************************/
// std::new_handler() which gets called when malloc() returns nullptr

static std::recursive_mutex s_new_mutex;
static std::vector<BlockPool*> s_blockpools;

static std::atomic<bool> in_new_handler {
    false
};

static void OurNewHandler() {
    std::unique_lock<std::recursive_mutex> lock(s_new_mutex);
    if (in_new_handler) {
        printf("new handler called recursively! fixup using mem::Pool!\n");
        abort();
    }

    static bool s_notify_new_handler = false;
    if (!s_notify_new_handler) {
        fprintf(stderr, "Thrill: new_handler called! Program is out of C++ heap memory, trying to\n");
        fprintf(stderr, "Thrill: swap out Blocks to external memory. Check your program's memory usage.\n");
        in_new_handler = true;
    }

    static size_t s_iter = 0;
    io::RequestPtr req;

    // first try to find a handle to a currently being written block.
    for (size_t i = 0; i < s_blockpools.size(); ++i) {
        req = s_blockpools[s_iter]->GetAnyWriting();
        ++s_iter %= s_blockpools.size();
        if (req) break;
    }

    if (!req) {
        // if no writing active, evict a block
        for (size_t i = 0; i < s_blockpools.size(); ++i) {
            req = s_blockpools[s_iter]->EvictBlockLRU();
            ++s_iter %= s_blockpools.size();
            if (req) break;
        }
    }

    if (req) {
        req->wait();
        in_new_handler = false;
    }
    else {
        printf("new handler found no ByteBlock to evict.\n");
        for (size_t i = 0; i < s_blockpools.size(); ++i) {
            LOG1 << "BlockPool[" << i << "]"
                 << " total_blocks=" << s_blockpools[i]->total_blocks()
                 << " total_bytes=" << s_blockpools[i]->total_bytes()
                 << " pinned_blocks=" << s_blockpools[i]->pinned_blocks()
                 << " writing_blocks=" << s_blockpools[i]->writing_blocks()
                 << " swapped_blocks=" << s_blockpools[i]->swapped_blocks()
                 << " reading_blocks=" << s_blockpools[i]->reading_blocks();
        }
        mem::malloc_tracker_print_status();
        in_new_handler = false;

        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

/******************************************************************************/
// BlockPool::PinCount

struct BlockPool::Counter {
    //! current counter value
    size_t value = 0;
    //! maximum counter value since last stats read
    size_t hmax = 0;

    operator size_t () const { return value; }

    Counter& operator += (size_t v) {
        value += v;
        hmax = std::max(hmax, value);
        return *this;
    }
    Counter& operator -= (size_t v) {
        value -= v;
        return *this;
    }

    //! get last held max value and update to current
    size_t hmax_update() {
        size_t m = hmax;
        hmax = value;
        return m;
    }
};

struct BlockPool::PinCount {
    //! current total number of pins, where each thread pin counts
    //! individually.
    size_t              total_pins_ = 0;

    //! total number of bytes pinned.
    Counter             total_pinned_bytes_;

    //! maximum number of total pins
    size_t              max_pins = 0;

    //! maximum number of pinned bytes
    size_t              max_pinned_bytes = 0;

    //! number of pinned blocks per local worker id - this is used to count
    //! the amount of memory locked per thread.
    std::vector<size_t> pin_count_;

    //! number of bytes pinned per local worker id.
    std::vector<size_t> pinned_bytes_;

    //! ctor: initializes vectors to correct size.
    explicit PinCount(size_t workers_per_host);

    //! increment pin counter for thread_id by given size in bytes
    void Increment(size_t local_worker_id, size_t size);

    //! decrement pin counter for thread_id by given size in bytes
    void Decrement(size_t local_worker_id, size_t size);

    //! assert that it is zero.
    void AssertZero() const;
};

BlockPool::PinCount::PinCount(size_t workers_per_host)
    : pin_count_(workers_per_host),
      pinned_bytes_(workers_per_host) { }

void BlockPool::PinCount::Increment(size_t local_worker_id, size_t size) {
    ++pin_count_[local_worker_id];
    pinned_bytes_[local_worker_id] += size;
    ++total_pins_;
    total_pinned_bytes_ += size;
    max_pins = std::max(max_pins, total_pins_);
    max_pinned_bytes = std::max(max_pinned_bytes, total_pinned_bytes_.value);
}

void BlockPool::PinCount::Decrement(size_t local_worker_id, size_t size) {
    die_unless(pin_count_[local_worker_id] > 0);
    die_unless(pinned_bytes_[local_worker_id] >= size);
    die_unless(total_pins_ > 0);
    die_unless(total_pinned_bytes_ >= size);

    --pin_count_[local_worker_id];
    pinned_bytes_[local_worker_id] -= size;
    --total_pins_;
    total_pinned_bytes_ -= size;
}

void BlockPool::PinCount::AssertZero() const {
    die_unless(total_pins_ == 0);
    die_unless(total_pinned_bytes_ == 0);
    for (const size_t& pc : pin_count_)
        die_unless(pc == 0);
    for (const size_t& pb : pinned_bytes_)
        die_unless(pb == 0);
}

std::ostream& operator << (std::ostream& os, const BlockPool::PinCount& p) {
    os << " total_pins_=" << p.total_pins_
       << " total_pinned_bytes_=" << p.total_pinned_bytes_
       << " pin_count_=[" << tlx::join(',', p.pin_count_) << "]"
       << " pinned_bytes_=[" << tlx::join(',', p.pinned_bytes_) << "]"
       << " max_pin=" << p.max_pins
       << " max_pinned_bytes=" << p.max_pinned_bytes;
    return os;
}

/******************************************************************************/
// BlockPool::Data

//! type of set of ByteBlocks currently begin written to EM.
using WritingMap = std::unordered_map<
          ByteBlock*, io::RequestPtr,
          std::hash<ByteBlock*>, std::equal_to<>,
          mem::GPoolAllocator<std::pair<ByteBlock* const, io::RequestPtr> > >;

//! type of set of ByteBlocks currently begin read from EM.
using ReadingMap = std::unordered_map<
          ByteBlock*, PinRequestPtr,
          std::hash<ByteBlock*>, std::equal_to<>,
          mem::GPoolAllocator<
              std::pair<ByteBlock* const, PinRequestPtr> > >;

class BlockPool::Data
{
public:
    //! For waiting on hard memory limit
    std::condition_variable cv_memory_change_;

    //! Soft limit for the block pool, blocks will be written to disk if this
    //! limit is reached. 0 for no limit.
    size_t soft_ram_limit_;

    //! Hard limit for the block pool, memory requests will block if this limit
    //! is reached. 0 for no limit.
    size_t hard_ram_limit_;

    //! print a message on the first block evicted to external memory
    bool notify_em_used_ = false;

    //! list of all blocks that are _in_memory_ but are _not_ pinned.
    tlx::LruCacheSet<
        ByteBlock*, mem::GPoolAllocator<ByteBlock*> > unpinned_blocks_;

    //! set of ByteBlocks currently begin written to EM.
    WritingMap writing_;

    //! set of ByteBlocks currently begin read from EM.
    ReadingMap reading_;

    //! set of ByteBlock currently in EM.
    std::unordered_set<
        ByteBlock*, std::hash<ByteBlock*>, std::equal_to<>,
        mem::GPoolAllocator<ByteBlock*> > swapped_;

    //! I/O layer stats when BlockPool was created.
    io::StatsData io_stats_first_;

    //! I/O layer stats of previous profile tick
    io::StatsData io_stats_prev_;

    //! reference to io block manager
    io::BlockManager* bm_;

    //! Allocator for ByteBlocks such that they are aligned for faster
    //! I/O. Allocations are counted via mem_manager_.
    mem::AlignedAllocator<Byte, mem::Allocator<char> > aligned_alloc_;

    //! next unique File id
    std::atomic<size_t> next_file_id_ { 0 };

    //! number of unpinned bytes
    Counter unpinned_bytes_;

    //! pin counter class
    PinCount pin_count_;

    //! number of bytes currently begin requested from RAM.
    size_t requested_bytes_ = 0;

    //! number of bytes currently being written to EM.
    Counter writing_bytes_;

    //! total number of bytes in swapped blocks
    Counter swapped_bytes_;

    //! number of bytes currently being read from to EM.
    Counter reading_bytes_;

    //! total number of ByteBlocks allocated
    size_t total_byte_blocks_ = 0;

    //! condition variable to wait on for ByteBlock deallocation
    std::condition_variable cv_total_byte_blocks_;

    //! total number of bytes in all ByteBlocks (in memory or swapped)
    Counter total_bytes_;

    //! maximum number of bytes in all ByteBlocks (in memory or swapped)
    size_t max_total_bytes_ = 0;

    //! total number of bytes used in RAM by pinned and unpinned blocks, and
    //! also additionally reserved memory via BlockPoolMemoryHolder.
    Counter total_ram_bytes_;

    //! last time statistics where outputted
    std::chrono::steady_clock::time_point tp_last_
        = std::chrono::steady_clock::now();

public:
    Data(BlockPool& block_pool,
         size_t soft_ram_limit, size_t hard_ram_limit,
         size_t workers_per_host)
        : soft_ram_limit_(soft_ram_limit),
          hard_ram_limit_(hard_ram_limit),
          bm_(io::BlockManager::GetInstance()),
          aligned_alloc_(mem::Allocator<char>(block_pool.mem_manager_)),
          pin_count_(workers_per_host) { }

    //! Updates the memory manager for internal memory. If the hard limit is
    //! reached, the call is blocked intil memory is free'd
    void IntRequestInternalMemory(std::unique_lock<std::mutex>& lock, size_t size);

    //! Updates the memory manager for the internal memory, wakes up waiting
    //! BlockPool::RequestInternalMemory calls
    void IntReleaseInternalMemory(size_t size);

    //! Unpins a block. If all pins are removed, the block might be swapped.
    //! Returns immediately. Actual unpinning is async.
    void IntUnpinBlock(
        BlockPool& bp, ByteBlock* block_ptr, size_t local_worker_id);

    //! Evict a block from the lru list into external memory
    io::RequestPtr IntEvictBlockLRU();

    //! Evict a block into external memory. The block must be unpinned and not
    //! swapped.
    io::RequestPtr IntEvictBlock(ByteBlock* block_ptr);

    //! \name Block Statistics
    //! \{

    //! Total number of allocated blocks of this block pool
    size_t int_total_blocks()  noexcept;

    //! Total number of bytes allocated in blocks of this block pool
    size_t int_total_bytes()  noexcept;

    //! \}
};

/******************************************************************************/
// BlockPool

BlockPool::BlockPool(size_t workers_per_host)
    : BlockPool(0, 0, nullptr, nullptr, workers_per_host) { }

BlockPool::BlockPool(size_t soft_ram_limit, size_t hard_ram_limit,
                     common::JsonLogger* logger, mem::Manager* mem_manager,
                     size_t workers_per_host)
    : logger_(logger),
      mem_manager_(mem_manager, "BlockPool"),
      workers_per_host_(workers_per_host),
      d_(std::make_unique<Data>(
             *this, soft_ram_limit, hard_ram_limit, workers_per_host)) {

    die_unless(hard_ram_limit >= soft_ram_limit);
    {
        std::unique_lock<std::recursive_mutex> lock(s_new_mutex);
        // register BlockPool as method of OurNewHandler to free memory.
        s_blockpools.reserve(32);
        s_blockpools.push_back(this);

        std::set_new_handler(OurNewHandler);
    }

    d_->io_stats_first_ = d_->io_stats_prev_ =
                              io::StatsData(*io::Stats::GetInstance());

    logger_ << "class" << "BlockPool"
            << "event" << "create"
            << "soft_ram_limit" << soft_ram_limit
            << "hard_ram_limit" << hard_ram_limit;
}

BlockPool::~BlockPool() {
    std::unique_lock<std::mutex> lock(mutex_);

    // check that not writing any block.
    while (d_->writing_.begin() != d_->writing_.end()) {

        ByteBlock* block_ptr = d_->writing_.begin()->first;
        io::RequestPtr req = d_->writing_.begin()->second;

        LOGC(debug_em)
            << "BlockPool::~BlockPool() block=" << block_ptr
            << " is currently begin written to external memory, canceling.";

        lock.unlock();
        // cancel I/O request
        if (!req->cancel()) {

            LOGC(debug_em)
                << "BlockPool::~BlockPool() block=" << block_ptr
                << " is currently begin written to external memory,"
                << " cancel failed, waiting.";

            // must still wait for cancellation to complete and the I/O handler.
            req->wait();
        }
        lock.lock();

        LOGC(debug_em)
            << "BlockPool::PinBlock block=" << block_ptr
            << " is currently begin written to external memory,"
            << " cancel/wait done.";
    }

    die_unless(d_->writing_bytes_ == 0);

    // check that not reading any block.
    while (d_->reading_.begin() != d_->reading_.end()) {

        ByteBlock* block_ptr = d_->reading_.begin()->first;
        PinRequestPtr read = d_->reading_.begin()->second;

        LOGC(debug_em)
            << "BlockPool::~BlockPool() block=" << block_ptr
            << " is currently begin read from external memory, waiting.";

        lock.unlock();
        // wait for I/O request for completion and the I/O handler.
        read->req_->wait();
        lock.lock();
    }

    die_unless(d_->reading_bytes_ == 0);

    // wait for deletion of last ByteBlocks. this may actually be needed, when
    // the I/O handlers have been finished, and the corresponding references are
    // freed, but DestroyBlock() could not be called yet.
    d_->cv_total_byte_blocks_.wait(
        lock, [this]() { return d_->total_byte_blocks_ == 0; });

    d_->pin_count_.AssertZero();
    die_unequal(d_->total_ram_bytes_, 0u);
    die_unequal(d_->total_bytes_, 0u);
    die_unequal(d_->unpinned_blocks_.size(), 0u);

    LOGC(debug_pin)
        << "~BlockPool()"
        << " max_pin=" << d_->pin_count_.max_pins
        << " max_pinned_bytes=" << d_->pin_count_.max_pinned_bytes;

    logger_ << "class" << "BlockPool"
            << "event" << "destroy"
            << "max_pins" << d_->pin_count_.max_pins
            << "max_pinned_bytes" << d_->pin_count_.max_pinned_bytes;

    std::unique_lock<std::recursive_mutex> s_new_lock(s_new_mutex);
    s_blockpools.erase(
        std::find(s_blockpools.begin(), s_blockpools.end(), this));
}

PinnedByteBlockPtr
BlockPool::AllocateByteBlock(size_t size, size_t local_worker_id) {
    assert(local_worker_id < workers_per_host_);
    std::unique_lock<std::mutex> lock(mutex_);

    if (!(size % THRILL_DEFAULT_ALIGN == 0 && tlx::is_power_of_two(size))
        // make exception to block_size constraint for test programs, which use
        // irregular block sizes to check all corner cases
        && d_->hard_ram_limit_ != 0) {
        die("BlockPool: requested unaligned block_size=" << size << "." <<
            "ByteBlocks must be >= " << THRILL_DEFAULT_ALIGN << " and a power of two.");
    }

    d_->IntRequestInternalMemory(lock, size);

    // allocate block memory. -- unlock mutex for that time, since it may
    // require block eviction.
    lock.unlock();
    Byte* data = d_->aligned_alloc_.allocate(size);
    lock.lock();

    // create tlx::CountingPtr, no need for special make_shared()-equivalent
    PinnedByteBlockPtr block_ptr(
        mem::GPool().make<ByteBlock>(this, data, size), local_worker_id);
    ++d_->total_byte_blocks_;
    d_->total_bytes_ += size;
    d_->max_total_bytes_ = std::max(d_->max_total_bytes_, d_->total_bytes_.value);
    IntIncBlockPinCount(block_ptr.get(), local_worker_id);

    d_->pin_count_.Increment(local_worker_id, size);

    LOGC(debug_blc)
        << "BlockPool::AllocateBlock()"
        << " ptr=" << block_ptr.get()
        << " size=" << size
        << " local_worker_id=" << local_worker_id
        << " total_blocks()=" << d_->int_total_blocks()
        << " total_bytes()=" << d_->int_total_bytes()
        << d_->pin_count_;

    return block_ptr;
}

ByteBlockPtr BlockPool::MapExternalBlock(
    const io::FileBasePtr& file, int64_t offset, size_t size) {
    std::unique_lock<std::mutex> lock(mutex_);
    // create tlx::CountingPtr, no need for special make_shared()-equivalent
    ByteBlockPtr block_ptr(
        mem::GPool().make<ByteBlock>(this, file, offset, size));
    ++d_->total_byte_blocks_;
    d_->max_total_bytes_ = std::max(d_->max_total_bytes_, d_->total_bytes_.value);
    d_->total_bytes_ += size;

    LOGC(debug_blc)
        << "BlockPool::MapExternalBlock()"
        << " ptr=" << block_ptr.get()
        << " offset=" << offset
        << " size=" << size;

    return block_ptr;
}

//! Pins a block by swapping it in if required.
PinRequestPtr BlockPool::PinBlock(const Block& block, size_t local_worker_id) {
    assert(local_worker_id < workers_per_host_);
    std::unique_lock<std::mutex> lock(mutex_);

    ByteBlock* block_ptr = block.byte_block().get();

    if (block_ptr->pin_count_[local_worker_id] > 0) {
        // We may get a Block who's underlying is already pinned, since
        // PinnedBlock become Blocks when transfered between Files or delivered
        // via GetItemRange() or Scatter().

        die_unless(!d_->unpinned_blocks_.exists(block_ptr));
        die_unless(d_->reading_.find(block_ptr) == d_->reading_.end());

        LOGC(debug_pin)
            << "BlockPool::PinBlock block=" << &block
            << " already pinned by thread";

        IntIncBlockPinCount(block_ptr, local_worker_id);

        return PinRequestPtr(mem::GPool().make<PinRequest>(
                                 this, PinnedBlock(block, local_worker_id)));
    }

    if (block_ptr->total_pins_ > 0) {
        // This block was already pinned by another thread, hence we only need
        // to get a pin for the new thread.

        die_unless(!d_->unpinned_blocks_.exists(block_ptr));
        die_unless(d_->reading_.find(block_ptr) == d_->reading_.end());

        LOGC(debug_pin)
            << "BlockPool::PinBlock block=" << &block
            << " already pinned by another thread"
            << d_->pin_count_;

        IntIncBlockPinCount(block_ptr, local_worker_id);
        d_->pin_count_.Increment(local_worker_id, block_ptr->size());

        return PinRequestPtr(mem::GPool().make<PinRequest>(
                                 this, PinnedBlock(block, local_worker_id)));
    }

    // check that not writing the block.
    WritingMap::iterator write_it;
    while ((write_it = d_->writing_.find(block_ptr)) != d_->writing_.end()) {

        LOGC(debug_em)
            << "BlockPool::PinBlock() block=" << block_ptr
            << " is currently begin written to external memory, canceling.";

        die_unless(!block_ptr->ext_file_);

        // get reference count to request, since complete handler removes it
        // from the map.
        io::RequestPtr req = write_it->second;
        lock.unlock();
        // cancel I/O request
        if (!req->cancel()) {

            LOGC(debug_em)
                << "BlockPool::PinBlock() block=" << block_ptr
                << " is currently begin written to external memory, "
                << "cancel failed, waiting.";

            // must still wait for cancellation to complete and the I/O
            // handler.
            req->wait();
        }
        lock.lock();

        LOGC(debug_em)
            << "BlockPool::PinBlock() block=" << block_ptr
            << " is currently begin written to external memory, "
            << "cancel/wait done.";

        // recheck whether block is being written, it may have been evicting
        // the unlocked time.
    }

    // check if block is being loaded. in this case, just deliver the
    // shared_future.
    ReadingMap::iterator read_it = d_->reading_.find(block_ptr);
    if (read_it != d_->reading_.end())
        return read_it->second;

    if (block_ptr->in_memory())
    {
        // unpinned block in memory, no need to load from EM.

        // remove from unpinned list
        die_unless(d_->unpinned_blocks_.exists(block_ptr));
        d_->unpinned_blocks_.erase(block_ptr);
        d_->unpinned_bytes_ -= block_ptr->size();

        IntIncBlockPinCount(block_ptr, local_worker_id);
        d_->pin_count_.Increment(local_worker_id, block_ptr->size());

        LOGC(debug_pin)
            << "BlockPool::PinBlock block=" << &block
            << " pinned from internal memory"
            << d_->pin_count_;

        return PinRequestPtr(mem::GPool().make<PinRequest>(
                                 this, PinnedBlock(block, local_worker_id)));
    }

    // else need to initiate an async read to get the data.

    die_unless(block_ptr->em_bid_.storage);

    // maybe blocking call until memory is available, this also swaps out other
    // blocks.
    d_->IntRequestInternalMemory(lock, block_ptr->size());

    // the requested memory is already counted as a pin.
    d_->pin_count_.Increment(local_worker_id, block_ptr->size());

    // initiate reading from EM -- already create PinnedBlock, which will hold
    // the read data
    PinRequestPtr read(
        mem::GPool().make<PinRequest>(
            this, PinnedBlock(block, local_worker_id), /* ready */ false));
    d_->reading_[block_ptr] = read;

    // allocate block memory.
    lock.unlock();
    Byte* data = read->byte_block()->data_ =
                     d_->aligned_alloc_.allocate(block_ptr->size());
    lock.lock();

    if (!block_ptr->ext_file_) {
        d_->swapped_.erase(block_ptr);
        d_->swapped_bytes_ -= block_ptr->size();
    }

    LOGC(debug_em)
        << "BlockPool::PinBlock block=" << &block
        << " requested from external memory"
        << d_->pin_count_;

    // issue I/O request, hold the reference to the request in the hashmap
    read->req_ =
        block_ptr->em_bid_.storage->aread(
            // parameters for the read
            data, block_ptr->em_bid_.offset, block_ptr->size(),
            // construct an immediate CompletionHandler callback
            io::CompletionHandler::make<
                PinRequest, &PinRequest::OnComplete>(*read));

    d_->reading_bytes_ += block_ptr->size();

    return read;
}

void PinRequest::OnComplete(io::Request* req, bool success) {
    return block_pool_->OnReadComplete(this, req, success);
}

void BlockPool::OnReadComplete(
    PinRequest* read, io::Request* req, bool success) {
    std::unique_lock<std::mutex> lock(mutex_);

    ByteBlock* block_ptr = read->block_.byte_block().get();
    size_t block_size = block_ptr->size();

    LOGC(debug_em)
        << "OnReadComplete():"
        << " req " << req << " block " << block_ptr
        << " size " << block_size << " done,"
        << " from " << block_ptr->em_bid_ << " success = " << success;
    req->check_error();

    if (!success)
    {
        // request was canceled. this is not an I/O error, but intentional,
        // e.g. because the Block was deleted.

        if (!block_ptr->ext_file_) {
            d_->swapped_.insert(block_ptr);
            d_->swapped_bytes_ += block_size;
        }

        // release memory
        d_->aligned_alloc_.deallocate(read->byte_block()->data_, block_size);

        d_->IntReleaseInternalMemory(block_size);

        // the requested memory was already counted as a pin.
        d_->pin_count_.Decrement(read->block_.local_worker_id_, block_size);

        // set delivered PinnedBlock as invalid.
        read->byte_block().reset();
    }
    else    // success
    {
        // set pin on ByteBlock
        IntIncBlockPinCount(block_ptr, read->block_.local_worker_id_);

        if (!block_ptr->ext_file_) {
            d_->bm_->delete_block(block_ptr->em_bid_);
            block_ptr->em_bid_ = io::BID<0>();
        }
    }

    read->ready_ = true;
    d_->reading_bytes_ -= block_size;
    cv_read_complete_.notify_all();

    // remove the PinRequest from the hash map. The problem here is that the
    // PinRequestPtr may have been discarded (the Pin wasn't needed after
    // all). In that case, deletion of PinRequest will call Unpin, which creates
    // a deadlock on the mutex_. Hence, we first move the PinRequest out of the
    // map, then unlock, and delete it. -tb
    auto it = d_->reading_.find(block_ptr);
    die_unless(it != d_->reading_.end());
    PinRequestPtr holder = std::move(it->second);
    d_->reading_.erase(it);
    lock.unlock();
}

void BlockPool::IncBlockPinCount(ByteBlock* block_ptr, size_t local_worker_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    assert(local_worker_id < workers_per_host_);
    die_unless(block_ptr->pin_count_[local_worker_id] > 0);
    return IntIncBlockPinCount(block_ptr, local_worker_id);
}

void BlockPool::IntIncBlockPinCount(ByteBlock* block_ptr, size_t local_worker_id) {
    assert(local_worker_id < workers_per_host_);

    ++block_ptr->pin_count_[local_worker_id];
    ++block_ptr->total_pins_;

    LOGC(debug_pin)
        << "BlockPool::IncBlockPinCount()"
        << " block=" << block_ptr
        << " ++block.pin_count[" << local_worker_id << "]="
        << block_ptr->pin_count_[local_worker_id]
        << " ++block.total_pins_=" << block_ptr->total_pins_
        << d_->pin_count_;
}

void BlockPool::DecBlockPinCount(ByteBlock* block_ptr, size_t local_worker_id) {
    std::unique_lock<std::mutex> lock(mutex_);

    assert(local_worker_id < workers_per_host_);
    die_unless(block_ptr->pin_count_[local_worker_id] > 0);
    die_unless(block_ptr->total_pins_ > 0);

    size_t p = --block_ptr->pin_count_[local_worker_id];
    size_t tp = --block_ptr->total_pins_;

    LOGC(debug_pin)
        << "BlockPool::DecBlockPinCount()"
        << " block=" << block_ptr
        << " --block.pin_count[" << local_worker_id << "]=" << p
        << " --block.total_pins_=" << tp
        << " local_worker_id=" << local_worker_id;

    if (p == 0)
        d_->IntUnpinBlock(*this, block_ptr, local_worker_id);
}

void BlockPool::Data::IntUnpinBlock(
    BlockPool& bp, ByteBlock* block_ptr, size_t local_worker_id) {
    die_unless(local_worker_id < bp.workers_per_host_);

    // decrease per-thread total pin count (memory locked by thread)
    die_unless(block_ptr->pin_count(local_worker_id) == 0);

    pin_count_.Decrement(local_worker_id, block_ptr->size());

    if (block_ptr->total_pins_ != 0) {
        LOGC(debug_pin)
            << "BlockPool::IntUnpinBlock()"
            << " --block.total_pins_=" << block_ptr->total_pins_;
        return;
    }

    // if all per-thread pins are zero, allow this Block to be swapped out.
    die_unless(!unpinned_blocks_.exists(block_ptr));
    unpinned_blocks_.put(block_ptr);
    unpinned_bytes_ += block_ptr->size();

    LOGC(debug_pin)
        << "BlockPool::IntUnpinBlock()"
        << " block=" << block_ptr
        << " --total_pins_=" << block_ptr->total_pins_
        << " allow swap out.";
}

size_t BlockPool::total_blocks() noexcept {
    std::unique_lock<std::mutex> lock(mutex_);
    return d_->int_total_blocks();
}

size_t BlockPool::Data::int_total_blocks() noexcept {

    LOG << "BlockPool::total_blocks()"
        << " pinned_blocks_=" << pin_count_.total_pins_
        << " unpinned_blocks_=" << unpinned_blocks_.size()
        << " writing_.size()=" << writing_.size()
        << " swapped_.size()=" << swapped_.size()
        << " reading_.size()=" << reading_.size();

    return pin_count_.total_pins_
           + unpinned_blocks_.size() + writing_.size()
           + swapped_.size() + reading_.size();
}

size_t BlockPool::hard_ram_limit() noexcept {
    std::unique_lock<std::mutex> lock(mutex_);
    return d_->hard_ram_limit_;
}

size_t BlockPool::total_bytes() noexcept {
    std::unique_lock<std::mutex> lock(mutex_);
    return d_->int_total_bytes();
}

size_t BlockPool::max_total_bytes() noexcept {
    std::unique_lock<std::mutex> lock(mutex_);
    return d_->max_total_bytes_;
}

size_t BlockPool::Data::int_total_bytes() noexcept {
    LOG << "BlockPool::total_bytes()"
        << " pinned_bytes_=" << pin_count_.total_pinned_bytes_
        << " unpinned_bytes_=" << unpinned_bytes_
        << " writing_bytes_=" << writing_bytes_
        << " swapped_bytes_=" << swapped_bytes_
        << " reading_bytes_=" << reading_bytes_;

    return pin_count_.total_pinned_bytes_
           + unpinned_bytes_ + writing_bytes_
           + swapped_bytes_ + reading_bytes_;
}

size_t BlockPool::pinned_blocks() noexcept {
    std::unique_lock<std::mutex> lock(mutex_);
    return d_->pin_count_.total_pins_;
}

size_t BlockPool::unpinned_blocks() noexcept {
    std::unique_lock<std::mutex> lock(mutex_);
    return d_->unpinned_blocks_.size();
}

size_t BlockPool::writing_blocks() noexcept {
    std::unique_lock<std::mutex> lock(mutex_);
    return d_->writing_.size();
}

size_t BlockPool::swapped_blocks() noexcept {
    std::unique_lock<std::mutex> lock(mutex_);
    return d_->swapped_.size();
}

size_t BlockPool::reading_blocks() noexcept {
    std::unique_lock<std::mutex> lock(mutex_);
    return d_->reading_.size();
}

void BlockPool::DestroyBlock(ByteBlock* block_ptr) {
    LOGC(debug_blc)
        << "BlockPool::DestroyBlock() block_ptr=" << block_ptr
        << " byte_block=" << *block_ptr;

    std::unique_lock<std::mutex> lock(mutex_);
    // this method is called by ByteBlockPtr's deleter when the reference
    // counter reaches zero to deallocate the block.

    // pinned blocks cannot be destroyed since they are always unpinned first
    die_unless(block_ptr->total_pins_ == 0);

    do {
        if (block_ptr->in_memory())
        {
            // block was evicted, may still be writing to EM.
            WritingMap::iterator it = d_->writing_.find(block_ptr);
            if (it != d_->writing_.end()) {
                // get reference count to request, since complete handler
                // removes it from the map.
                io::RequestPtr req = it->second;
                lock.unlock();
                // cancel I/O request
                if (!req->cancel()) {
                    // must still wait for cancellation to complete and the I/O
                    // handler.
                    req->wait();
                }
                lock.lock();

                // recheck whether block is being written, it may have been
                // evicting the unlocked time.
                continue;
            }
        }
        else
        {
            // block was being pinned. cancel read operation
            ReadingMap::iterator it = d_->reading_.find(block_ptr);
            if (it != d_->reading_.end()) {
                // get reference count to request, since complete handler
                // removes it from the map.
                io::RequestPtr req = it->second->req_;
                lock.unlock();
                // cancel I/O request
                if (!req->cancel()) {
                    // must still wait for cancellation to complete and the I/O
                    // handler.
                    req->wait();
                }
                lock.lock();

                // recheck whether block is being read, it may have been
                // evicting again in the unlocked time.
                continue;
            }
        }
    }
    while (0); // NOLINT

    if (block_ptr->ext_file_ && block_ptr->in_memory())
    {
        LOGC(debug_blc)
            << "BlockPool::DestroyBlock() block_ptr=" << block_ptr
            << " external block, in memory: release memory.";

        die_unless(d_->unpinned_blocks_.exists(block_ptr));
        d_->unpinned_blocks_.erase(block_ptr);
        d_->unpinned_bytes_ -= block_ptr->size();

        // release memory
        d_->aligned_alloc_.deallocate(block_ptr->data_, block_ptr->size());
        block_ptr->data_ = nullptr;

        d_->IntReleaseInternalMemory(block_ptr->size());
    }
    else if (block_ptr->ext_file_)
    {
        LOGC(debug_blc)
            << "BlockPool::DestroyBlock() block_ptr=" << block_ptr
            << " external block, but not in memory: nothing to do, thus just"
            << " delete the reference";
    }
    else if (block_ptr->in_memory())
    {
        LOGC(debug_blc)
            << "BlockPool::DestroyBlock() block_ptr=" << block_ptr
            << " unpinned block in memory, remove from list";

        die_unless(d_->unpinned_blocks_.exists(block_ptr));
        d_->unpinned_blocks_.erase(block_ptr);
        d_->unpinned_bytes_ -= block_ptr->size();

        // release memory
        d_->aligned_alloc_.deallocate(block_ptr->data_, block_ptr->size());
        block_ptr->data_ = nullptr;

        d_->IntReleaseInternalMemory(block_ptr->size());
    }
    else
    {
        LOGC(debug_blc)
            << "BlockPool::DestroyBlock() block_ptr=" << block_ptr
            << " block in external memory, delete block";

        auto it = d_->swapped_.find(block_ptr);
        die_unless(it != d_->swapped_.end());

        d_->swapped_.erase(it);
        d_->swapped_bytes_ -= block_ptr->size();

        d_->bm_->delete_block(block_ptr->em_bid_);
        block_ptr->em_bid_ = io::BID<0>();
    }

    assert(d_->total_byte_blocks_ > 0);
    assert(d_->total_bytes_ >= block_ptr->size());
    --d_->total_byte_blocks_;
    d_->total_bytes_ -= block_ptr->size();
    d_->cv_total_byte_blocks_.notify_all();
}

void BlockPool::RequestInternalMemory(size_t size) {
    std::unique_lock<std::mutex> lock(mutex_);
    return d_->IntRequestInternalMemory(lock, size);
}

void BlockPool::Data::IntRequestInternalMemory(
    std::unique_lock<std::mutex>& lock, size_t size) {

    requested_bytes_ += size;

    LOGC(debug_mem)
        << "BlockPool::RequestInternalMemory()"
        << " size=" << size
        << " total_ram_bytes_=" << total_ram_bytes_
        << " writing_bytes_=" << writing_bytes_
        << " requested_bytes_=" << requested_bytes_
        << " soft_ram_limit_=" << soft_ram_limit_
        << " hard_ram_limit_=" << hard_ram_limit_
        << pin_count_
        << " unpinned_blocks_.size()=" << unpinned_blocks_.size()
        << " swapped_.size()=" << swapped_.size();

    while (soft_ram_limit_ != 0 &&
           unpinned_blocks_.size() &&
           total_ram_bytes_ + requested_bytes_ > soft_ram_limit_ + writing_bytes_)
    {
        // evict blocks: schedule async writing which increases writing_bytes_.
        IntEvictBlockLRU();
    }

    // wait up to 60 seconds for other threads to free up memory or pins
    static constexpr size_t max_retry = 60;
    size_t retry = max_retry;
    size_t last_writing_bytes = 0;

    // wait for memory change due to blocks begin written and deallocated.
    while (hard_ram_limit_ != 0 && total_ram_bytes_ + size > hard_ram_limit_)
    {
        while (hard_ram_limit_ != 0 &&
               unpinned_blocks_.size() &&
               total_ram_bytes_ + requested_bytes_ > hard_ram_limit_ + writing_bytes_)
        {
            // evict blocks: schedule async writing which increases writing_bytes_.
            IntEvictBlockLRU();
        }

        cv_memory_change_.wait_for(lock, std::chrono::seconds(1));

        LOGC(debug_mem)
            << "BlockPool::RequestInternalMemory() waiting for memory"
            << " total_ram_bytes_=" << total_ram_bytes_
            << " writing_bytes_=" << writing_bytes_
            << " requested_bytes_=" << requested_bytes_
            << " soft_ram_limit_=" << soft_ram_limit_
            << " hard_ram_limit_=" << hard_ram_limit_
            << pin_count_
            << " unpinned_blocks_.size()=" << unpinned_blocks_.size()
            << " swapped_.size()=" << swapped_.size();

        if (writing_bytes_ == 0 &&
            total_ram_bytes_ + requested_bytes_ > hard_ram_limit_) {

            LOG1 << "abort() due to out-of-pinned-memory ???"
                 << " total_ram_bytes_=" << total_ram_bytes_
                 << " writing_bytes_=" << writing_bytes_
                 << " requested_bytes_=" << requested_bytes_
                 << " soft_ram_limit_=" << soft_ram_limit_
                 << " hard_ram_limit_=" << hard_ram_limit_
                 << pin_count_
                 << " unpinned_blocks_.size()=" << unpinned_blocks_.size()
                 << " swapped_.size()=" << swapped_.size();

            if (writing_bytes_ == last_writing_bytes) {
                if (--retry == 0)
                    abort();
            }
            else {
                last_writing_bytes = writing_bytes_;
                retry = max_retry;
            }
        }
    }

    requested_bytes_ -= size;
    total_ram_bytes_ += size;
}

void BlockPool::AdviseFree(size_t size) {
    std::unique_lock<std::mutex> lock(mutex_);

    LOGC(debug_mem)
        << "BlockPool::AdviseFree() advice to free memory"
        << " size=" << size
        << " total_ram_bytes_=" << d_->total_ram_bytes_
        << " writing_bytes_=" << d_->writing_bytes_
        << " requested_bytes_=" << d_->requested_bytes_
        << " soft_ram_limit_=" << d_->soft_ram_limit_
        << " hard_ram_limit_=" << d_->hard_ram_limit_
        << d_->pin_count_
        << " unpinned_blocks_.size()=" << d_->unpinned_blocks_.size()
        << " swapped_.size()=" << d_->swapped_.size();

    while (d_->soft_ram_limit_ != 0 && d_->unpinned_blocks_.size() &&
           d_->total_ram_bytes_ + d_->requested_bytes_ + size > d_->hard_ram_limit_ + d_->writing_bytes_)
    {
        // evict blocks: schedule async writing which increases writing_bytes_.
        d_->IntEvictBlockLRU();
    }
}
void BlockPool::ReleaseInternalMemory(size_t size) {
    std::unique_lock<std::mutex> lock(mutex_);
    return d_->IntReleaseInternalMemory(size);
}

void BlockPool::Data::IntReleaseInternalMemory(size_t size) {

    LOGC(debug_mem)
        << "BlockPool::IntReleaseInternalMemory()"
        << " size=" << size
        << " total_ram_bytes_=" << total_ram_bytes_;

    die_unless(total_ram_bytes_ >= size);
    total_ram_bytes_ -= size;

    cv_memory_change_.notify_all();
}

void BlockPool::EvictBlock(ByteBlock* block_ptr) {
    std::unique_lock<std::mutex> lock(mutex_);

    die_unless(block_ptr->in_memory());

    die_unless(d_->unpinned_blocks_.exists(block_ptr));
    d_->unpinned_blocks_.erase(block_ptr);
    d_->unpinned_bytes_ -= block_ptr->size();

    d_->IntEvictBlock(block_ptr);
}

io::RequestPtr BlockPool::GetAnyWriting() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!d_->writing_.size()) return io::RequestPtr();
    return d_->writing_.begin()->second;
}

io::RequestPtr BlockPool::EvictBlockLRU() {
    std::unique_lock<std::mutex> lock(mutex_);
    return d_->IntEvictBlockLRU();
}

io::RequestPtr BlockPool::Data::IntEvictBlockLRU() {

    if (!unpinned_blocks_.size()) return io::RequestPtr();

    ByteBlock* block_ptr = unpinned_blocks_.pop();
    die_unless(block_ptr);
    unpinned_bytes_ -= block_ptr->size();

    return IntEvictBlock(block_ptr);
}

io::RequestPtr BlockPool::Data::IntEvictBlock(ByteBlock* block_ptr) {

    // die_unless(block_ptr->block_pool_ == this);

    if (block_ptr->ext_file_) {
        // if in external file -> free memory without writing

        LOGC(debug_em)
            << "EvictBlock(): " << block_ptr << " - " << *block_ptr
            << " from ext_file " << block_ptr->ext_file_;

        // release memory
        aligned_alloc_.deallocate(block_ptr->data_, block_ptr->size());
        block_ptr->data_ = nullptr;

        IntReleaseInternalMemory(block_ptr->size());
        return io::RequestPtr();
    }

    if (!notify_em_used_) {
        std::cerr << "Thrill: evicting first Block to external memory. "
            "Be aware, that unexpected" << std::endl;
        std::cerr << "Thrill: use of external memory may lead to "
            "disappointingly slow performance." << std::endl;
        notify_em_used_ = true;
    }

    die_unless(block_ptr->em_bid_.storage == nullptr);

    // allocate EM block
    block_ptr->em_bid_.size = block_ptr->size();
    bm_->new_block(io::FullyRandom(), block_ptr->em_bid_);

    LOGC(debug_em)
        << "EvictBlock(): " << block_ptr << " - " << *block_ptr
        << " to em_bid " << block_ptr->em_bid_;

    writing_bytes_ += block_ptr->size();

    // initiate writing to EM.
    io::RequestPtr req =
        block_ptr->em_bid_.storage->awrite(
            block_ptr->data_, block_ptr->em_bid_.offset, block_ptr->size(),
            // construct an immediate CompletionHandler callback
            io::CompletionHandler::make<
                ByteBlock, &ByteBlock::OnWriteComplete>(block_ptr));

    return (writing_[block_ptr] = std::move(req));
}

void BlockPool::OnWriteComplete(
    ByteBlock* block_ptr, io::Request* req, bool success) {
    std::unique_lock<std::mutex> lock(mutex_);

    LOGC(debug_em)
        << "OnWriteComplete(): " << req
        << " done, to " << block_ptr->em_bid_ << " success = " << success;
    req->check_error();

    die_unless(!block_ptr->ext_file_);
    die_unequal(d_->writing_.erase(block_ptr), 1u);
    d_->writing_bytes_ -= block_ptr->size();

    if (!success)
    {
        // request was canceled. this is not an I/O error, but intentional,
        // e.g. because the block was deleted.

        die_unless(!d_->unpinned_blocks_.exists(block_ptr));
        d_->unpinned_blocks_.put(block_ptr);
        d_->unpinned_bytes_ += block_ptr->size();

        d_->bm_->delete_block(block_ptr->em_bid_);
        block_ptr->em_bid_ = io::BID<0>();
    }
    else    // success
    {
        d_->swapped_.insert(block_ptr);
        d_->swapped_bytes_ += block_ptr->size();

        // release memory
        d_->aligned_alloc_.deallocate(block_ptr->data_, block_ptr->size());
        block_ptr->data_ = nullptr;

        d_->IntReleaseInternalMemory(block_ptr->size());
    }
}

void BlockPool::RunTask(const std::chrono::steady_clock::time_point& tp) {
    std::unique_lock<std::mutex> lock(mutex_);

    io::StatsData stnow(*io::Stats::GetInstance());
    io::StatsData stf = stnow - d_->io_stats_first_;
    io::StatsData stp = stnow - d_->io_stats_prev_;
    d_->io_stats_prev_ = stnow;

    double elapsed = static_cast<double>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            tp - d_->tp_last_).count()) / 1e6;
    d_->tp_last_ = tp;

    // LOG0 << stp;
    // LOG0 << stf;

    size_t unpinned_bytes = d_->unpinned_bytes_.hmax_update();
    size_t writing_bytes = d_->writing_bytes_.hmax_update();
    size_t reading_bytes = d_->reading_bytes_.hmax_update();
    size_t pinned_bytes = d_->pin_count_.total_pinned_bytes_.hmax_update();

    logger_ << "class" << "BlockPool"
            << "event" << "profile"
            << "total_blocks" << d_->int_total_blocks()
            << "total_bytes" << d_->total_bytes_.hmax_update()
            << "max_total_bytes" << d_->max_total_bytes_
            << "total_ram_bytes" << d_->total_ram_bytes_.hmax_update()
            << "ram_bytes"
            << (unpinned_bytes + pinned_bytes + writing_bytes + reading_bytes)
            << "pinned_blocks" << d_->pin_count_.total_pins_
            << "pinned_bytes" << pinned_bytes
            << "unpinned_blocks" << d_->unpinned_blocks_.size()
            << "unpinned_bytes" << unpinned_bytes
            << "swapped_blocks" << d_->swapped_.size()
            << "swapped_bytes" << d_->swapped_bytes_.hmax_update()
            << "max_pinned_blocks" << d_->pin_count_.max_pins
            << "max_pinned_bytes" << d_->pin_count_.max_pinned_bytes
            << "writing_blocks" << d_->writing_.size()
            << "writing_bytes" << writing_bytes
            << "reading_blocks" << d_->reading_.size()
            << "reading_bytes" << reading_bytes
            << "rd_ops_total" << stf.read_ops()
            << "rd_bytes_total" << stf.read_volume()
            << "wr_ops_total" << stf.write_ops()
            << "wr_bytes_total" << stf.write_volume()
            << "rd_ops" << stp.read_ops()
            << "rd_bytes" << stp.read_volume()
            << "rd_speed" << static_cast<double>(stp.read_volume()) / elapsed
            << "wr_ops" << stp.write_ops()
            << "wr_bytes" << stp.write_volume()
            << "wr_speed" << static_cast<double>(stp.write_volume()) / elapsed
            << "disk_allocation" << d_->bm_->current_allocation();
}

size_t BlockPool::next_file_id() {
    return ++d_->next_file_id_;
}

} // namespace data
} // namespace thrill

/******************************************************************************/
