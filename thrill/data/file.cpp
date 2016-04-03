/*******************************************************************************
 * thrill/data/file.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/data/file.hpp>

#include <deque>
#include <string>

namespace thrill {
namespace data {

/******************************************************************************/
// File

File::File(BlockPool& block_pool, size_t local_worker_id, size_t dia_id)
    : BlockSink(block_pool, local_worker_id),
      id_(block_pool.next_file_id()), dia_id_(dia_id) { }

File::~File() {
    // assert(dia_id_ != 0);
    logger()
        << "class" << "File"
        << "event" << "close"
        << "id" << id_
        << "dia_id" << dia_id_
        << "items" << stats_items_
        << "bytes" << stats_bytes_;
}

File File::Copy() const {
    File f(*block_pool(), local_worker_id(), dia_id_);
    f.blocks_ = blocks_;
    f.num_items_sum_ = num_items_sum_;
    f.size_bytes_ = size_bytes_;
    f.stats_bytes_ = stats_bytes_;
    f.stats_items_ = stats_items_;
    return f;
}

void File::Close() {
    // 2016-02-04: Files are never closed, one can always append. This is
    // current used by the ReduceTables -tb.
}

void File::Clear() {
    std::deque<Block>().swap(blocks_);
    std::deque<size_t>().swap(num_items_sum_);
    size_bytes_ = 0;
}

File::Writer File::GetWriter(size_t block_size) {
    return Writer(this, block_size);
}

File::DynWriter File::GetDynWriter(size_t block_size) {
    return DynWriter(this, block_size);
}

File::KeepReader File::GetKeepReader(size_t num_prefetch) const {
    return KeepReader(
        KeepFileBlockSource(*this, local_worker_id_, num_prefetch));
}

File::ConsumeReader File::GetConsumeReader(size_t num_prefetch) {
    return ConsumeReader(
        ConsumeFileBlockSource(this, local_worker_id_, num_prefetch));
}

File::Reader File::GetReader(bool consume, size_t num_prefetch) {
    if (consume)
        return ConstructDynBlockReader<ConsumeFileBlockSource>(
            this, local_worker_id_, num_prefetch);
    else
        return ConstructDynBlockReader<KeepFileBlockSource>(
            *this, local_worker_id_, num_prefetch);
}

std::string File::ReadComplete() const {
    std::string output;
    for (const Block& b : blocks_)
        output += b.PinWait(0).ToString();
    return output;
}

std::ostream& operator << (std::ostream& os, const File& f) {
    os << "[File " << std::hex << &f << std::dec
       << " Blocks=[";
    size_t i = 0;
    for (const Block& b : f.blocks_)
        os << "\n    " << i++ << " " << b;
    return os << "]]";
}

/******************************************************************************/
// KeepFileBlockSource

KeepFileBlockSource::KeepFileBlockSource(
    const File& file, size_t local_worker_id,
    size_t num_prefetch,
    size_t first_block, size_t first_item)
    : file_(file), local_worker_id_(local_worker_id),
      num_prefetch_(num_prefetch),
      first_block_(first_block), current_block_(first_block),
      first_item_(first_item) { }

PinnedBlock KeepFileBlockSource::NextBlock() {

    if (current_block_ >= file_.num_blocks() && fetching_blocks_.empty())
        return PinnedBlock();

    if (num_prefetch_ == 0)
    {
        // operate without prefetching
        return NextUnpinnedBlock().PinWait(local_worker_id_);
    }
    else
    {
        // prefetch #desired blocks
        while (fetching_blocks_.size() < num_prefetch_ &&
               current_block_ < file_.num_blocks())
        {
            fetching_blocks_.emplace_back(
                NextUnpinnedBlock().Pin(local_worker_id_));
        }

        // this might block if the prefetching is not finished
        PinnedBlock b = fetching_blocks_.front()->Wait();
        fetching_blocks_.pop_front();
        return b;
    }
}

//! Determine current unpinned Block to deliver via NextBlock()
Block KeepFileBlockSource::NextUnpinnedBlock() {
    if (current_block_ == first_block_) {
        // construct first block differently, in case we want to shorten it.
        Block b = file_.block(current_block_++);
        if (first_item_ != keep_first_item)
            b.set_begin(first_item_);
        return b;
    }
    else {
        return file_.block(current_block_++);
    }
}

/******************************************************************************/
// ConsumeFileBlockSource

ConsumeFileBlockSource::ConsumeFileBlockSource(
    File* file, size_t local_worker_id, size_t num_prefetch)
    : file_(file), local_worker_id_(local_worker_id),
      num_prefetch_(num_prefetch) { Prefetch(num_prefetch_); }

ConsumeFileBlockSource::ConsumeFileBlockSource(ConsumeFileBlockSource&& s)
    : file_(s.file_), local_worker_id_(s.local_worker_id_),
      num_prefetch_(s.num_prefetch_),
      fetching_blocks_(std::move(s.fetching_blocks_)) {
    s.file_ = nullptr;
}

void ConsumeFileBlockSource::Prefetch(size_t prefetch) {
    if (prefetch >= num_prefetch_) {
        num_prefetch_ = prefetch;
        while (fetching_blocks_.size() < num_prefetch_ && !file_->blocks_.empty()) {
            fetching_blocks_.emplace_back(
                file_->blocks_.front().Pin(local_worker_id_));
            file_->blocks_.pop_front();
        }
    }
    else if (prefetch < num_prefetch_) {
        num_prefetch_ = prefetch;
        // cannot discard prefetched Blocks
    }
}

PinnedBlock ConsumeFileBlockSource::NextBlock() {
    assert(file_);
    if (file_->blocks_.empty() && fetching_blocks_.empty())
        return PinnedBlock();

    // operate without prefetching
    if (num_prefetch_ == 0) {
        data::PinRequestPtr f = file_->blocks_.front().Pin(local_worker_id_);
        file_->blocks_.pop_front();
        return f->Wait();
    }

    // prefetch #desired blocks
    while (fetching_blocks_.size() < num_prefetch_ && !file_->blocks_.empty()) {
        fetching_blocks_.emplace_back(
            file_->blocks_.front().Pin(local_worker_id_));
        file_->blocks_.pop_front();
    }

    // this might block if the prefetching is not finished
    PinnedBlock b = fetching_blocks_.front()->Wait();
    fetching_blocks_.pop_front();
    return b;
}

ConsumeFileBlockSource::~ConsumeFileBlockSource() {
    if (file_) {
        file_->Clear();
    }
}

} // namespace data
} // namespace thrill

/******************************************************************************/
