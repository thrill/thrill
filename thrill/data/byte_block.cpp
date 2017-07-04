/*******************************************************************************
 * thrill/data/byte_block.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/data/block_pool.hpp>
#include <thrill/data/byte_block.hpp>
#include <thrill/mem/pool.hpp>

#include <tlx/string/join_generic.hpp>

#include <sstream>
#include <string>

namespace thrill {
namespace data {

size_t start_block_size = 2 * 1024 * 1024;
size_t default_block_size = 2 * 1024 * 1024;

ByteBlock::ByteBlock(BlockPool* block_pool, Byte* data, size_t size)
    : data_(data), size_(size),
      block_pool_(block_pool),
      pin_count_(block_pool_->workers_per_host())
{ }

ByteBlock::ByteBlock(
    BlockPool* block_pool, const io::FileBasePtr& ext_file,
    int64_t offset, size_t size)
    : data_(nullptr), size_(size),
      block_pool_(block_pool),
      pin_count_(block_pool_->workers_per_host()),
      em_bid_(ext_file.get(), offset, size),
      ext_file_(ext_file)
{ }

void ByteBlock::Deleter::operator () (ByteBlock* bb) const {
    sLOG << "ByteBlock[" << bb << "]::deleter()"
         << "pin_count_" << bb->pin_count_str();
    assert(bb->total_pins_ == 0);
    assert(bb->reference_count() == 0);

    // call BlockPool's DestroyBlock() to de-register ByteBlock and free data
    assert(bb->block_pool_);
    bb->block_pool_->DestroyBlock(bb);

    sLOG << "ByteBlock[ " << bb << "]::destroy()";
    mem::GPool().destroy(bb);
}

void ByteBlock::Deleter::operator () (const ByteBlock* bb) const {
    return operator () (const_cast<ByteBlock*>(bb));
}

std::string ByteBlock::pin_count_str() const {
    return "[" + tlx::join(',', pin_count_) + "]";
}

void ByteBlock::IncPinCount(size_t local_worker_id) {
    return block_pool_->IncBlockPinCount(this, local_worker_id);
}

void ByteBlock::DecPinCount(size_t local_worker_id) {
    return block_pool_->DecBlockPinCount(this, local_worker_id);
}

void ByteBlock::OnWriteComplete(io::Request* req, bool success) {
    return block_pool_->OnWriteComplete(this, req, success);
}

std::ostream& operator << (std::ostream& os, const ByteBlock& b) {
    os << "[ByteBlock" << " " << &b
       << " size_=" << b.size_
       << " block_pool_=" << b.block_pool_
       << " total_pins_=" << b.total_pins_
       << " ext_file_=" << b.ext_file_;
    return os << "]";
}

} // namespace data
} // namespace thrill

/******************************************************************************/
