/*******************************************************************************
 * thrill/data/block.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/data/block.hpp>
#include <thrill/data/block_pool.hpp>

namespace thrill {
namespace data {

std::future<PinnedBlock> Block::Pin(size_t local_worker_id) const {
    return byte_block()->block_pool_->PinBlock(*this, local_worker_id);
}

} // namespace data
} // namespace thrill

/******************************************************************************/
