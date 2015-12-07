/*******************************************************************************
 * thrill/core/bucket_block_pool.hpp
 *
 * BucketBlockPool to stack allocated BucketBlocks.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_BUCKET_BLOCK_POOL_HEADER
#define THRILL_CORE_BUCKET_BLOCK_POOL_HEADER

#include <thrill/common/functional.hpp>

#include <stack>

namespace thrill {
namespace core {

template <typename BucketBlock>
class BucketBlockPool
{

public:
    // constructor (we don't need to do anything here)
    BucketBlockPool() { }

    // delete the copy constructor; we can't copy it:
    BucketBlockPool(const BucketBlockPool&) = delete;

    // move constructor, so we can move it:
    BucketBlockPool(BucketBlockPool&& other) {
        this->free(std::move(other.free));
    }

    // allocate a chunk of memory as big as Type needs:
    BucketBlock * GetBlock() {
        BucketBlock* place;
        if (!free.empty()) {
            place = static_cast<BucketBlock*>(free.top());
            free.pop();
        }
        else {
            place = static_cast<BucketBlock*>(operator new (sizeof(BucketBlock)));
            place->size = 0;
            place->next = nullptr;
        }

        return place;
    }

    // mark some memory as available (no longer used):
    void Deallocate(BucketBlock* o) {
        o->size = 0;
        o->next = nullptr;
        free.push(static_cast<BucketBlock*>(o));
    }

    void Destroy() {
        while (!free.empty()) {
            free.top()->destroy_items();
            operator delete (free.top());
            free.pop();
        }
    }

    // delete all of the available memory chunks:
    ~BucketBlockPool() { }

private:
    // stack to hold pointers to free chunks:
    std::stack<BucketBlock*> free;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_BUCKET_BLOCK_POOL_HEADER

/******************************************************************************/
