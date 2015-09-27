/*******************************************************************************
 * thrill/core/bucket_block_pool.hpp
 *
 * BucketBlockPool to stack allocated BucketBlocks.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#ifndef THRILL_BUCKET_BLOCK_POOL_HPP
#define THRILL_BUCKET_BLOCK_POOL_HPP

#include <stack>
#include <thrill/common/functional.hpp>

namespace thrill {
namespace core {

template<typename BucketBlock>
class BucketBlockPool {

public:
    //constructor (we don't need to do anything here)
    BucketBlockPool() { }

    //delete the copy constructor; we can't copy it:
    BucketBlockPool(const BucketBlockPool &) = delete;

    //move constructor, so we can move it:
    BucketBlockPool(BucketBlockPool &&other) {
        this->free(std::move(other.free));
    }

    //allocate a chunk of memory as big as Type needs:
    BucketBlock* GetBlock() {
        BucketBlock* place;
        if (!free.empty()) {
            place = static_cast<BucketBlock*>(free.top());
            free.pop();
        }
        else {
            place = static_cast<BucketBlock*>(operator new(sizeof(BucketBlock)));
            place->size = 0;
            place->next = nullptr;
        }

        return place;
    }

    //mark some memory as available (no longer used):
    void Deallocate(BucketBlock *o) {
        o->size = 0;
        o->next = nullptr;
        free.push(static_cast<BucketBlock*>(o));
    }

    void Destroy() {
        while (!free.empty()) {
            free.top()->destroy_items();
            operator delete(free.top());
            free.pop();
        }
    }

    //delete all of the available memory chunks:
    ~BucketBlockPool() { }

private:

    //stack to hold pointers to free chunks:
    std::stack<BucketBlock*> free;
};

}
}

#endif //THRILL_BUCKET_BLOCK_POOL_HPP
