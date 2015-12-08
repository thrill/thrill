/*******************************************************************************
 * tests/io/block_manager_test.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/io/block_manager.hpp>
#include <thrill/io/request.hpp>
#include <thrill/io/request_operations.hpp>
#include <thrill/io/typed_block.hpp>
#include <thrill/mem/new_alloc.hpp>

#include <iostream>
#include <vector>

using namespace thrill;

#define BLOCK_SIZE (1024 * 512)

struct MyType
{
    int integer;
    // char chars[4];
    ~MyType() { }
};

struct my_handler
{
    void operator () (io::request* req) {
        LOG1 << req << " done, type=" << req->io_type();
    }
};
template class io::typed_block<BLOCK_SIZE, int>;    // forced instantiation
template class io::typed_block<BLOCK_SIZE, MyType>; // forced instantiation

TEST(BlockManager, Test1) {
    static const bool debug = true;

    using block_type = io::typed_block<BLOCK_SIZE, MyType>;

    LOG << sizeof(MyType) << " " << (BLOCK_SIZE % sizeof(MyType));
    LOG << sizeof(block_type) << " " << BLOCK_SIZE;

    const unsigned nblocks = 2;
    io::BIDArray<BLOCK_SIZE> bids(nblocks);
    std::vector<int> disks(nblocks, 2);
    io::request_ptr* reqs = new io::request_ptr[nblocks];
    io::block_manager* bm = io::block_manager::get_instance();
    bm->new_blocks(io::striping(), bids.begin(), bids.end());

    block_type* block = new block_type[2];
    LOG << std::hex;
    LOG << "Allocated block address    : " << (block);
    LOG << "Allocated block address + 1: " << (block + 1);
    LOG << std::dec;

    for (size_t i = 0; i < block_type::size; ++i)
    {
        block->elem_[i].integer = i;
        // memcpy (block->elem[i].chars, "STXXL", 4);
    }
    for (size_t i = 0; i < nblocks; ++i)
        reqs[i] = block->write(bids[i], my_handler());

    std::cout << "Waiting " << std::endl;
    wait_all(reqs, nblocks);

    for (size_t i = 0; i < nblocks; ++i)
    {
        reqs[i] = block->read(bids[i], my_handler());
        reqs[i]->wait();
        for (size_t j = 0; j < block_type::size; ++j)
        {
            die_unequal(j, static_cast<size_t>(block->elem_[j].integer));
        }
    }

    bm->delete_blocks(bids.begin(), bids.end());

    delete[] reqs;
    delete[] block;

#if 0
    // variable-size blocks, not supported currently

    BIDArray<0> vbids(nblocks);
    for (i = 0; i < nblocks; i++)
        vbids[i].size = 1024 + i;

    bm->new_blocks(striping(), vbids.begin(), vbids.end());

    for (i = 0; i < nblocks; i++)
        STXXL_MSG("Allocated block: offset=" << vbids[i].offset << ", size=" << vbids[i].size);

    bm->delete_blocks(vbids.begin(), vbids.end());
#endif
}

TEST(BlockManager, Test2) {

    using block_type = io::typed_block<128* 1024, double>;
    std::vector<block_type::bid_type> bids;
    std::vector<io::request_ptr> requests;
    io::block_manager* bm = io::block_manager::get_instance();
    bm->new_blocks<block_type>(32, io::striping(), std::back_inserter(bids));
    std::vector<block_type, mem::new_alloc<block_type> > blocks(32);
    for (size_t vIndex = 0; vIndex < 32; ++vIndex) {
        for (int vIndex2 = 0; vIndex2 < block_type::size; ++vIndex2) {
            blocks[vIndex][vIndex2] = vIndex2;
        }
    }
    for (size_t vIndex = 0; vIndex < 32; ++vIndex) {
        requests.push_back(blocks[vIndex].write(bids[vIndex]));
    }
    wait_all(requests.begin(), requests.end());
    bm->delete_blocks(bids.begin(), bids.end());
}

TEST(BlockManager, Test3) {
    static const bool debug = true;

    using block_type = io::typed_block<BLOCK_SIZE, int>;

    int64_t totalsize = 0;
    io::config* config = io::config::get_instance();

    for (size_t i = 0; i < config->disks_number(); ++i)
        totalsize += config->disk_size(i);

    size_t totalblocks = totalsize / block_type::raw_size;

    LOG << "external memory: " << totalsize << " bytes  ==  " << totalblocks << " blocks";

    io::BIDArray<BLOCK_SIZE> b5a(totalblocks / 5);
    io::BIDArray<BLOCK_SIZE> b5b(totalblocks / 5);
    io::BIDArray<BLOCK_SIZE> b5c(totalblocks / 5);
    io::BIDArray<BLOCK_SIZE> b5d(totalblocks / 5);
    io::BIDArray<BLOCK_SIZE> b2(totalblocks / 2);

    io::block_manager* bm = io::block_manager::get_instance();

    LOG << "get 4 x " << totalblocks / 5;
    bm->new_blocks(io::striping(), b5a.begin(), b5a.end());
    bm->new_blocks(io::striping(), b5b.begin(), b5b.end());
    bm->new_blocks(io::striping(), b5c.begin(), b5c.end());
    bm->new_blocks(io::striping(), b5d.begin(), b5d.end());

    LOG << "free 2 x " << totalblocks / 5;
    bm->delete_blocks(b5a.begin(), b5a.end());
    bm->delete_blocks(b5c.begin(), b5c.end());

    // the external memory should now be fragmented enough,
    // s.t. the following request needs to be split into smaller ones
    LOG << "get 1 x " << totalblocks / 2;
    bm->new_blocks(io::striping(), b2.begin(), b2.end());

    bm->delete_blocks(b5b.begin(), b5b.end());
    bm->delete_blocks(b5d.begin(), b5d.end());

    bm->delete_blocks(b2.begin(), b2.end());
}

/******************************************************************************/
