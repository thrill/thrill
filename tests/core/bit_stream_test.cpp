/*******************************************************************************
 * tests/core/bit_stream_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>

#include <thrill/common/logger.hpp>
#include <thrill/core/bit_stream.hpp>
#include <thrill/core/golomb_bit_stream.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <cmath>
#include <string>
#include <utility>
#include <vector>

using namespace thrill;

TEST(BitStreamWriter, Test) {

    data::BlockPool block_pool_;

    // construct File with very small blocks for testing
    data::File file(block_pool_, 0, /* dia_id */ 0);

    {
        data::File::Writer fw = file.GetWriter(16);
        core::BitStreamWriter<data::File::Writer> bsw(fw);

        bsw.PutBits(5, 4);
        bsw.PutBits(42, 6);
        bsw.PutBits(0xC0FFEE, 24);
        bsw.PutBits(0xC0FFEE, 32);
    }

    // BitStreamWriter stores in size_t items
    ASSERT_EQ(16u, file.size_bytes());

    const unsigned char file_bytes[] = {
        0xFB, 0x3F, 0x30, 0x80, 0xFB, 0x3F, 0xB0, 0x5A,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80
    };

    std::string file_data(reinterpret_cast<const char*>(file_bytes),
                          sizeof(file_bytes));

    // compare frozen byte data with File contents
    if (file_data != file.ReadComplete())
        std::cout << common::Hexdump(file.ReadComplete()) << std::endl;

    ASSERT_EQ(file_data, file.ReadComplete());

    {
        data::File::Reader fr = file.GetReader(/* consume */ false);
        core::BitStreamReader<data::File::Reader> bsr(fr);

        ASSERT_EQ(5u, bsr.GetBits(4));
        ASSERT_EQ(42u, bsr.GetBits(6));
        ASSERT_EQ(0xC0FFEEu, bsr.GetBits(24));
        ASSERT_EQ(0xC0FFEEu, bsr.GetBits(32));
    }
}

TEST(GolombBitStreamWriter, Test) {

    data::BlockPool block_pool_;

    // construct File with very small blocks for testing
    data::File file(block_pool_, 0, /* dia_id */ 0);

    {
        data::File::Writer fw = file.GetWriter(16);
        core::GolombBitStreamWriter<data::File::Writer> gbsw(fw, 16);

        gbsw.PutGolomb(5);
        gbsw.PutGolomb(42);
        gbsw.PutGolomb(0);
        gbsw.PutGolomb(0xC0);
        gbsw.PutGolomb(0xFF);
        gbsw.PutGolomb(0xEE);
        gbsw.PutGolomb(0xC0);
        gbsw.PutGolomb(0xFF);
        gbsw.PutGolomb(0xEE);
    }

    // BitStreamWriter stores in size_t items
    ASSERT_EQ(24u, file.size_bytes());

    const unsigned char file_bytes[] = {
        0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xFE, 0xFF, 0xF7, 0xFF, 0x07, 0xFF, 0x0F, 0xD4,
        0xE0, 0xFE, 0xFF, 0xF7, 0xFF, 0x07, 0xFF, 0xEF
    };

    std::string file_data(reinterpret_cast<const char*>(file_bytes),
                          sizeof(file_bytes));

    // compare frozen byte data with File contents
    if (file_data != file.ReadComplete())
        std::cout << common::Hexdump(file.ReadComplete()) << std::endl;

    ASSERT_EQ(file_data, file.ReadComplete());

    {
        data::File::Reader fr = file.GetReader(/* consume */ false);
        core::GolombBitStreamReader<data::File::Reader> gbsr(fr, 16);

        ASSERT_EQ(5u, gbsr.GetGolomb());
        ASSERT_EQ(42u, gbsr.GetGolomb());
        ASSERT_EQ(0u, gbsr.GetGolomb());
        ASSERT_EQ(0xC0, gbsr.GetGolomb());
        ASSERT_EQ(0xFF, gbsr.GetGolomb());
        ASSERT_EQ(0xEE, gbsr.GetGolomb());
        ASSERT_EQ(0xC0, gbsr.GetGolomb());
        ASSERT_EQ(0xFF, gbsr.GetGolomb());
        ASSERT_EQ(0xEE, gbsr.GetGolomb());
    }
}

/******************************************************************************/
