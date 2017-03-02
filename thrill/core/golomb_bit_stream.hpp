/*******************************************************************************
 * thrill/core/golomb_bit_stream.hpp
 *
 * Encode bit stream using Golomb code into Block Reader/Writers via
 * BitStreamWriter/BitStreamReader.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_GOLOMB_BIT_STREAM_HEADER
#define THRILL_CORE_GOLOMB_BIT_STREAM_HEADER

#include <thrill/core/bit_stream.hpp>

#include <tlx/math/integer_log2.hpp>

namespace thrill {
namespace core {

/******************************************************************************/
// GolombBitStreamWriter

template <typename BlockWriter>
class GolombBitStreamWriter : public BitStreamWriter<BlockWriter>
{
private:
    using Super = BitStreamWriter<BlockWriter>;

    using Super::buffer_bits_;

    enum : size_t { all_set = ~((size_t)0) };

public:
    GolombBitStreamWriter(BlockWriter& block_writer, const size_t& b)
        : Super(block_writer),
          b_(b),
          log2b_(tlx::integer_log2_ceil(b_)), // helper var for Golomb in
          max_little_value_((((size_t)1) << log2b_) - b_) {
        die_unless(block_writer.block_size() % sizeof(size_t) == 0);
    }

    //! non-copyable: delete copy-constructor
    GolombBitStreamWriter(const GolombBitStreamWriter&) = delete;
    //! non-copyable: delete assignment operator
    GolombBitStreamWriter& operator = (const GolombBitStreamWriter&) = delete;
    //! move-constructor: default
    GolombBitStreamWriter(GolombBitStreamWriter&&) = default;
    //! move-assignment operator: default
    GolombBitStreamWriter& operator = (GolombBitStreamWriter&&) = default;

    ~GolombBitStreamWriter() {
        if (Super::pos_ != 0) {
            // fill currently remaining buffer item with ones. the decoder will
            // detect that no zero follows these ones.
            unsigned bits = buffer_bits_ - Super::pos_;
            Super::PutBits(all_set >> (buffer_bits_ - bits), bits);
            assert(Super::pos_ == 0);
        }
    }

    /*!
     * Append new Golomb-encoded value to bitset
     */
    void PutGolomb(const size_t& value) {

        if (TLX_UNLIKELY(first_call_)) {
            // First value can be very large. It is therefore not encoded.
            Super::block_writer_.PutRaw(value);
            first_call_ = false;
            return;
        }

        // golomb_enc(value) =
        // unary encoding of (value / b),0,binary encoding of (value % b)

        size_t q = value / b_;
        size_t r = value - (q * b_);

        // d049672: PutGolomb() might fail on pathological sequences that push
        // the Golomb code to its maximum size.  In these cases, it is possible
        // that the unary sequence of 1s is greater than buffer_bits_, which is
        // not supported by the original implementation!  See
        // microbench_golomb_coding_pathological_case.cpp for an example.
        while (q >= buffer_bits_) {
            q -= buffer_bits_;
            Super::PutBits(all_set, buffer_bits_);
        }

        if (TLX_UNLIKELY(q + 1 + log2b_ > buffer_bits_)) {
            // When we need more than buffer_bits_ bits to encode a value, q
            // and r have to be inserted separately, as PutBits can only
            // handle up to buffer_bits_ bits at once
            assert(q + 1 <= buffer_bits_);
            size_t res = (all_set >> (buffer_bits_ - q - 1)) - 1;
            Super::PutBits(res, q + 1);
            if (r >= max_little_value_) {
                Super::PutBits(r + max_little_value_, log2b_);
            }
            else {
                Super::PutBits(r, log2b_ - 1);
            }
        }
        else {
            // default case
            size_t res = (all_set >> (buffer_bits_ - q - 1)) - 1;

            if (r >= max_little_value_) {
                res = (res << log2b_) | (r + max_little_value_);
                Super::PutBits(res, q + 1 + log2b_);
            }
            else {
                res = (res << (log2b_ - 1)) | r;
                Super::PutBits(res, q + 1 + log2b_ - 1);
            }
        }
    }

    void Put(size_t value) {
        PutGolomb(value);
    }

private:
    //! Golomb code parameter
    size_t b_;

    //! ceil(log2(b_))
    int log2b_;

    //! escape value
    size_t max_little_value_;

    //! false, when PutGolomb_in was called already
    bool first_call_ = true;
};

/******************************************************************************/
// GolombBitStreamReader

template <typename BlockReader>
class GolombBitStreamReader : public BitStreamReader<BlockReader>
{
private:
    using Super = BitStreamReader<BlockReader>;

    using Super::buffer_bits_;

public:
    GolombBitStreamReader(BlockReader& block_reader, const size_t& b)
        : Super(block_reader),
          b_(b),
          log2b_(tlx::integer_log2_ceil(b_)), // helper var for Golomb in
          max_little_value_((((size_t)1) << log2b_) - b_)
    { }

    //! non-copyable: delete copy-constructor
    GolombBitStreamReader(const GolombBitStreamReader&) = delete;
    //! non-copyable: delete assignment operator
    GolombBitStreamReader& operator = (const GolombBitStreamReader&) = delete;
    //! move-constructor: default
    GolombBitStreamReader(GolombBitStreamReader&&) = default;
    //! move-assignment operator: default
    GolombBitStreamReader& operator = (GolombBitStreamReader&&) = default;

    bool HasNext() {
        if (TLX_UNLIKELY(first_call_))
            return Super::block_reader_.HasNext();

        return Super::HasNextZeroTest();
    }

    size_t GetGolomb() {
        if (TLX_UNLIKELY(first_call_)) {
            first_call_ = false;
            return Super::block_reader_.template GetRaw<size_t>();
        }

        size_t q = Super::GetNumberOfOnesUntilNextZero();
        size_t r = Super::GetBits(log2b_ - 1);

        if (r >= max_little_value_)
            r = ((r << 1) + Super::GetBits(1)) - max_little_value_;

        return (q * b_) + r;
    }

    template <typename Type2>
    size_t Next() {
        static_assert(
            std::is_same<size_t, Type2>::value, "Invalid Next() call");
        return GetGolomb();
    }

private:
    //! Golomb code parameter
    size_t b_;

    //! ceil(log2(b_))
    int log2b_;

    //! escape value
    size_t max_little_value_;

    //! false, when PutGolomb_in was called already
    bool first_call_ = true;
};

/******************************************************************************/

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_GOLOMB_BIT_STREAM_HEADER

/******************************************************************************/
