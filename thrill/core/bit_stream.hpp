/*******************************************************************************
 * thrill/core/bit_stream.hpp
 *
 * Encode bit stream into a BlockWriter and read from BlockReader.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_BIT_STREAM_HEADER
#define THRILL_CORE_BIT_STREAM_HEADER

#include <thrill/common/math.hpp>
#include <thrill/data/block_reader.hpp>
#include <thrill/data/block_writer.hpp>

#include <tlx/die.hpp>

namespace thrill {
namespace core {

template <typename BlockWriter>
class BitStreamWriter
{
protected:
    //! number of bits in buffer_
    enum : size_t { buffer_bits_ = sizeof(size_t) * 8 };

    //! modulo mask of number of bits in buffer for pos_ counter
    enum : size_t { mask = buffer_bits_ - 1 };

public:
    explicit BitStreamWriter(BlockWriter& block_writer)
        : block_writer_(block_writer) {
        die_unless(block_writer.block_size() % sizeof(size_t) == 0);
    }

    //! non-copyable: delete copy-constructor
    BitStreamWriter(const BitStreamWriter&) = delete;
    //! non-copyable: delete assignment operator
    BitStreamWriter& operator = (const BitStreamWriter&) = delete;
    //! move-constructor: default
    BitStreamWriter(BitStreamWriter&&) = default;
    //! move-assignment operator: default
    BitStreamWriter& operator = (BitStreamWriter&&) = default;

    ~BitStreamWriter() {
        FlushBits();
    }

    /*!
     * Append k bits to the data array.
     *
     * \param value new value
     * \param bits k = size of the new value in bits
     */
    void PutBits(const size_t& value, unsigned bits) {
        // check that only valid bits are set in value
        assert(bits == 64 || (value & (~size_t(0) << bits)) == 0);

        if (pos_ + bits > buffer_bits_) {
            // buffer overflown
            int length_first = buffer_bits_ - pos_,
                length_second = bits - length_first;

            buffer_ |= value >> (bits - length_first);
            block_writer_.PutRaw(buffer_);

            buffer_ = value << (buffer_bits_ - length_second);
            pos_ = (pos_ + bits) & mask;
        }
        else if (pos_ + bits == buffer_bits_) {
            // buffer just filled
            buffer_ |= value;
            block_writer_.PutRaw(buffer_);

            buffer_ = 0;
            pos_ = 0;
        }
        else {
            // buffer not full
            buffer_ |= value << (buffer_bits_ - (bits + pos_));
            pos_ += bits;
        }
    }

    /*!
     * Flush out buffered bits
     */
    void FlushBits() {
        if (pos_ != 0) {
            block_writer_.PutRaw(buffer_);
            buffer_ = 0;
            pos_ = 0;
        }
    }

protected:
    //! Output BlockWriter
    BlockWriter& block_writer_;

    //! current buffer of 32/64 bits
    size_t buffer_ = 0;

    //! currently filled number of bits
    size_t pos_ = 0;
};

template <typename BlockReader>
class BitStreamReader
{
protected:
    //! number of bits in buffer_
    enum : size_t { buffer_bits_ = sizeof(size_t) * 8 };

    //! modulo mask of number of bits in buffer for pos_ counter
    enum : size_t { mask = (buffer_bits_ - 1) };

    //! highest bit set
    enum : size_t { msb_set = ((size_t)1) << (buffer_bits_ - 1) };

public:
    explicit BitStreamReader(BlockReader& block_reader)
        : block_reader_(block_reader)
    { }

    //! non-copyable: delete copy-constructor
    BitStreamReader(const BitStreamReader&) = delete;
    //! non-copyable: delete assignment operator
    BitStreamReader& operator = (const BitStreamReader&) = delete;
    //! move-constructor: default
    BitStreamReader(BitStreamReader&&) = default;
    //! move-assignment operator: default
    BitStreamReader& operator = (BitStreamReader&&) = default;

    /*!
     * Get bits at the cursor.
     *
     * \param bits number of bits to return
     *
     * \return {bits} at the cursor
     */
    size_t GetBits(unsigned bits) {
        size_t res;
        if (pos_ + bits > buffer_bits_) {
            // value continuing in next array element
            int bits_first = buffer_bits_ - pos_ + 1,
                bits_second = bits - bits_first;

            res = buffer_ >> (pos_ - 1) << bits_second;
            buffer_ = block_reader_.template GetRaw<size_t>();

            res |= buffer_ >> (2 * buffer_bits_ - bits - pos_);

            pos_ = (pos_ + bits) & mask;
            buffer_ <<= pos_;
        }
        else {
            // in single array element
            res = buffer_ >> (buffer_bits_ - bits);
            pos_ += bits;
            buffer_ <<= bits;
        }
        return res;
    }

    //! Test if the buffer contains a zero or if another item can be read. This
    //! test is used by the Golomb decoder to check if another value is
    //! available.
    bool HasNextZeroTest() {

        if (pos_ == buffer_bits_) {
            if (!block_reader_.HasNext())
                return false;

            pos_ = 0;
            buffer_ = block_reader_.template GetRaw<size_t>();
        }

        // this buffer contains some zero or next available.
        return (~buffer_ >> pos_) != 0 || block_reader_.HasNext();
    }

    /*!
     * Returns the number of continuous 1 bits at the cursor, followed by a
     * zero. Used in Golomb decoding.
     *
     * \return Number of continuous 1 bits at the cursor, the zero is skipped.
     */
    unsigned GetNumberOfOnesUntilNextZero() {
        unsigned no_ones = 0;

        if (pos_ == buffer_bits_) {
            pos_ = 0;
            buffer_ = block_reader_.template GetRaw<size_t>();
        }

        while (buffer_ & msb_set) {
            buffer_ <<= 1;
            pos_++;
            no_ones++;
            if (pos_ == buffer_bits_) {
                pos_ = 0;
                buffer_ = block_reader_.template GetRaw<size_t>();
            }
        }

        // skip 0
        buffer_ <<= 1;
        pos_++;
        return no_ones;
    }

protected:
    //! Input BlockReader
    BlockReader& block_reader_;

    //! current buffer of 32/64 bits
    size_t buffer_ = 0;

    //! currently used number of bits
    size_t pos_ = buffer_bits_;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_BIT_STREAM_HEADER

/******************************************************************************/
