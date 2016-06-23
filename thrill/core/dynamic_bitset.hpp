
/*******************************************************************************
 * thrill/core/dynamic_bitset.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_DYNAMIC_BITSET_HEADER
#define THRILL_CORE_DYNAMIC_BITSET_HEADER

#include <thrill/common/defines.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/math.hpp>

#include <algorithm>
#include <cmath>

// TODO: Add Copyright of Sebastian Schlag& original author.

namespace thrill {
namespace core {

/*!
 * Dynamic bitset, which encodes values with a golomb encoder.
 *
 * \param BaseType integer type used in the encoder
 */
template <typename BaseType = uint64_t>
class DynamicBitset
{
private:
    //! Helper constants
    static const int bit_length = sizeof(BaseType) * 8, bit_length_doubled = 2 * bit_length, logbase = std::log2(bit_length);
    static const BaseType mask = (((BaseType)1) << logbase) - 1, all_set = ~((BaseType)0), msb_set = ((BaseType)1) << (bit_length - 1);

    static constexpr bool debug = false;
    //! Used to compute total amount of entropy encoded
    std::vector<size_t> inserted_elements_;

    const int alignment = 64;

    using IndexType = size_t;
    using byte = uint8_t;

    //! Maximum size of bitset
    IndexType bitset_size_bits_, bitset_size_base_;

    BaseType* data_;
    byte* memory_;

    //! true, when golomb_in was called already
    bool in_called_already_;
    //! true, when golomb_out was called already
    bool out_called_already_;

    //! Golomb Parameter
    const BaseType b_;
    //! ceil(log2(b))
    const int log2b_;
    const BaseType max_little_value_;

public:
    /*!
     * Create a new bitset, allocate memory
     *
     * \param n bits of allocated memory
     * \param init true, when bits should be set
     * \param b golomb parameter
     */
    DynamicBitset(IndexType n, bool init = false, BaseType b = 1)
        : b_(b),                               // Golomb tuning parameter
          log2b_(common::IntegerLog2Ceil(b_)), // helper var for golomb in
          max_little_value_((((BaseType)1) << log2b_) - b_) {
        bitset_size_bits_ = n;
        bitset_size_base_ = bitset_size_bits_ / (sizeof(BaseType) * 8) + 1;
        memory_ = new byte[sizeof(BaseType) * bitset_size_base_ + alignment];
        in_called_already_ = false;
        out_called_already_ = false;
        num_elements_ = 0;

        data_ = new ((void*)((((ptrdiff_t)memory_) & ~((ptrdiff_t)alignment - 1)) + alignment))BaseType[bitset_size_base_];

        if (init) {
            for (IndexType i = 0; i < bitset_size_base_; i++) {
                data_[i] = all_set;                 //all bits set
            }
        }
        else {
            for (IndexType i = 0; i < bitset_size_base_; i++) {
                data_[i] = 0;
            }
        }

        pos_ = maxpos_ = 0;
        bits_ = 0;
        buffer_ = 0;
    }

    ~DynamicBitset() {
        if (debug && num_elements_) {
            //! Compute total entropy and compare bitset size with entropy
            std::sort(inserted_elements_.begin(), inserted_elements_.end());
            double entropy_total = 0;
            size_t last = 0;
            double total_prob = 0;
            for (size_t i = 1; i < inserted_elements_.size(); ++i) {
                if (inserted_elements_[i] > inserted_elements_[i - 1]) {
                    size_t equal_elements = i - last;
                    double probability = (double)equal_elements / (double)num_elements_;
                    total_prob += probability;
                    double entropy_i = probability * std::log2(probability);
                    assert(entropy_i < 0);
                    entropy_total -= entropy_i;
                    last = i;
                }
            }
            double last_prob = (double)(num_elements_ - last) / (double)num_elements_;
            entropy_total -= last_prob * std::log2(last_prob);
            total_prob += last_prob;

            assert(std::fabs(total_prob - 1.0) <= 0.00001);

            size_t total_inform = std::ceil(entropy_total * (double)num_elements_);

            sLOG1 << "Bitset: items:" << num_elements_
                  << "size(b):" << bit_size()
                  << "total_inform" << total_inform
                  << "size_factor" << (double)bit_size() / (double)total_inform;
        }

        if (memory_ != nullptr) {
            delete[] memory_;
        }
    }

    /*!
     * Create bitset with pre-existing data.
     *
     * \param data Pointer to bitset data
     * \param bitsize Size of data in BaseType elements
     * \param b Golomb parameter
     * \param num_elements Number of elements in bitset
     */
    DynamicBitset(BaseType* data, IndexType size, BaseType b, size_t num_elements)
        : bitset_size_bits_(0),
          bitset_size_base_(size),
          data_(data),
          b_(b),
          log2b_(common::IntegerLog2Ceil(b_)),
          max_little_value_((((BaseType)1) << log2b_) - b_),
          num_elements_(num_elements) {
        memory_ = nullptr;
        pos_ = 0;
        maxpos_ = size;
        bits_ = 0;
        buffer_ = 0;
        in_called_already_ = (size > 0);
        out_called_already_ = false;
    }

    //! \name Parameter Getters
    //! \{
    inline IndexType Getm() {
        return bitset_size_base_;
    }

    inline IndexType GetMaxPos() {
        return maxpos_;
    }

    inline IndexType GetPos() {
        return pos_;
    }
    inline int GetBits() {
        return bits_;
    }
    inline BaseType * GetGolombData() const {
        return data_;
    }

    inline IndexType ByteSize() const {
        return byte_size();
    }

    inline BaseType GetBuffer() const {
        return buffer_;
    }

    inline IndexType size() const {
        return common::IntegerDivRoundUp((IndexType)byte_size(), (IndexType)8);
    }

    inline size_t byte_size() const {
        if (maxpos_ > 0) {
            return (maxpos_ * bit_length / 8) + common::IntegerDivRoundUp(bits_, 8);
        }
        else {
            return common::IntegerDivRoundUp(bits_, 8);
        }
    }

    inline size_t bit_size() const {
        return maxpos_ * bit_length + bits_;
    }

    //! \}

    inline void clear() {
        for (IndexType i = 0; i < bitset_size_base_; i++) {
            data_[i] = 0;
        }
        pos_ = maxpos_ = 0;
        bits_ = 0;
        buffer_ = 0;
    }

    /* -------------------------------------------------------------------------------*/

    //! \name Setters {
    inline void set(IndexType pos) {
        data_[pos >> logbase] |= (msb_set >> (pos & mask));
    }

    inline void set(IndexType pos, bool value) {
        if (value)
            set(pos);
        else
            reset(pos);
    }

    // PRECONDITION: length < bit_length
    inline void set(IndexType pos, int length, BaseType value) {
        // cout << pos << " " << length << endl;

        IndexType block = pos >> logbase;      // / bit_length
        int bit_start = pos & mask;            // % bit_length
        // bit_start < bit_length

        if (bit_start + length > bit_length) { //equality to avoid problems with shifts of more than word-length
            // use two block, even more are unsupported
            int length_first = bit_length - bit_start, length_second = length - length_first;
            // length_second < bit_length
            data_[block] = (data_[block] & ~(all_set >> bit_start)) | (value >> length_second);
            data_[block + 1] = (data_[block + 1] & (all_set >> length_second)) | (value << (bit_length - length_second));
        }
        else if (bit_start + length == bit_length) {
            data_[block] = (data_[block] & ~(all_set >> bit_start))
                           | (value << (bit_length - (bit_start + length)));
        }
        else {
            data_[block] = (data_[block] & (~(all_set >> bit_start) | (all_set >> (bit_start + length))))
                           | (value << (bit_length - (bit_start + length)));
        }
    }

    inline void reset(IndexType pos) {
        data_[pos >> logbase] &= ~(msb_set >> (pos & mask));
    }

    //! \}

    //! \name Getters
    //! \{

    inline bool operator [] (IndexType pos) const {
        return (data_[pos >> logbase] & (msb_set >> (pos & mask))) != 0;
    }

    inline BaseType get(IndexType pos) const {
        return (data_[pos >> logbase] & (msb_set >> (pos & mask))) >> (~pos & mask);
    }

    // PRECONDITION: length < bit_length
    inline BaseType get(IndexType pos, int length) const {
        BaseType res;

        // cout << pos << " " << length << endl;

        IndexType block = pos >> logbase;                                                               // / bit_length
        int bit_start = pos & mask;                                                                     // % bit_length

        if (bit_start + length >= bit_length) {                                                         //equality to avoid problems with shifts of more than word-length
            // use two block, even more are unsupported
            int length_first = bit_length - bit_start, length_second = length - length_first;
            res = ((data_[block] & (all_set >> bit_start)) << length_second)
                  | ((data_[block + 1] & ~(all_set >> length_second)) >> (bit_length - length_second)); //length_second == 0 does not hurt here, since 0 anyways
        }
        else {
            res = (data_[block] & (all_set >> bit_start) & ~(all_set >> (bit_start + length))) >> ((~(pos + length - 1)) & mask);
        }
        return res;
    }

    inline long long get_signed(IndexType pos, int length) const {
        long long res = get(pos, length);

        // sign extension

        if (res >> (length - 1) == 1)
            res |= (all_set << length);

        return res;
    }

    //! \}

    // streaming functions

private:
    BaseType buffer_;
    IndexType pos_, maxpos_;
    int bits_;
    size_t num_elements_;

public:
    /*!
     * Sets the pointer to a specific position
     *
     * \param bit_pos Target position
     */
    inline void seek(size_t bit_pos = 0) {
        pos_ = bit_pos >> logbase;
        bits_ = bit_pos & mask;
        buffer_ = data_[pos_] << bits_;
    }

    /*!
     * Returns the postiion of the cursor
     *
     * \return cursor position
     */
    inline BaseType cursor() {
        return (pos_ << logbase) + bits_;
    }

    /*!
     * Inserts a new value into the data array.
     *
     * \param length size of the new value in bits
     * \param value new value
     */
    inline void stream_in(short length, BaseType value) {
        assert(pos_ * 8 < bitset_size_bits_ + alignment);
        if (bits_ + length > bit_length) {
            //! buffer overflown
            int length_first = bit_length - bits_, length_second = length - length_first;

            buffer_ |= value >> (length - length_first);

            data_[pos_] = buffer_;
            pos_++;

            buffer_ = value << (bit_length - length_second);
            bits_ = (bits_ + length) & mask;
        }
        else if (bits_ + length == bit_length) {
            //! buffer just filled
            buffer_ |= value;

            data_[pos_] = buffer_;
            pos_++;

            buffer_ = 0;
            bits_ = 0;
        }
        else {
            //! buffer not full
            buffer_ |= value << (bit_length - (length + bits_));
            bits_ += length;
        }

        data_[pos_] = buffer_;
        maxpos_ = std::max(pos_, maxpos_);
    }

    /*!
     * Returns bits following the cursor.
     *
     * \param length number of bits to return
     *
     * \return {length} bits following the cursor
     */
    inline BaseType stream_out(short length) {
        BaseType res;

        if ((bits_ + length) > bit_length) {
            // value continuing in next array element
            int length_first = bit_length - bits_ + 1, length_second = length - length_first;

            res = buffer_ >> (bits_ - 1) << length_second;
            pos_++;
            buffer_ = data_[pos_];

            res |= buffer_ >> (bit_length_doubled - length - bits_);

            bits_ = (bits_ + length) & mask;
            buffer_ <<= bits_;
        }
        else if ((bits_ + length) == bit_length) {
            // value ending at end of array element
            res = buffer_ >> bits_;
            pos_++;
            bits_ = 0;
            buffer_ = data_[pos_];
        }
        else {
            // in single array element
            res = (buffer_ >> (bit_length - length));
            bits_ += length;
            buffer_ <<= length;
        }

        return res;
    }

    /*!
     * Returns the number of continuous 1 bits following the cursor. Used in golomb
     * decoding.
     *
     * \return Number of continuous 1 bits following the cursor
     */
    inline int number_of_ones() {
        int no1 = 0;
        while (buffer_ & msb_set) {
            buffer_ <<= 1;
            bits_++;
            no1++;
            if (bits_ == bit_length) {
                bits_ = 0;
                pos_++;
                buffer_ = data_[pos_];
            }
        }

        // skip 0
        buffer_ <<= 1;
        bits_++;
        if (bits_ == bit_length) {
            bits_ = 0;
            pos_++;
            buffer_ = data_[pos_];
        }
        return no1;
    }

    /*!
     * Inserts a new value into the bitset.
     */
    inline void golomb_in(const BaseType& value) {
        ++num_elements_;
        if (debug) {
            inserted_elements_.push_back(value);
        }
        if (THRILL_LIKELY(in_called_already_)) {
            assert(pos_ > 0);

            //! As we encode deltas, no value can be 0. Therefore we encode value - 1
            //! and add 1 when decoding.
            //! golomb_enc(value) = unary encoding of (value / b),0,binary encoding of (value % b)
            BaseType q = (value - 1) / b_;
            BaseType r = (value - 1) - (q * b_);

            // d049672: golomb_in might fail on pathological sequences
            // that push the golomb code to its maximum size.
            // In these cases, it is possible that the unary sequence
            // of 1s is greater than bit_length, which is not
            // supported by the original implementation!
            // See microbench_golomb_coding_pathological_case.cpp for an example.
            while (static_cast<int>(q) >= bit_length) {
                q -= bit_length;
                stream_in(bit_length, all_set);
            }

            if (THRILL_UNLIKELY(q + 1 + log2b_ > bit_length)) {
                //! When we need more than bit_length bits to encode a value, q and r
                //! have to be inserted separately, as stream_in can only handle up to
                //! bit_length bits at once
                assert(q + 1 <= bit_length);
                BaseType res = (all_set >> (bit_length - q - 1)) - 1;
                stream_in(q + 1, res);
                if (r >= max_little_value_) {
                    stream_in(log2b_, r + max_little_value_);
                }
                else {
                    stream_in(log2b_ - 1, r);
                }
            }
            else {
                //! default case
                BaseType res = (all_set >> (bit_length - q - 1)) - 1;

                if (r >= max_little_value_) {
                    res = (res << log2b_) | (r + max_little_value_);
                    stream_in(q + 1 + log2b_, res);
                }
                else {
                    res = (res << (log2b_ - 1)) | r;
                    stream_in(q + 1 + log2b_ - 1, res);
                }
            }
        }
        else {
            //! First value can be very large. It is therefore not encoded.
            assert(pos_ == 0);
            assert(maxpos_ == 0);
            data_[0] = value;
            pos_++;
            maxpos_++;
            bits_ = 0;
            in_called_already_ = true;
        }
    }

    /*!
     * Decodes the next value following the cursor and returns it as a BaseType integral
     *
     * \return the value following the cursor
     */
    inline BaseType golomb_out() {
        if (THRILL_LIKELY(out_called_already_)) {
            assert(pos_ > 0);

            BaseType q = number_of_ones();
            BaseType r = stream_out(log2b_ - 1);

            if (r >= max_little_value_)
                r = ((r << 1) + stream_out(1)) - max_little_value_;

            if (debug && !bitset_size_bits_) {
                inserted_elements_.push_back(((q * b_) + r) + 1);
            }

            return ((q * b_) + r) + 1;
        }
        else {
            out_called_already_ = true;
            assert(pos_ == 0);

            if (maxpos_ == 0) {
                maxpos_ = 1;
            }
            pos_ = 1;
            bits_ = 0;
            buffer_ = data_[1];
            if (debug && !bitset_size_bits_) {
                inserted_elements_.push_back(data_[0]);
            }
            return data_[0];
        }
    }
};

} //namespace core
} //namespace thrill

#endif // !THRILL_CORE_DYNAMIC_BITSET_HEADER

/******************************************************************************/
