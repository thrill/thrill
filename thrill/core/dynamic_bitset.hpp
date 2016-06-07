/*******************************************************************************
 * thrill/core/dynamic_bitset.hpp
 *
 * Golomb encoded bitset
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

#include <algorithm>
#include <cmath>

#include <thrill/common/defines.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/math.hpp>
// TODO: Add Copyright of Sebastian Schlag& original author.

namespace thrill {
namespace core {

template <typename base = uint64_t>
class DynamicBitset
{
private:
    static const int bit_length = sizeof(base) * 8, bit_length_doubled = 2 * bit_length, logbase = std::log2(bit_length);
    static const base mask = (((base)1) << logbase) - 1, all_set = ~((base)0), msb_set = ((base)1) << (bit_length - 1);

    static constexpr bool debug = false;
    std::vector<size_t> inserted_elements;

    const int alignment = 64;

    using index_type = size_t;
    using byte = uint8_t;

    index_type n, m;

    base* v;
    byte* memory1;

    bool in_called_already;
    bool out_called_already;

    const base b;
    const int p;
    const base max_little_value;

public:
    DynamicBitset(index_type _n, bool init = false, base _b = 1)
        : b(_b),                         // Golomb tuning parameter
          p(common::IntegerLog2Ceil(b)), // helper var for golomb in
          max_little_value((((base)1) << p) - b) {
        n = _n;
        m = n / (sizeof(base) * 8) + 1;
        memory1 = new byte[sizeof(base) * m + alignment];
        in_called_already = false;
        out_called_already = false;
        num_elements = 0;

        v = new ((void*)((((ptrdiff_t)memory1) & ~((ptrdiff_t)alignment - 1)) + alignment))base[m];

        if (init) {
            for (index_type i = 0; i < m; i++) {
                v[i] = all_set;                 //all bits set
            }
        }
        else {
            for (index_type i = 0; i < m; i++) {
                v[i] = 0;
            }
        }

        pos = maxpos = 0;
        bits = 0;
        buffer = 0;
    }

    ~DynamicBitset() {
        if (debug && num_elements) {
            std::sort(inserted_elements.begin(), inserted_elements.end());
            double entropy_total = 0;
            size_t last = 0;
            double total_prob = 0;
            for (size_t i = 1; i < inserted_elements.size(); ++i) {
                if (inserted_elements[i] > inserted_elements[i - 1]) {
                    size_t equal_elements = i - last;
                    double probability = (double)equal_elements / (double)num_elements;
                    total_prob += probability;
                    double entropy_i = probability * std::log2(probability);
                    assert(entropy_i < 0);
                    entropy_total -= entropy_i;
                    last = i;
                }
            }
            double last_prob = (double)(num_elements - last) / (double)num_elements;
            entropy_total -= last_prob * std::log2(last_prob);
            total_prob += last_prob;

            assert(std::fabs(total_prob - 1.0) <= 0.00001);

            size_t total_inform = std::ceil(entropy_total * (double)num_elements);

            sLOG << "Bitset: items:" << num_elements
                 << "size(b):" << bit_size()
                 << "total_inform" << total_inform
                 << "size_factor" << (double)bit_size() / (double)total_inform;
        }
        if (memory1 != nullptr) {
            delete[] memory1;
        }
    }
    /* -------------------------------------------------------------------------------*/
    DynamicBitset(base* _data, index_type _m, base _b, size_t _num_elements)
        : n(0),
          m(_m),
          v(_data),
          b(_b),
          p(common::IntegerLog2Ceil(b)),
          max_little_value((((base)1) << p) - b),
          num_elements(_num_elements) {
        memory1 = nullptr;
        pos = 0;
        maxpos = _m;
        bits = 0;
        buffer = 0;
        in_called_already = (_m > 0);
        out_called_already = false;
    }

    inline index_type Getm() {
        return m;
    }

    inline index_type GetMaxPos() {
        return maxpos;
    }

    inline index_type GetPos() {
        return pos;
    }
    inline int GetBits() {
        return bits;
    }
    inline base * GetGolombData() const {
        return v;
    }

    inline index_type ByteSize() const {
        return byte_size();
    }

    inline base GetBuffer() const {
        return buffer;
    }

    inline index_type size() const {
        return common::IntegerDivRoundUp((index_type)byte_size(), (index_type)8);
    }

    inline void clear() {
        for (index_type i = 0; i < m; i++) {
            v[i] = 0;
        }
        pos = maxpos = 0;
        bits = 0;
        buffer = 0;
    }

    /* -------------------------------------------------------------------------------*/

    inline void set(index_type pos) {
        v[pos >> logbase] |= (msb_set >> (pos & mask));
    }

    inline void set(index_type pos, bool value) {
        if (value)
            set(pos);
        else
            reset(pos);
    }

    // PRECONDITION: length < bit_length
    inline void set(index_type pos, int length, base value) {
        // cout << pos << " " << length << endl;

        index_type block = pos >> logbase;     // / bit_length
        int bit_start = pos & mask;            // % bit_length
        // bit_start < bit_length

        if (bit_start + length > bit_length) { //equality to avoid problems with shifts of more than word-length
            // use two block, even more are unsupported
            int length_first = bit_length - bit_start, length_second = length - length_first;
            // length_second < bit_length
            v[block] = (v[block] & ~(all_set >> bit_start)) | (value >> length_second);
            v[block + 1] = (v[block + 1] & (all_set >> length_second)) | (value << (bit_length - length_second));
        }
        else if (bit_start + length == bit_length) {
            v[block] = (v[block] & ~(all_set >> bit_start))
                       | (value << (bit_length - (bit_start + length)));
        }
        else {
            v[block] = (v[block] & (~(all_set >> bit_start) | (all_set >> (bit_start + length))))
                       | (value << (bit_length - (bit_start + length)));
        }
    }

    inline void reset(index_type pos) {
        v[pos >> logbase] &= ~(msb_set >> (pos & mask));
    }

    inline bool operator [] (index_type pos) const {
        return (v[pos >> logbase] & (msb_set >> (pos & mask))) != 0;
    }

    inline base get(index_type pos) const {
        return (v[pos >> logbase] & (msb_set >> (pos & mask))) >> (~pos & mask);
    }

    // PRECONDITION: length < bit_length
    inline base get(index_type pos, int length) const {
        base res;

        // cout << pos << " " << length << endl;

        index_type block = pos >> logbase;                                                          // / bit_length
        int bit_start = pos & mask;                                                                 // % bit_length

        if (bit_start + length >= bit_length) {                                                     //equality to avoid problems with shifts of more than word-length
            // use two block, even more are unsupported
            int length_first = bit_length - bit_start, length_second = length - length_first;
            res = ((v[block] & (all_set >> bit_start)) << length_second)
                  | ((v[block + 1] & ~(all_set >> length_second)) >> (bit_length - length_second)); //length_second == 0 does not hurt here, since 0 anyways
        }
        else {
            res = (v[block] & (all_set >> bit_start) & ~(all_set >> (bit_start + length))) >> ((~(pos + length - 1)) & mask);
        }
        return res;
    }

    inline long long get_signed(index_type pos, int length) const {
        long long res = get(pos, length);

        // sign extension

        if (res >> (length - 1) == 1)
            res |= (all_set << length);

        return res;
    }

    // streaming functions

private:
    base buffer;
    index_type pos, maxpos;
    int bits;
    size_t num_elements;

public:
    inline void seek(size_t bit_pos = 0) {
        //      v[pos] = buffer;  //write back last result

        pos = bit_pos >> logbase;
        bits = bit_pos & mask;
        buffer = v[pos] << bits;
    }

    inline base cursor() {
        return (pos << logbase) + bits;
    }

    inline void stream_in(short length, base value) {
        assert(pos * 8 < n);
        if (bits + length > bit_length) {
            int length_first = bit_length - bits, length_second = length - length_first;

            buffer |= value >> (length - length_first);

            v[pos] = buffer;
            pos++;

            buffer = value << (bit_length - length_second);
            bits = (bits + length) & mask;
        }
        else if (bits + length == bit_length) {
            buffer |= value;

            v[pos] = buffer;
            pos++;

            buffer = 0;
            bits = 0;
        }
        else {
            buffer |= value << (bit_length - (length + bits));
            bits += length;
        }

        v[pos] = buffer;

        maxpos = std::max(pos, maxpos);
        /*	if (pos > maxpos) {
                maxpos = pos;
                }*/
    }

    inline base stream_out(short length) {
        base res;

        if ((bits + length) > bit_length) {
            int length_first = bit_length - bits + 1, length_second = length - length_first;

            res = buffer >> (bits - 1) << length_second;
            pos++;
            buffer = v[pos];

            res |= buffer >> (bit_length_doubled - length - bits);

            bits = (bits + length) & mask;
            buffer <<= bits;
        }
        else if ((bits + length) == bit_length) {
            res = buffer >> bits;
            pos++;
            bits = 0;
            buffer = v[pos];
        }
        else {
            res = (buffer >> (bit_length - length));
            bits += length;
            buffer <<= length;
        }

        return res;
    }

    inline int number_of_ones() {
        int no1 = 0;
        while (buffer & msb_set) {
            buffer <<= 1;
            bits++;
            no1++;
            if (bits == bit_length) {
                bits = 0;
                pos++;
                buffer = v[pos];
            }
        }

        // skip 0
        buffer <<= 1;
        bits++;
        if (bits == bit_length) {
            bits = 0;
            pos++;
            buffer = v[pos];
        }
        return no1;
    }

    inline void golomb_in(const base& value) {
        ++num_elements;
        if (debug) {
            inserted_elements.push_back(value);
        }
        if (THRILL_LIKELY(in_called_already)) {
            assert(pos > 0);

            base q = (value - 1) / b;
            base r = (value - 1) - (q * b);

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

            if (THRILL_UNLIKELY(q + 1 + p > bit_length)) {
                assert(q + 1 <= bit_length);
                base res = (all_set >> (bit_length - q - 1)) - 1;
                stream_in(q + 1, res);
                if (r >= max_little_value) {
                    stream_in(p, r + max_little_value);
                }
                else {
                    stream_in(p - 1, r);
                }
            }
            else {

                base res = (all_set >> (bit_length - q - 1)) - 1;

                if (r >= max_little_value) {
                    res = (res << p) | (r + max_little_value);
                    stream_in(q + 1 + p, res);
                }
                else {
                    res = (res << (p - 1)) | r;
                    stream_in(q + 1 + p - 1, res);
                }
            }
        }
        else {
            assert(pos == 0);
            assert(maxpos == 0);
            v[0] = value;
            pos++;
            maxpos++;
            bits = 0;
            in_called_already = true;
        }
    }

    inline base golomb_out() {
        if (THRILL_LIKELY(out_called_already)) {
            assert(pos > 0);
            base q = number_of_ones();
            base r = stream_out(p - 1);

            if (r >= max_little_value)
                r = ((r << 1) + stream_out(1)) - max_little_value;

            if (debug && !n) {
                inserted_elements.push_back(((q * b) + r) + 1);
            }

            return ((q * b) + r) + 1;
        }
        else {
            out_called_already = true;
            assert(pos == 0);

            if (maxpos == 0) {
                maxpos = 1;
            }
            pos = 1;
            bits = 0;
            buffer = v[1];
            if (debug && !n) {
                inserted_elements.push_back(v[0]);
            }
            return v[0];
        }
    }

    inline size_t byte_size() const {
        if (maxpos > 0) {
            return (maxpos * bit_length / 8) + common::IntegerDivRoundUp(bits, 8);
        }
        else {
            return common::IntegerDivRoundUp(bits, 8);
        }
    }

    inline size_t bit_size() const {
        return maxpos * bit_length + bits;
    }
};
} //namespace core
} //namespace thrill
#endif // !THRILL_CORE_DYNAMIC_BITSET_HEADER

/******************************************************************************/
