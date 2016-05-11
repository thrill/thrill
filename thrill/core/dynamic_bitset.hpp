/*******************************************************************************
 * thrill/core/dynamic_bitset.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * TODO: Copyright from Sebastian Schlag
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_DYNAMIC_BITSET_HEADER
#define THRILL_CORE_DYNAMIC_BITSET_HEADER

#include <thrill/common/logger.hpp>
#include <thrill/common/math.hpp>
#include <cmath>

namespace thrill {
namespace core {

template<typename base = uint64_t>
class DynamicBitset {
private:
	static const int bit_length = sizeof(base) * 8, bit_length_doubled = 2 * bit_length, logbase = std::log2(bit_length);
	static const base mask = (((base)1) << logbase) - 1, all_set = ~((base)0), msb_set = ((base)1) << (bit_length - 1);

	//TODO: set this.
    const int alignment = 64;

	using index_type = size_t;
	using byte = uint8_t;
	
	index_type n, m;

	base *v;
	byte *memory1;

	const base b;
	const int p;
	const base max_little_value;

public:
	DynamicBitset(index_type _n, bool init = false, base _b = 1) :
		b(_b),  // Golomb tuning parameter
		p(common::IntegerLog2Ceil(b)), // helper var for golomb in
		max_little_value((((base)1) << p) - b) {
		n = _n;
		m = n / (sizeof(base) * 8) + 1;
		LOG1 << "ALLOCATING " << sizeof(base) * m + alignment << " BYTES OF MEMORY";
		memory1 = new byte[sizeof(base) * m + alignment];
		
		v = new((void *)((((ptrdiff_t)memory1) & ~((ptrdiff_t)alignment - 1)) + alignment)) base[m];

		if (init){
			for (index_type i = 0; i < m; i++) {
				v[i] = all_set; //all bits set
			}
		} else {
			for (index_type i = 0; i < m; i++) {
				v[i] = 0;
			}
		} 

            pos = maxpos = 0;
            bits = 0;
            buffer = 0;
        }

        ~DynamicBitset() {
			LOG1 << "do i delete too often?";
			LOG1 << "mem: " << Getm();
            if (memory1 != NULL) {
				 delete[] memory1;
            }
        }
        /* -------------------------------------------------------------------------------*/
        DynamicBitset(base *_data, index_type _m, base _b) :
            n(0),
            m(_m),
            v(_data),
            b(_b),
            p(common::IntegerLog2Ceil(b)),
            max_little_value((((base)1) << p) - b) {
            memory1 = NULL;
            pos = maxpos = 0;
            bits = 0;
            buffer = 0;
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
        inline base *GetGolombData() const {
            return v;
        }

	inline index_type ByteSize() const {
		return byte_size();
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

        //PRECONDITION: length < bit_length
        inline void set(index_type pos, int length, base value) {
            //cout << pos << " " << length << endl;

            index_type block = pos >> logbase;  // / bit_length
            int bit_start = pos & mask; // % bit_length
            //bit_start < bit_length

            if (bit_start + length > bit_length) { //equality to avoid problems with shifts of more than word-length
                //use two block, even more are unsupported
                int length_first = bit_length - bit_start, length_second = length - length_first;
                //length_second < bit_length
                v[block    ] = (v[block    ] & ~(all_set >> bit_start    )) | (value >> length_second);
                v[block + 1] = (v[block + 1] &  (all_set >> length_second)) | (value << (bit_length - length_second));
            } else if (bit_start + length == bit_length) {
                v[block    ] =  (v[block   ] & ~(all_set >> bit_start))
                                |       (value << (bit_length - (bit_start + length)));
            } else {
                v[block    ] =  (v[block   ] & (~(all_set >> bit_start) | (all_set >> (bit_start + length))))
                                |       (value << (bit_length - (bit_start + length)));
            }
        }

        inline void reset(index_type pos) {
            v[pos >> logbase] &= ~(msb_set >> (pos & mask));
        }

        inline bool operator[](index_type pos) const {
            return (v[pos >> logbase] & (msb_set >> (pos & mask))) != 0;
        }

        inline base get(index_type pos) const {
            return (v[pos >> logbase] & (msb_set >> (pos & mask))) >> (~pos & mask);
        }

        //PRECONDITION: length < bit_length
        inline base get(index_type pos, int length) const {
            base res;

            //cout << pos << " " << length << endl;

            index_type block = pos >> logbase;  // / bit_length
            int bit_start = pos & mask; // % bit_length

            if (bit_start + length >= bit_length) { //equality to avoid problems with shifts of more than word-length
                //use two block, even more are unsupported
                int length_first = bit_length - bit_start, length_second = length - length_first;
                res =   ((v[block    ] &  (all_set >> bit_start    )) << length_second)
                        |   ((v[block + 1] & ~(all_set >> length_second)) >> (bit_length - length_second)); //length_second == 0 does not hurt here, since 0 anyways
            } else {
                res = (v[block] & (all_set >> bit_start) & ~(all_set >> (bit_start + length))) >> ((~(pos + length - 1)) & mask);
            }
            return res;
        }

        inline long long get_signed(index_type pos, int length) const {
            long long res = get(pos, length);

            //sign extension

            if (res >> (length - 1) == 1)
                res |= (all_set << length);

            return res;
        }

        //streaming functions

    private:
        base buffer;
        index_type pos, maxpos;
        int bits;

    public:
        inline void seek(size_t bit_pos = 0) {
            //v[pos] = buffer;  //write back last result

            pos = bit_pos >> logbase;
            bits = bit_pos & mask;
            buffer = v[pos] << bits;
        }

        inline base cursor() {
            return (pos << logbase) + bits;
        }

        inline void stream_in(short length, base value) {
            if (bits + length > bit_length) {
                int length_first = bit_length - bits, length_second = length - length_first;

                buffer |= value >> (length - length_first);

                v[pos] = buffer;
                pos++;

                buffer = value << (bit_length - length_second);
                bits = (bits + length) & mask;
            } else if (bits + length == bit_length) {
                buffer |= value;

                v[pos] = buffer;
                pos++;

                buffer = 0;
                bits = 0;
            } else {
                buffer |=  value << (bit_length - (length + bits));
                bits += length;
            }

            v[pos] = buffer;

            maxpos = std::max(pos, maxpos);
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
            } else if ((bits + length) == bit_length) {
                res = buffer >> bits;
                pos++;
                bits = 0;
                buffer = v[pos];
            } else {
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

            //skip 0
            buffer <<= 1;
            bits++;
            if (bits == bit_length) {
                bits = 0;
                pos++;
                buffer = v[pos];
            }
            return no1;
        }

        inline void golomb_in(base value) {
            value--;
            base q = value / b;
            base r = value - q * b;

            //d049672: golomb_in might fail on pathological sequences that push the golomb code to its maximum size.
            //         In these cases, it is possible that the unary sequence of 1s is greater than bit_length, which is not
            //         supported by the original implementation! See microbench_golomb_coding_pathological_case.cpp for an example.
            while (static_cast<int>(q) > bit_length) {
                q -= bit_length;
                stream_in(bit_length, all_set);
            }

            base res = (all_set >> (bit_length - q - 1)) - 1;

            if (r >= max_little_value) {
                res = (res << p) | (r + max_little_value);
                stream_in(q + 1 + p, res);
            } else {
                res = (res << (p - 1)) | r;
                stream_in(q + 1 + p - 1, res);
            }
        }

        inline base golomb_out() {
            base q = number_of_ones();
            base r = stream_out(p - 1);

            if (r >= max_little_value)
                r = ((r << 1) + stream_out(1)) - max_little_value;

            return ((q * b) + r) + 1;
        }

        inline unsigned long long byte_size() const {
            if (maxpos > 0) {
                return (maxpos * bit_length / 8) + common::IntegerDivRoundUp(bits, 8);
            } else {
                return common::IntegerDivRoundUp(bits, 8);
            }

        }
};

} //namespace core
} //namespace thrill

#endif // !THRILL_CORE_DYNAMIC_BITSET_HEADER
