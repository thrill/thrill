/*******************************************************************************
 * thrill/core/hyperloglog.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2017 Moritz Kiefer <moritz.kiefer@purelyfunctional.org>
 * Copyright (C) 2017 Tino Fuhrmann <tino-fuhrmann@web.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_HYPERLOGLOG_HEADER
#define THRILL_CORE_HYPERLOGLOG_HEADER

#include <thrill/data/serialization_fwd.hpp>
#include <tlx/die.hpp>
#include <tlx/math/clz.hpp>
#include <tlx/siphash.hpp>

#include <cmath>
#include <vector>

namespace thrill {
namespace core {

// The high 25 bit in this register are used for the index, the next 6 bits for
// the value and the last bit is currently unused
using HyperLogLogSparseRegister = uint32_t;

enum class HyperLogLogRegisterFormat { SPARSE, DENSE };

template <size_t p>
class HyperLogLogRegisters
{
public:
    HyperLogLogRegisters() : format_(HyperLogLogRegisterFormat::SPARSE) { }

    size_t size() const { return entries_.size(); }

    void toDense();

    bool shouldConvertToDense();
    bool shouldMerge();

    template <typename ValueType>
    void insert(const ValueType& value) {
        // first p bits are the index
        insert_hash(tlx::siphash(value));
    }

    void insert_hash(const uint64_t& hash_value);
    void mergeSparse();

    void mergeDense(const HyperLogLogRegisters<p>& b);

    //! calculate count estimation result adjusted for bias
    double result();

    //! combine two HyperloglogRegisters, switches between sparse/dense
    //! representations
    HyperLogLogRegisters operator + (
        const HyperLogLogRegisters<p>& registers2) const;

    //! declare friendship with serializers
    template <typename Archive, typename T, typename Enable>
    friend struct data::Serialization;

private:
    unsigned sparse_size_ = 0;
    HyperLogLogRegisterFormat format_;

    // Register values are always smaller than 64. We thus need log2(64) = 6
    // bits to store them. In particular an uint8_t is sufficient
    std::vector<uint8_t> sparseListBuffer_;
    std::vector<HyperLogLogSparseRegister> deltaSet_;
    std::vector<uint8_t> entries_;
};

/******************************************************************************/
// Additional Helpers, exposed mainly for testing

namespace hyperloglog {

template <size_t sparsePrecision, size_t densePrecision>
uint32_t encodeHash(uint64_t hash);

template <size_t sparsePrecision, size_t densePrecision>
std::pair<size_t, uint8_t> decodeHash(HyperLogLogSparseRegister reg);

//! Perform a varint and a difference encoding
std::vector<uint8_t> encodeSparseList(const std::vector<uint32_t>& sparseList);
std::vector<uint32_t> decodeSparseList(const std::vector<uint8_t>& sparseList);

} // namespace hyperloglog
} // namespace core

namespace data {

template <typename Archive, size_t p>
struct Serialization<Archive, core::HyperLogLogRegisters<p> >{

    static void Serialize(const core::HyperLogLogRegisters<p>& x, Archive& ar);
    static core::HyperLogLogRegisters<p> Deserialize(Archive& ar);

    static constexpr bool   is_fixed_size = false;
    static constexpr size_t fixed_size = 0;
};

} // namespace data
} // namespace thrill

#endif // !THRILL_CORE_HYPERLOGLOG_HEADER

/******************************************************************************/
