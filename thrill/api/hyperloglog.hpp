/*******************************************************************************
 * thrill/api/hyperloglog.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2017 Moritz Kiefer <moritz.kiefer@purelyfunctional.org>
 * Copyright (C) 2017 Tino Fuhrmann <tino-fuhrmann@web.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_HYPERLOGLOG_HEADER
#define THRILL_API_HYPERLOGLOG_HEADER

#include <cmath>
#include <iostream>
#include <limits.h>

#include <thrill/api/all_reduce.hpp>
#include <thrill/api/distribute.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/siphash.hpp>

namespace thrill {

template <typename Value>
uint64_t hash(const Value& val) {
    const unsigned char key[16] = {
        0, 0, 0, 0, 0, 0, 0, 0x4,
        0, 0, 0, 0, 0, 0, 0, 0x7
    };
    return common::siphash(
        key, reinterpret_cast<const unsigned char*>(&val), sizeof(val));
}

// The high 25 bit in this register are used for the index, the next 6 bits for
// the value and the last bit is currently unused
using SparseRegister = uint32_t;

enum class RegisterFormat { SPARSE, DENSE };

constexpr uint64_t lowerNBitMask(uint64_t n) {
    return (static_cast<uint64_t>(1) << n) - 1;
}

constexpr uint32_t upperNBitMask(uint32_t n) {
    return ~(static_cast<uint32_t>((1 << (32 - n)) - 1));
}

template <size_t sparsePrecision, size_t densePrecision>
std::pair<size_t, uint8_t> decodeHash(SparseRegister reg) {
    static_assert(sparsePrecision >= densePrecision,
                  "densePrecision must not be greater than sparsePrecision");
    uint32_t denseValue;
    uint32_t lowestBit = reg & 1;
    uint32_t index;
    if (lowestBit == 1) {
        uint32_t sparseValue = (reg >> 1) & lowerNBitMask(31 - sparsePrecision);
        denseValue = sparseValue + (sparsePrecision - densePrecision);
        index = (reg >> (32 - densePrecision)) & lowerNBitMask(densePrecision);
    }
    else {
        // First zero bottom bits, then shift the bits used for the new index to
        // the left
        uint32_t topBitsValue =
            (reg & upperNBitMask(sparsePrecision)) << densePrecision;
        denseValue = __builtin_clz(topBitsValue) + 1;
        index = (reg >> (32 - densePrecision)) & lowerNBitMask(densePrecision);
    }
    return std::make_pair(index, denseValue);
}

template <size_t precision>
std::pair<uint32_t, uint8_t> splitSparseRegister(const SparseRegister& reg) {
    uint8_t value = (reg & lowerNBitMask(32 - precision)) >> 1;
    uint32_t idx = reg >> (32 - precision);
    return std::make_pair(idx, value);
}

template <size_t sparsePrecision, size_t densePrecision>
uint32_t encodePair(uint32_t index, uint8_t value) {
    // x_{63 - densePrecision}...x_{64 - sparsePrecision}
    uint32_t decidingBits =
        lowerNBitMask(sparsePrecision - densePrecision) & index;
    if (decidingBits == 0) {
        return index << (32 - sparsePrecision) | (value << 1) | 1;
    }
    else {
        // x_63...x_{64 - sparsePrecision} || 0
        uint32_t shifted = index << (32 - sparsePrecision);
        return shifted;
    }
}

template <size_t sparsePrecision, size_t densePrecision>
uint32_t encodeHash(uint64_t hash) {
    static_assert(sparsePrecision <= 32,
                  "sparse precision must be smaller than 32");
    static_assert(densePrecision < sparsePrecision,
                  "dense precision must be smaller than sparse precision");

    // precision bits are used for the index, the rest is used as the value
    uint64_t valueBits = hash << sparsePrecision;
    static_assert(sizeof(long long) * CHAR_BIT == 64,
                  "64 bit long long are required for hyperloglog.");
    uint8_t leadingZeroes =
        valueBits == 0 ? (64 - sparsePrecision) : __builtin_clzll(valueBits);
    uint32_t index = (hash >> (64 - sparsePrecision));
    return encodePair<sparsePrecision, densePrecision>(index, leadingZeroes + 1);
}

template <size_t precision>
std::vector<SparseRegister>
mergeSameIndices(const std::vector<SparseRegister>& sparseList) {
    if (sparseList.empty()) {
        return { };
    }
    auto it = sparseList.begin();
    std::vector<SparseRegister> mergedSparseList = { *it };
    ++it;
    std::pair<size_t, uint8_t> lastEntry =
        splitSparseRegister<precision>(mergedSparseList.back());
    for ( ; it != sparseList.end(); ++it) {
        auto decoded = splitSparseRegister<precision>(*it);
        assert(decoded.first >= lastEntry.first);
        if (decoded.first > lastEntry.first) {
            mergedSparseList.emplace_back(*it);
        }
        else {
            assert(decoded.second >= lastEntry.second);
            mergedSparseList.back() = *it;
        }
        lastEntry = decoded;
    }
    return mergedSparseList;
}

class VectorWriter : public common::ItemWriterToolsBase<VectorWriter>
{
    std::vector<uint8_t>& buffer;

public:
    VectorWriter(std::vector<uint8_t>& buffer) : buffer(buffer) { }
    VectorWriter& PutByte(uint8_t data) {
        buffer.emplace_back(data);
        return *this;
    }
};

template <typename ForwardIt>
class SparseListIterator
    : public common::ItemReaderToolsBase<SparseListIterator<ForwardIt> >,
      public std::iterator<std::forward_iterator_tag, uint32_t, std::ptrdiff_t,
                           void, void>
{
    ForwardIt iterator;
    uint32_t lastVal = 0;
    static_assert(std::is_same<typename ForwardIt::value_type, uint8_t>::value,
                  "ForwardIt must return uint8_t");

public:
    explicit SparseListIterator(ForwardIt it) : iterator(it) { }
    uint8_t GetByte() { return *iterator++; }
    SparseListIterator<ForwardIt>& operator ++ () {
        lastVal = lastVal + this->GetVarint32();
        return *this;
    }
    SparseListIterator<ForwardIt> operator ++ (int) {
        SparseListIterator<ForwardIt> prev(*this);
        ++*this;
        return prev;
    }
    uint32_t operator * () {
        ForwardIt prevIt = iterator;
        uint32_t val = this->GetVarint32();
        iterator = prevIt;
        return lastVal + val;
    }
    bool operator == (const SparseListIterator<ForwardIt>& other) {
        return iterator == other.iterator;
    }
    bool operator != (const SparseListIterator<ForwardIt>& other) {
        return !(*this == other);
    }
};

template <typename ForwardIt>
SparseListIterator<ForwardIt> makeSparseListIterator(ForwardIt it) {
    return SparseListIterator<ForwardIt>(it);
}

class DecodedSparseList
{
    const std::vector<uint8_t>& sparseListBuffer;

public:
    DecodedSparseList(const std::vector<uint8_t>& sparseListBuf)
        : sparseListBuffer(sparseListBuf) { }
    auto begin() { return makeSparseListIterator(sparseListBuffer.begin()); }
    auto end() { return makeSparseListIterator(sparseListBuffer.end()); }
};

// Perform a varint and a difference encoding
std::vector<uint8_t> encodeSparseList(const std::vector<uint32_t>& sparseList);

extern const std::array<double, 15> thresholds;
extern const std::array<std::vector<double>, 15> rawEstimateData;
extern const std::array<std::vector<double>, 15> biasData;

template <size_t p>
constexpr double alpha() {
    return 0.7213 / (1 + 1.079 / (1 << p));
}
template <>
constexpr double alpha<4>() {
    return 0.673;
}
template <>
constexpr double alpha<5>() {
    return 0.697;
}
template <>
constexpr double alpha<6>() {
    return 0.709;
}

template <size_t p>
static double threshold() {
    return thresholds[p - 4];
}

template <size_t p>
double estimateBias(double rawEstimate);

template <size_t p>
class Registers
{
public:
    unsigned sparseSize = 0;
    RegisterFormat format;
    // Register values are always smaller than 64. We thus need log2(64) = 6
    // bits to store them. In particular an uint8_t is sufficient
    std::vector<uint8_t> sparseListBuffer;
    std::vector<SparseRegister> tmpSet;
    std::vector<uint8_t> entries;

    Registers() : format(RegisterFormat::SPARSE) { }

    size_t size() const { return entries.size(); }

    void toDense() {
        assert(format == RegisterFormat::SPARSE);
        format = RegisterFormat::DENSE;
        entries.resize(1 << p, 0);
        for (auto val : DecodedSparseList(sparseListBuffer)) {
            auto decoded = decodeHash<25, p>(val);
            auto entry = entries[decoded.first];
            auto value = std::max(entry, decoded.second);
            entries[decoded.first] = value;
        }

        for (auto& val : tmpSet) {
            auto decoded = decodeHash<25, p>(val);
            auto entry = entries[decoded.first];
            auto value = std::max(entry, decoded.second);
            entries[decoded.first] = value;
        }
        sparseListBuffer.clear();
        tmpSet.clear();
        sparseListBuffer.shrink_to_fit();
        tmpSet.shrink_to_fit();
    }
    bool shouldConvertToDense() {
        size_t tmpSize = tmpSet.size() * sizeof(SparseRegister);
        size_t sparseSize = tmpSize + sparseListBuffer.size() * sizeof(uint8_t);
        size_t denseSize = (1 << p) * sizeof(uint8_t);
        return sparseSize > denseSize;
    }

    bool shouldMerge() {
        size_t tmpSize = tmpSet.size() * sizeof(SparseRegister);
        size_t denseSize = (1 << p) * sizeof(uint8_t);
        return tmpSize > (denseSize / 4);
    }

    template <typename ValueType>
    void insert(const ValueType& value) {
        // first p bits are the index
        uint64_t hashVal = hash<ValueType>(value);
        static_assert(sizeof(long long) * CHAR_BIT == 64,
                      "64 Bit long long are required for hyperloglog.");
        switch (format) {
        case RegisterFormat::SPARSE: {
            sparseSize++;
            tmpSet.emplace_back(encodeHash<25, p>(hashVal));

            if (shouldMerge()) {
                mergeSparse();
            }

            if (shouldConvertToDense()) {
                toDense();
            }
            break;
        }
        case RegisterFormat::DENSE:
            uint64_t index = hashVal >> (64 - p);
            uint64_t val = hashVal << p;
            uint8_t leadingZeroes = val == 0 ? (64 - p) : __builtin_clzll(val);
            assert(leadingZeroes >= 0 && leadingZeroes <= (64 - p));
            entries[index] =
                std::max<uint8_t>(leadingZeroes + 1, entries[index]);
            break;
        }
    }

    void mergeSparse() {
        DecodedSparseList sparseList(sparseListBuffer);
        assert(std::is_sorted(sparseList.begin(), sparseList.end()));
        std::sort(tmpSet.begin(), tmpSet.end());
        std::vector<SparseRegister> resultVec;
        std::merge(sparseList.begin(), sparseList.end(), tmpSet.begin(),
                   tmpSet.end(), std::back_inserter(resultVec));
        tmpSet.clear();
        tmpSet.shrink_to_fit();
        std::vector<SparseRegister> vec = mergeSameIndices<25>(resultVec);
        sparseSize = vec.size();
        sparseListBuffer = encodeSparseList(vec);
    }

    void mergeDense(const Registers<p>& b) {
        assert(format == RegisterFormat::DENSE);
        const size_t m = 1 << p;
        assert(m == size() && m == b.size());
        for (size_t i = 0; i < m; ++i) {
            entries[i] = std::max(entries[i], b.entries[i]);
        }
    }

    double result() {
        if (format == RegisterFormat::SPARSE) {
            mergeSparse();
            // 25 is precision of sparse representation
            const size_t m = 1 << 25;
            unsigned sparseListCount = sparseSize;
            unsigned V = m - sparseListCount;
            return m * log(static_cast<double>(m) / V);
        }

        const size_t m = 1 << p;
        assert(size() == m);

        double E = 0.0;
        unsigned V = 0;

        for (const uint64_t& entry : entries) {
            E += std::pow(2.0, -static_cast<double>(entry));
            V += (entry == 0 ? 1 : 0);
        }

        E = alpha<p>() * m * m / E;
        double E_ = E;
        if (E <= 5 * m) {
            double bias = estimateBias<p>(E);
            E_ -= bias;
        }

        double H = E_;
        if (V != 0) {
            // linear count
            H = m * log(static_cast<double>(m) / V);
        }

        if (H <= threshold<p>()) {
            return H;
        }
        else {
            return E_;
        }
    }
};

//! combine two HyperloglogRegisters, switches between sparse/dense
//! representations
template <size_t p>
Registers<p> operator + (
    const Registers<p>& registers1, const Registers<p>& registers2) {

    if (registers1.format == RegisterFormat::SPARSE &&
        registers2.format == RegisterFormat::SPARSE) {

        Registers<p> result = registers1;

        DecodedSparseList sparseList2(registers2.sparseListBuffer);
        result.tmpSet.insert(result.tmpSet.end(),
                             sparseList2.begin(), sparseList2.end());
        result.tmpSet.insert(result.tmpSet.end(),
                             registers2.tmpSet.begin(), registers2.tmpSet.end());
        result.mergeSparse();
        if (result.shouldConvertToDense()) {
            result.toDense();
        }
        return result;
    }
    else if (registers1.format == RegisterFormat::SPARSE &&
             registers2.format == RegisterFormat::DENSE) {

        Registers<p> result = registers1;
        result.toDense();
        result.mergeDense(registers2);
        return result;
    }
    else if (registers1.format == RegisterFormat::DENSE &&
             registers2.format == RegisterFormat::SPARSE) {

        Registers<p> result = registers2;
        result.toDense();
        result.mergeDense(registers1);
        return result;
    }
    else if (registers1.format == RegisterFormat::DENSE &&
             registers2.format == RegisterFormat::DENSE) {

        Registers<p> result = registers1;
        result.mergeDense(registers2);
        return result;
    }
    die("Impossible.");
}

namespace data {

template <typename Archive, size_t p>
struct Serialization<Archive, Registers<p>,
                     typename std::enable_if<p <= 16>::type>{
    static void Serialize(const Registers<p>& x, Archive& ar) {
        Serialization<Archive, RegisterFormat>::Serialize(x.format, ar);
        switch (x.format) {
        case RegisterFormat::SPARSE:
            Serialization<Archive, decltype(x.sparseListBuffer)>::Serialize(
                x.sparseListBuffer, ar);
            Serialization<Archive, decltype(x.tmpSet)>::Serialize(x.tmpSet, ar);
            break;
        case RegisterFormat::DENSE:
            for (auto it = x.entries.begin(); it != x.entries.end(); ++it) {
                Serialization<Archive, uint64_t>::Serialize(*it, ar);
            }
            break;
        }
    }
    static Registers<p> Deserialize(Archive& ar) {
        Registers<p> out;
        out.format =
            std::move(Serialization<Archive, RegisterFormat>::Deserialize(ar));
        switch (out.format) {
        case RegisterFormat::SPARSE:
            out.sparseListBuffer = std::move(
                Serialization<Archive,
                              decltype(out.sparseListBuffer)>::Deserialize(ar));
            out.tmpSet = std::move(
                Serialization<Archive, decltype(out.tmpSet)>::Deserialize(ar));
            break;
        case RegisterFormat::DENSE:
            out.entries.resize(1 << p);
            for (size_t i = 0; i != out.size(); ++i) {
                out.entries[i] = std::move(
                    Serialization<Archive, uint64_t>::Deserialize(ar));
            }
            break;
        }
        return out;
    }
    static constexpr bool   is_fixed_size = false;
    static constexpr size_t fixed_size = 0;
};

} // namespace data

namespace api {

/*!
 * \ingroup api_layer
 */
template <size_t p, typename ValueType>
class HyperLogLogNode final : public ActionResultNode<Registers<p> >
{
    static constexpr bool debug = false;

    using Super = ActionResultNode<Registers<p> >;
    using Super::context_;

public:
    template <typename ParentDIA>
    HyperLogLogNode(const ParentDIA& parent, const char* label)
        : Super(parent.ctx(), label, { parent.id() }, { parent.node() }) {
        // Hook PreOp(s)
        auto pre_op_fn = [this](const ValueType& input) {
                             registers.insert(input);
                         };

        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    //! Executes the sum operation.
    void Execute() final {
        // process the reduce
        registers = context_.net.AllReduce(registers);
    }

    //! Returns result of global sum.
    const Registers<p>& result() const final { return registers; }

private:
    Registers<p> registers;
};

template <typename ValueType, typename Stack>
template <size_t p>
double DIA<ValueType, Stack>::HyperLogLog() const {
    assert(IsValid());

    auto node = tlx::make_counting<HyperLogLogNode<p, ValueType> >(
        *this, "HyperLogLog");
    node->RunScope();
    auto registers = node->result();
    return registers.result();
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_HYPERLOGLOG_HEADER

/******************************************************************************/
