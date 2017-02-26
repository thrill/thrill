/*******************************************************************************
 * thrill/common/vector.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_VECTOR_HEADER
#define THRILL_COMMON_VECTOR_HEADER

#include <thrill/data/serialization_cereal.hpp>

#include <algorithm>
#include <cassert>
#include <functional>
#include <vector>

namespace thrill {
namespace common {

//! A compile-time fixed-length D-dimensional point with double precision.
template <size_t D, typename Type = double>
class Vector
{
public:
    //! coordinates array
    Type x[D];

    using type = Type;

    static size_t dim() { return D; }

    static Vector Make(size_t D_) {
        die_unless(D_ == D);
        return Vector();
    }
    static Vector Origin() {
        Vector p;
        std::fill(p.x, p.x + D, 0.0);
        return p;
    }
    template <typename Distribution, typename Generator>
    static Vector Random(size_t dim, Distribution& dist, Generator& gen) {
        die_unless(dim == D);
        Vector p;
        for (size_t i = 0; i < D; ++i) p.x[i] = dist(gen);
        return p;
    }
    Type DistanceSquare(const Vector& b) const {
        Type sum = 0.0;
        for (size_t i = 0; i < D; ++i) sum += (x[i] - b.x[i]) * (x[i] - b.x[i]);
        return sum;
    }
    Type Distance(const Vector& b) const {
        return std::sqrt(DistanceSquare(b));
    }
    Vector operator + (const Vector& b) const {
        Vector p;
        for (size_t i = 0; i < D; ++i) p.x[i] = x[i] + b.x[i];
        return p;
    }
    Vector& operator += (const Vector& b) {
        for (size_t i = 0; i < D; ++i) x[i] += b.x[i];
        return *this;
    }
    Vector operator / (const Type& s) const {
        Vector p;
        for (size_t i = 0; i < D; ++i) p.x[i] = x[i] / s;
        return p;
    }
    Vector& operator /= (const Type& s) {
        for (size_t i = 0; i < D; ++i) x[i] /= s;
        return *this;
    }
    friend std::ostream& operator << (std::ostream& os, const Vector& a) {
        os << '(' << a.x[0];
        for (size_t i = 1; i != D; ++i) os << ',' << a.x[i];
        return os << ')';
    }
};

//! A variable-length D-dimensional point with double precision
template <typename Type = double>
class VVector
{
public:
    using TypeVector = std::vector<Type>;

    using type = Type;

    //! coordinates array
    TypeVector x;

    explicit VVector(size_t D = 0) : x(D) { }
    explicit VVector(TypeVector&& v) : x(std::move(v)) { }

    size_t dim() const { return x.size(); }

    static VVector Make(size_t D) {
        return VVector(D);
    }
    template <typename Distribution, typename Generator>
    static VVector Random(size_t D, Distribution& dist, Generator& gen) {
        VVector p(D);
        for (size_t i = 0; i < D; ++i) p.x[i] = dist(gen);
        return p;
    }
    Type DistanceSquare(const VVector& b) const {
        assert(x.size() == b.x.size());
        Type sum = 0.0;
        for (size_t i = 0; i < x.size(); ++i)
            sum += (x[i] - b.x[i]) * (x[i] - b.x[i]);
        return sum;
    }
    Type Distance(const VVector& b) const {
        return std::sqrt(DistanceSquare(b));
    }
    VVector operator + (const VVector& b) const {
        assert(x.size() == b.x.size());
        VVector p(x.size());
        for (size_t i = 0; i < x.size(); ++i) p.x[i] = x[i] + b.x[i];
        return p;
    }
    VVector& operator += (const VVector& b) {
        assert(x.size() == b.x.size());
        for (size_t i = 0; i < x.size(); ++i) x[i] += b.x[i];
        return *this;
    }
    VVector operator / (const Type& s) const {
        VVector p(x.size());
        for (size_t i = 0; i < x.size(); ++i) p.x[i] = x[i] / s;
        return p;
    }
    VVector& operator /= (const Type& s) {
        for (size_t i = 0; i < x.size(); ++i) x[i] /= s;
        return *this;
    }
    friend std::ostream& operator << (std::ostream& os, const VVector& a) {
        os << '(' << a.x[0];
        for (size_t i = 1; i != a.x.size(); ++i) os << ',' << a.x[i];
        return os << ')';
    }

    template <typename Archive>
    void serialize(Archive& archive) {
        archive(x);
    }
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_VECTOR_HEADER

/******************************************************************************/
