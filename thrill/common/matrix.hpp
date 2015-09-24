/*******************************************************************************
 * thrill/common/matrix.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_MATRIX_HEADER
#define THRILL_COMMON_MATRIX_HEADER

#include <thrill/data/serialization.hpp>

#include <algorithm>
#include <cassert>
#include <functional>
#include <vector>

namespace thrill {
namespace common {

/*!
 * A simple m x n dense matrix for generating statistics.
 */
template <typename Type>
class Matrix
{
public:
    //! empty matrix constructor
    Matrix() = default;

    //! constructor of m times n matrix.
    Matrix(size_t rows, size_t columns, const Type& initial = Type())
        : rows_(rows), columns_(columns),
          data_(rows_ * columns_, initial) { }

    //! constructor of square n times n matrix.
    explicit Matrix(size_t rows_columns, const Type& initial = Type())
        : Matrix(rows_columns, rows_columns, initial) { }

    //! constructor of m times n matrix from appropriate vector
    Matrix(size_t rows, size_t columns, std::vector<Type>&& data)
        : rows_(rows), columns_(columns),
          data_(std::move(data)) {
        assert(data_.size() == rows_ * columns_);
    }

    //! number of rows in matrix
    size_t rows() const { return rows_; }

    //! number of columns in matrix
    size_t columns() const { return columns_; }

    //! raw data of matrix
    Type * data() const { return data_.data(); }

    //! size of matrix raw data (rows * columns)
    size_t size() const { return data_.size(); }

    //! return reference to element at cell
    const Type & at(size_t row, size_t column) const {
        assert(row < rows_);
        assert(column < columns_);
        return data_[columns_ * row + column];
    }

    //! return reference to element at cell
    Type & at(size_t row, size_t column) {
        assert(row < rows_);
        assert(column < columns_);
        return data_[columns_ * row + column];
    }

    //! return reference to element at cell
    const Type& operator () (size_t row, size_t column) const {
        return at(row, column);
    }

    //! return reference to element at cell
    Type& operator () (size_t row, size_t column) {
        return at(row, column);
    }

    //! add matrix to this one
    Matrix& operator += (const Matrix& b) {
        assert(rows() == b.rows() && columns() == b.columns());
        std::transform(b.data_.begin(), b.data_.end(),
                       data_.begin(), data_.begin(), std::plus<Type>());
        return *this;
    }

    //! add two matrix to this one
    Matrix operator + (const Matrix& b) const {
        assert(rows() == b.rows() && columns() == b.columns());
        Matrix n = *this;
        return (n += b);
    }

    //! multiple matrix with a scalar
    Matrix& operator *= (const Type& s) {
        std::for_each(data_.begin(), data_.end(), [&s](Type& t) { t *= s; });
        return *this;
    }

    //! equality operator
    bool operator == (const Matrix& b) const noexcept {
        if (rows() != b.rows() || columns() != b.columns()) return false;
        return std::equal(data_.begin(), data_.end(), b.data_.begin());
    }

    //! inequality operator
    bool operator != (const Matrix& b) const noexcept {
        return !operator == (b);
    }

    /**************************************************************************/

    static const bool thrill_is_fixed_size = false;
    static const size_t thrill_fixed_size = 0;

    //! serialization with Thrill's serializer
    template <typename Archive>
    void ThrillSerialize(Archive& ar) const {
        ar.template Put<size_t>(rows_);
        ar.template Put<size_t>(columns_);
        for (typename std::vector<Type>::const_iterator it = data_.begin();
             it != data_.end(); ++it) {
            data::Serialization<Archive, Type>::Serialize(*it, ar);
        }
    }

    //! deserialization with Thrill's serializer
    template <typename Archive>
    static Matrix ThrillDeserialize(Archive& ar) {
        size_t rows = ar.template Get<size_t>();
        size_t columns = ar.template Get<size_t>();
        std::vector<Type> data;
        data.resize(0);
        data.reserve(rows * columns);
        for (size_t i = 0; i < rows * columns; ++i) {
            data.emplace_back(
                data::Serialization<Archive, Type>::Deserialize(ar));
        }
        return Matrix(rows, columns, std::move(data));
    }

private:
    //! number of rows in matrix
    size_t rows_ = 0;
    //! number of columns in matrix
    size_t columns_ = 0;

    //! data of matrix.
    std::vector<Type> data_;
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_MATRIX_HEADER

/******************************************************************************/
