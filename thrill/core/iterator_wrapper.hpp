/*******************************************************************************
 * thrill/core/iterator_wrapper.hpp
 *
 * Part of Project Thrill.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_ITERATOR_WRAPPER_HEADER
#define THRILL_CORE_ITERATOR_WRAPPER_HEADER
/******************************************************************************
 * src/SortAlgo.h
 *
 * Implementations of many sorting algorithms.
 *
 * Note that these implementations may not be as good/fast as possible. Some
 * are modified so that the visualization is more instructive.
 *
 * Futhermore, some algorithms are annotated using the mark() and watch()
 * functions from SortArray. These functions add colors to the illustratation
 * and thereby makes the algorithm's visualization easier to explain.
 *
 ******************************************************************************
 * Copyright (C) 2013-2014 Timo Bingmann <tb@panthema.net>
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 *****************************************************************************/

#ifndef ITERATOR_WRAPPER_HEADER
#define ITERATOR_WRAPPER_HEADER

#include <thrill/common/logger.hpp>
#include <thrill/data/file.hpp>

#include <iomanip>
#include <vector>

namespace thrill {
namespace core {

static const bool debug = false;

template <typename ArrayItem>
class StxxlFileOutputWrapper
{
    using File = typename data::File;
    using Writer = typename File::Writer;

protected:
    std::shared_ptr<Writer> writer_;

public:
    StxxlFileOutputWrapper(std::shared_ptr<Writer> writer) : writer_(writer) { }

    void operator () (const ArrayItem& a) const {
        (*writer_)(a);
    }
};

template <typename ArrayItem>
struct IterStats {
    bool      has_elem_ = false;
    bool      is_valid_ = true;
    ArrayItem item_;
};

// ****************************************************************************
// *** Iterator Adapter for file readers

// iterator based on http://zotu.blogspot.de/2010/01/creating-random-access-iterator.html

template <typename ArrayItem>
class StxxlFileWrapper : public std::iterator<std::random_access_iterator_tag, ArrayItem>
{
    using File = typename data::File;
    using Reader = typename File::Reader;

protected:
    File* file_;
    std::shared_ptr<Reader> reader_;
    // Reader* reader_;
    std::size_t pos_;

    std::shared_ptr<IterStats<ArrayItem> > stats_;

public:
    using base_type = std::iterator<std::random_access_iterator_tag, ArrayItem>;

    using iterator_category = std::random_access_iterator_tag;

    using value_type = typename base_type::value_type;
    using difference_type = typename base_type::difference_type;
    using reference = typename base_type::reference;
    using pointer = typename base_type::pointer;

    void GetItem() const {
        assert(reader_->HasNext());
        stats_->item_ = reader_->Next<ArrayItem>();
        stats_->has_elem_ = true;

        LOG << "-----------------------------------------";
        LOG << "NEXT GOT " << stats_->item_ << " FROM " << file_->ToString();
        LOG << "-----------------------------------------";
    }

    void Invalidate() const {
        stats_->is_valid_ = false;
        stats_->has_elem_ = false;

        LOG << "-----------------------------------------";
        LOG << "INVALIDATING " << pos_ << " " << file_->ToString();
        LOG << "-----------------------------------------";
    }

    void GetItemOrInvalidate() const {
        if (reader_->HasNext()) {
            GetItem();
        }
        else {
            Invalidate();
        }
    }

    StxxlFileWrapper() : file_(), pos_(0) { }

    StxxlFileWrapper(File* file, std::shared_ptr<Reader> reader, std::size_t pos, bool valid = true)
    // StxxlFileWrapper(File* file, Reader* reader, std::size_t pos, bool valid=true)
        : file_(file),
          reader_(reader),
          pos_(pos),
          stats_(std::make_shared<IterStats<ArrayItem> >(IterStats<ArrayItem>())) {

        stats_->is_valid_ = valid;

        LOG << "    Created iterator";
        LOG << "        " << std::left << std::setw(7) << "pos: " << pos_;
        LOG << "        " << std::left << std::setw(7) << "file: " << file_->ToString();
        LOG << "        " << std::left << std::setw(7) << "valid: " << std::boolalpha << stats_->is_valid_;

        if (stats_->is_valid_ && !stats_->has_elem_) {
            GetItemOrInvalidate();
        }
    }

    // StxxlFileWrapper(const StxxlFileWrapper& r) {}
    StxxlFileWrapper& operator = (const StxxlFileWrapper& r) {
        file_ = r.file_;
        reader_ = r.reader_;
        stats_ = r.stats_;
        LOG << "    Operator=";
        LOG << "        " << std::left << std::setw(7) << "pos: " << pos_;
        LOG << "        " << std::left << std::setw(7) << "file: " << file_->ToString();
        LOG << "        " << std::left << std::setw(7) << "valid: " << std::boolalpha << stats_->is_valid_;
        return *this;
    }

    StxxlFileWrapper& operator ++ () {
        GetItemOrInvalidate();
        ++pos_;

        LOG << "    Operator++";
        LOG << "        " << std::left << std::setw(7) << "pos: " << pos_;
        LOG << "        " << std::left << std::setw(7) << "file: " << file_->ToString();
        LOG << "        " << std::left << std::setw(7) << "valid: " << std::boolalpha << stats_->is_valid_;
        return *this;
    }

    // StxxlFileWrapper& operator--() {
    //     Invalidate();
    //     --pos_;

    //     LOG << "    Operator--";
    //     LOG << "        " << std::left << std::setw(7) << "pos: " << pos_;
    //     LOG << "        " << std::left << std::setw(7) << "file: " << file_->ToString();
    //     LOG << "        " << std::left << std::setw(7) << "valid: " << std::boolalpha << stats_->is_valid_;
    //     return *this;
    // }

    StxxlFileWrapper operator ++ (int) {
        auto r = StxxlFileWrapper(file_, reader_, ++pos_, stats_.is_valid_);
        LOG << "    Operator++ (postfix)";
        LOG << "        " << std::left << std::setw(7) << "pos: " << r.pos_;
        LOG << "        " << std::left << std::setw(7) << "file: " << r.file_->ToString();
        LOG << "        " << std::left << std::setw(7) << "valid: " << std::boolalpha << r.stats_->is_valid_;
        return r;
    }

    // StxxlFileWrapper operator--(int) {}

    StxxlFileWrapper operator + (const difference_type& n) const {
        auto w = StxxlFileWrapper(file_, reader_, pos_ + n, stats_->is_valid_);
        for (difference_type t = 0; t < n - 1; ++t) {
            w.GetItemOrInvalidate();
        }

        LOG << "    Operator+ " << n;
        LOG << "        " << std::left << std::setw(7) << "pos: " << w.pos_;
        LOG << "        " << std::left << std::setw(7) << "file: " << w.file_->ToString();
        LOG << "        " << std::left << std::setw(7) << "valid: " << std::boolalpha << w.stats_->is_valid_;
        return w;
    }

    StxxlFileWrapper& operator += (const difference_type& n) {
        pos_ += n;
        for (difference_type t = 0; t < n; ++t) {
            GetItemOrInvalidate();
        }

        LOG << "    Operator+= " << n;
        LOG << "        " << std::left << std::setw(7) << "pos: " << pos_;
        LOG << "        " << std::left << std::setw(7) << "file: " << file_->ToString();
        LOG << "        " << std::left << std::setw(7) << "valid: " << std::boolalpha << stats_->is_valid_;
        return *this;
    }

    // StxxlFileWrapper operator-(const difference_type& n) const {
    //     auto w = StxxlFileWrapper(file_, reader_, pos_-n, false);
    //     LOG << "    Operator- " << n;
    //     LOG << "        " << std::left << std::setw(7) << "pos: " << w.pos_;
    //     LOG << "        " << std::left << std::setw(7) << "file: " << w.file_->ToString();
    //     LOG << "        " << std::left << std::setw(7) << "valid: " << std::boolalpha << w.stats_->is_valid_;
    //     return w;
    // }

    // StxxlFileWrapper& operator-=(const difference_type& n) {}

    reference operator * () const {
        LOG << "-----------------------------------------";
        LOG << "Trying to return " << stats_->item_ << " from " << pos_ << " " << file_->ToString();
        LOG << "-----------------------------------------";
        assert(stats_->is_valid_);

        LOG << "-----------------------------------------";
        LOG << "RETURN " << stats_->item_ << " FROM " << pos_ << " " << file_->ToString();
        LOG << "-----------------------------------------";
        LOG << "    Operator* ";
        LOG << "        " << std::left << std::setw(7) << "pos: " << pos_;
        LOG << "        " << std::left << std::setw(7) << "file: " << file_->ToString();
        LOG << "        " << std::left << std::setw(7) << "valid: " << std::boolalpha << stats_->is_valid_;
        LOG << "        " << std::left << std::setw(7) << "item: " << stats_->item_;
        return stats_->item_;
    }
    // pointer operator->() const {
    // reference operator[](const difference_type& n) const {}

    bool operator == (const StxxlFileWrapper& r) {
        LOG << "    Operator== ";
        LOG << "        " << std::left << std::setw(7) << "pos: " << pos_;
        LOG << "        " << std::left << std::setw(7) << "pos2: " << r.pos_;
        LOG << "        " << std::left << std::setw(7) << "file: " << file_->ToString();
        LOG << "        " << std::left << std::setw(7) << "file2: " << r.file_->ToString();
        LOG << "        " << std::left << std::setw(7) << "result: " << std::boolalpha << ((file_ == r.file_) && (pos_ == r.pos_));
        return (file_ == r.file_) && (pos_ == r.pos_);
    }

    bool operator != (const StxxlFileWrapper& r) {
        LOG << "    Operator!= ";
        LOG << "        " << std::left << std::setw(7) << "pos: " << pos_;
        LOG << "        " << std::left << std::setw(7) << "pos2: " << r.pos_;
        LOG << "        " << std::left << std::setw(7) << "file: " << file_->ToString();
        LOG << "        " << std::left << std::setw(7) << "file2: " << r.file_->ToString();
        LOG << "        " << std::left << std::setw(7) << "result: " << std::boolalpha << ((file_ != r.file_) || (pos_ != r.pos_));
        return (file_ != r.file_) || (pos_ != r.pos_);
    }

    // bool operator<(const StxxlFileWrapper& r)
    // { return (m_array == r.m_array ? (m_pos < r.m_pos) : (m_array < r.m_array)); }

    // bool operator>(const StxxlFileWrapper& r)
    // { return (m_array == r.m_array ? (m_pos > r.m_pos) : (m_array > r.m_array)); }

    // bool operator<=(const StxxlFileWrapper& r)
    // { return (m_array == r.m_array ? (m_pos <= r.m_pos) : (m_array <= r.m_array)); }

    // bool operator>=(const StxxlFileWrapper& r)
    // { return (m_array == r.m_array ? (m_pos >= r.m_pos) : (m_array >= r.m_array)); }

    // difference_type operator+(const StxxlFileWrapper& r2) const
    // { return (pos_ + r2.pos_); }

    difference_type operator - (const StxxlFileWrapper& r2) const {
        LOG << "    Operator-  StxxlFileWrapper";
        LOG << "        " << std::left << std::setw(7) << "pos: " << pos_;
        LOG << "        " << std::left << std::setw(7) << "pos2: " << r2.pos_;
        LOG << "        " << std::left << std::setw(7) << "result: " << pos_ - r2.pos_;
        return (pos_ - r2.pos_);
    }
};

// ****************************************************************************
// *** Iterator Adapter for vectors

// iterator based on http://zotu.blogspot.de/2010/01/creating-random-access-iterator.html

template <typename ArrayItem>
class StxxlVectorWrapper : public std::iterator<std::random_access_iterator_tag, ArrayItem>
{
protected:
    // SortArray*  m_array;
    std::vector<ArrayItem>* m_array;
    size_t m_pos;

public:
    using base_type = std::iterator<std::random_access_iterator_tag, ArrayItem>;

    using iterator_category = std::random_access_iterator_tag;

    using value_type = typename base_type::value_type;
    using difference_type = typename base_type::difference_type;
    using reference = typename base_type::reference;
    using pointer = typename base_type::pointer;

    StxxlVectorWrapper() : m_array(nullptr), m_pos(0) { }

    StxxlVectorWrapper(std::vector<ArrayItem>* A, size_t p) : m_array(A), m_pos(p) { }

    StxxlVectorWrapper(const StxxlVectorWrapper& r) : m_array(r.m_array), m_pos(r.m_pos) { }

    StxxlVectorWrapper& operator = (const StxxlVectorWrapper& r)
    { m_array = r.m_array, m_pos = r.m_pos; return *this; }

    StxxlVectorWrapper& operator ++ ()
    { ++m_pos; return *this; }

    StxxlVectorWrapper& operator -- ()
    { --m_pos; return *this; }

    StxxlVectorWrapper operator ++ (int)
    { return StxxlVectorWrapper(m_array, m_pos++); }

    StxxlVectorWrapper operator -- (int)
    { return StxxlVectorWrapper(m_array, m_pos--); }

    StxxlVectorWrapper operator + (const difference_type& n) const
    { return StxxlVectorWrapper(m_array, m_pos + n); }

    StxxlVectorWrapper& operator += (const difference_type& n)
    { m_pos += n; return *this; }

    StxxlVectorWrapper operator - (const difference_type& n) const
    { return StxxlVectorWrapper(m_array, m_pos - n); }

    StxxlVectorWrapper& operator -= (const difference_type& n)
    { m_pos -= n; return *this; }

    reference operator * () const
    { return m_array->at(m_pos); }

    pointer operator -> () const
    { return &(m_array->at(m_pos)); }

    reference operator [] (const difference_type& n) const
    { return m_array->at(n); }

    bool operator == (const StxxlVectorWrapper& r)
    { return (m_array == r.m_array) && (m_pos == r.m_pos); }

    bool operator != (const StxxlVectorWrapper& r)
    { return (m_array != r.m_array) || (m_pos != r.m_pos); }

    bool operator < (const StxxlVectorWrapper& r)
    { return (m_array == r.m_array ? (m_pos < r.m_pos) : (m_array < r.m_array)); }

    bool operator > (const StxxlVectorWrapper& r)
    { return (m_array == r.m_array ? (m_pos > r.m_pos) : (m_array > r.m_array)); }

    bool operator <= (const StxxlVectorWrapper& r)
    { return (m_array == r.m_array ? (m_pos <= r.m_pos) : (m_array <= r.m_array)); }

    bool operator >= (const StxxlVectorWrapper& r)
    { return (m_array == r.m_array ? (m_pos >= r.m_pos) : (m_array >= r.m_array)); }

    difference_type operator + (const StxxlVectorWrapper& r2) const
    { return (m_pos + r2.m_pos); }

    difference_type operator - (const StxxlVectorWrapper& r2) const
    { return (m_pos - r2.m_pos); }
};
} //end namespace core
#endif // !THRILL_CORE_ITERATOR_WRAPPER_HEADER
} //end namespace thrill

#endif // !THRILL_CORE_ITERATOR_WRAPPER_HEADER

/******************************************************************************/
