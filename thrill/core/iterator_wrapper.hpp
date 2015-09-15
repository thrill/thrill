/*******************************************************************************
 * thrill/core/iterator_wrapper.hpp
 *
 * Part of Project Thrill.
 *
 * Provides iterator wrapper for files
 *
 * Extracted and modified from Sound of Sorting (SortAlgo.h)
 * http://panthema.net/2013/sound-of-sorting/
 * https://github.com/bingmann/sound-of-sorting
 *
 * Copyright (C) 2013-2014 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Huyen Chau Nguyen <hello@chau-nguyen.de>
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_ITERATOR_WRAPPER_HEADER
#define THRILL_CORE_ITERATOR_WRAPPER_HEADER
<<<<<<< HEAD
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
 *****************************************************************************/
=======
>>>>>>> edf6ec2... add proper copyright and license info

#include <thrill/common/logger.hpp>
#include <thrill/data/file.hpp>

#include <iomanip>
#include <vector>

namespace thrill {
namespace core {

// *****************************************************************************
// *** Outpur Iterator Adapter for file writers
// iterator based on http://zotu.blogspot.de/2010/01/creating-random-access-iterator.html
// *****************************************************************************


template <typename ArrayItem>
class FileOutputIteratorWrapper
{
    static const bool debug = false;
    using File = typename data::File;
    using Writer = typename File::Writer;

protected:
    std::shared_ptr<Writer> writer_;

public:
    FileOutputIteratorWrapper(std::shared_ptr<Writer> writer) : writer_(writer) { }

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

// *****************************************************************************
// *** Iterator Adapter for file readers
// iterator based on http://zotu.blogspot.de/2010/01/creating-random-access-iterator.html
// *****************************************************************************

template <typename ArrayItem>
class FileIteratorWrapper : public std::iterator<std::random_access_iterator_tag, ArrayItem>
{
    static const bool debug = false;
    using File = typename data::File;
    using Reader = typename File::Reader;

protected:
    File* file_;
    std::shared_ptr<Reader> reader_;
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

    FileIteratorWrapper() : file_(), pos_(0) { }

    FileIteratorWrapper(File* file, std::shared_ptr<Reader> reader, std::size_t pos, bool valid = true)
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

    // FileIteratorWrapper(const FileIteratorWrapper& r) {}

    FileIteratorWrapper& operator = (const FileIteratorWrapper& r) {
        file_ = r.file_;
        reader_ = r.reader_;
        stats_ = r.stats_;
        LOG << "    Operator=";
        LOG << "        " << std::left << std::setw(7) << "pos: " << pos_;
        LOG << "        " << std::left << std::setw(7) << "file: " << file_->ToString();
        LOG << "        " << std::left << std::setw(7) << "valid: " << std::boolalpha << stats_->is_valid_;
        return *this;
    }

    FileIteratorWrapper& operator ++ () {
        GetItemOrInvalidate();
        ++pos_;

        LOG << "    Operator++";
        LOG << "        " << std::left << std::setw(7) << "pos: " << pos_;
        LOG << "        " << std::left << std::setw(7) << "file: " << file_->ToString();
        LOG << "        " << std::left << std::setw(7) << "valid: " << std::boolalpha << stats_->is_valid_;
        return *this;
    }

    // FileIteratorWrapper& operator--() {
    //     Invalidate();
    //     --pos_;

    //     LOG << "    Operator--";
    //     LOG << "        " << std::left << std::setw(7) << "pos: " << pos_;
    //     LOG << "        " << std::left << std::setw(7) << "file: " << file_->ToString();
    //     LOG << "        " << std::left << std::setw(7) << "valid: " << std::boolalpha << stats_->is_valid_;
    //     return *this;
    // }

    FileIteratorWrapper operator ++ (int) {
        auto r = FileIteratorWrapper(file_, reader_, ++pos_, stats_.is_valid_);
        LOG << "    Operator++ (postfix)";
        LOG << "        " << std::left << std::setw(7) << "pos: " << r.pos_;
        LOG << "        " << std::left << std::setw(7) << "file: " << r.file_->ToString();
        LOG << "        " << std::left << std::setw(7) << "valid: " << std::boolalpha << r.stats_->is_valid_;
        return r;
    }

    // FileIteratorWrapper operator--(int) {}

    FileIteratorWrapper operator + (const difference_type& n) const {
        auto w = FileIteratorWrapper(file_, reader_, pos_ + n, stats_->is_valid_);
        for (difference_type t = 0; t < n - 1; ++t) {
            w.GetItemOrInvalidate();
        }

        LOG << "    Operator+ " << n;
        LOG << "        " << std::left << std::setw(7) << "pos: " << w.pos_;
        LOG << "        " << std::left << std::setw(7) << "file: " << w.file_->ToString();
        LOG << "        " << std::left << std::setw(7) << "valid: " << std::boolalpha << w.stats_->is_valid_;
        return w;
    }

    FileIteratorWrapper& operator += (const difference_type& n) {
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

    // FileIteratorWrapper operator-(const difference_type& n) const {
    //     auto w = FileIteratorWrapper(file_, reader_, pos_-n, false);
    //     LOG << "    Operator- " << n;
    //     LOG << "        " << std::left << std::setw(7) << "pos: " << w.pos_;
    //     LOG << "        " << std::left << std::setw(7) << "file: " << w.file_->ToString();
    //     LOG << "        " << std::left << std::setw(7) << "valid: " << std::boolalpha << w.stats_->is_valid_;
    //     return w;
    // }

    // FileIteratorWrapper& operator-=(const difference_type& n) {}

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

    bool operator == (const FileIteratorWrapper& r) {
        LOG << "    Operator== ";
        LOG << "        " << std::left << std::setw(7) << "pos: " << pos_;
        LOG << "        " << std::left << std::setw(7) << "pos2: " << r.pos_;
        LOG << "        " << std::left << std::setw(7) << "file: " << file_->ToString();
        LOG << "        " << std::left << std::setw(7) << "file2: " << r.file_->ToString();
        LOG << "        " << std::left << std::setw(7) << "result: " << std::boolalpha << ((file_ == r.file_) && (pos_ == r.pos_));
        return (file_ == r.file_) && (pos_ == r.pos_);
    }

    bool operator != (const FileIteratorWrapper& r) {
        LOG << "    Operator!= ";
        LOG << "        " << std::left << std::setw(7) << "pos: " << pos_;
        LOG << "        " << std::left << std::setw(7) << "pos2: " << r.pos_;
        LOG << "        " << std::left << std::setw(7) << "file: " << file_->ToString();
        LOG << "        " << std::left << std::setw(7) << "file2: " << r.file_->ToString();
        LOG << "        " << std::left << std::setw(7) << "result: " << std::boolalpha << ((file_ != r.file_) || (pos_ != r.pos_));
        return (file_ != r.file_) || (pos_ != r.pos_);
    }

    // bool operator<(const FileIteratorWrapper& r)
    // { return (m_array == r.m_array ? (m_pos < r.m_pos) : (m_array < r.m_array)); }

    // bool operator>(const FileIteratorWrapper& r)
    // { return (m_array == r.m_array ? (m_pos > r.m_pos) : (m_array > r.m_array)); }

    // bool operator<=(const FileIteratorWrapper& r)
    // { return (m_array == r.m_array ? (m_pos <= r.m_pos) : (m_array <= r.m_array)); }

    // bool operator>=(const FileIteratorWrapper& r)
    // { return (m_array == r.m_array ? (m_pos >= r.m_pos) : (m_array >= r.m_array)); }

    // difference_type operator+(const FileIteratorWrapper& r2) const
    // { return (pos_ + r2.pos_); }

    difference_type operator - (const FileIteratorWrapper& r2) const {
        LOG << "    Operator-  FileIteratorWrapper";
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
class VectorIteratorWrapper : public std::iterator<std::random_access_iterator_tag, ArrayItem>
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

    VectorIteratorWrapper() : m_array(nullptr), m_pos(0) { }

    VectorIteratorWrapper(std::vector<ArrayItem>* A, size_t p) : m_array(A), m_pos(p) { }

    VectorIteratorWrapper(const VectorIteratorWrapper& r) : m_array(r.m_array), m_pos(r.m_pos) { }

    VectorIteratorWrapper& operator = (const VectorIteratorWrapper& r)
    { m_array = r.m_array, m_pos = r.m_pos; return *this; }

    VectorIteratorWrapper& operator ++ ()
    { ++m_pos; return *this; }

    VectorIteratorWrapper& operator -- ()
    { --m_pos; return *this; }

    VectorIteratorWrapper operator ++ (int)
    { return VectorIteratorWrapper(m_array, m_pos++); }

    VectorIteratorWrapper operator -- (int)
    { return VectorIteratorWrapper(m_array, m_pos--); }

    VectorIteratorWrapper operator + (const difference_type& n) const
    { return VectorIteratorWrapper(m_array, m_pos + n); }

    VectorIteratorWrapper& operator += (const difference_type& n)
    { m_pos += n; return *this; }

    VectorIteratorWrapper operator - (const difference_type& n) const
    { return VectorIteratorWrapper(m_array, m_pos - n); }

    VectorIteratorWrapper& operator -= (const difference_type& n)
    { m_pos -= n; return *this; }

    reference operator * () const
    { return m_array->at(m_pos); }

    pointer operator -> () const
    { return &(m_array->at(m_pos)); }

    reference operator [] (const difference_type& n) const
    { return m_array->at(n); }

    bool operator == (const VectorIteratorWrapper& r)
    { return (m_array == r.m_array) && (m_pos == r.m_pos); }

    bool operator != (const VectorIteratorWrapper& r)
    { return (m_array != r.m_array) || (m_pos != r.m_pos); }

    bool operator < (const VectorIteratorWrapper& r)
    { return (m_array == r.m_array ? (m_pos < r.m_pos) : (m_array < r.m_array)); }

    bool operator > (const VectorIteratorWrapper& r)
    { return (m_array == r.m_array ? (m_pos > r.m_pos) : (m_array > r.m_array)); }

    bool operator <= (const VectorIteratorWrapper& r)
    { return (m_array == r.m_array ? (m_pos <= r.m_pos) : (m_array <= r.m_array)); }

    bool operator >= (const VectorIteratorWrapper& r)
    { return (m_array == r.m_array ? (m_pos >= r.m_pos) : (m_array >= r.m_array)); }

    difference_type operator + (const VectorIteratorWrapper& r2) const
    { return (m_pos + r2.m_pos); }

    difference_type operator - (const VectorIteratorWrapper& r2) const
    { return (m_pos - r2.m_pos); }
};
} //end namespace core
} //end namespace thrill

#endif // !THRILL_CORE_ITERATOR_WRAPPER_HEADER

/******************************************************************************/
