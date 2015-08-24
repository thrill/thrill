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

// #include <wx/string.h>
// #include "array_wrapper.hpp"
#include <thrill/data/file.hpp>

#include <vector>

namespace thrill {
namespace core {

// ****************************************************************************
// *** Iterator Adapter for file readers

// iterator based on http://zotu.blogspot.de/2010/01/creating-random-access-iterator.html

// REVIEW(cn): this is the main problem: we must remove ALL methods except
// operator*, operator++, and all the pure calculation ones. operator[] is
// IMPOSSIBLE in a real external memory File. In theory, multiway merge does not
// need op[], so we have to find out why the multiway merge does need it and
// remove that.

// more REVIEW(cn): you included the WHOLE STXXL IN THE GIT REPOSITORY. NEVER DO
// THAT, it is utterly useless and a waste of repo space. You were supposed to
// take only the two files losertree.h and multiway_merge.h and make them
// compile without all the rest. And I mean without ALL the rest.

template <typename ArrayItem>
class StxxlFileWrapper : public std::iterator<std::random_access_iterator_tag, ArrayItem>
{
using File = typename data::File;
using Reader = typename File::Reader;


protected:
    File* file_;
    Reader* reader_;
    ArrayItem item_;
    size_t pos_;
    bool valid_ = true;

public:
    typedef std::iterator<std::random_access_iterator_tag, ArrayItem> base_type;

    typedef std::random_access_iterator_tag iterator_category;

    typedef typename base_type::value_type value_type;
    typedef typename base_type::difference_type difference_type;
    typedef typename base_type::reference reference;
    typedef typename base_type::pointer pointer;

    StxxlFileWrapper() : file_(), pos_(0) {}

    StxxlFileWrapper(File* F, Reader* R, size_t p) : file_(F), reader_(R), pos_(p) {
        if (reader_->HasNext()) {
            item_ = reader_->Next<ArrayItem>();
        }
    }

    StxxlFileWrapper(File* F, Reader* R, size_t p, bool b) : file_(F), reader_(R), pos_(p), valid_(b) {
        if (reader_->HasNext()) {
            item_ = reader_->Next<ArrayItem>();
        }
    }

    StxxlFileWrapper(const StxxlFileWrapper& r) :
        file_(r.file_), reader_(r.reader_), item_(r.item_), pos_(r.pos_) {}

    StxxlFileWrapper& operator=(const StxxlFileWrapper& r) {
        file_ = r.file_;
        reader_ = r.reader_;
        pos_ = r.pos_;
        return *this;
    }

    StxxlFileWrapper& operator++() {
        if (reader_->HasNext()) {
            item_ = reader_->Next<ArrayItem>();
        } else {
            valid_ = false;
        }
        ++pos_;
        return *this;
    }

    StxxlFileWrapper& operator--()
    { --pos_; valid_ = false; return *this; }

    StxxlFileWrapper operator++(int)
    { return StxxlFileWrapper(file_, reader_, ++pos_); }

    StxxlFileWrapper operator--(int)
    { return StxxlFileWrapper(file_, reader_, --pos_, false); }

    StxxlFileWrapper operator+(const difference_type& n) const {
        auto new_wrapper = *this;
        for (difference_type t = 0; t < n; ++t){
            // assert(new_wrapper.reader_->HasNext());
            new_wrapper++;
        }
        return new_wrapper;
    }

    StxxlFileWrapper& operator+=(const difference_type& n) {
        pos_ += n;
        for (difference_type t = 0; t < n; ++t){
            if (reader_->HasNext()) {
                item_ = reader_->Next<ArrayItem>();
            } else {
                valid_ = false;
            }
        }
        return *this;
    }

    StxxlFileWrapper operator-(const difference_type& n) const
    { return StxxlFileWrapper(file_, reader_, pos_-n, false); }

    StxxlFileWrapper& operator-=(const difference_type& n) {
        pos_ -= n;
        valid_ = false;
        return *this;
    }

    reference operator*() const {
        return const_cast<const reference>(item_);
    }

    pointer operator->() const {
        return &item_;
    }

    reference operator[](const difference_type& n) const {
        assert(pos_ <= n);
        if (pos_ == n) {
            return item_;
        } else {
            auto tmp = this;
            for (difference_type t = pos_; t < n; ++t){
                assert(tmp.reader_->HasNext());
                tmp++;
            }
            return tmp.item_;
        }
    }

    bool operator==(const StxxlFileWrapper& r)
    { return (file_ == r.file_) && (reader_ == r.reader_) && (pos_ == r.pos_); }

    bool operator!=(const StxxlFileWrapper& r)
    { return (file_ != r.file_) || (reader_ != r.reader_) || (pos_ != r.pos_); }

    // bool operator<(const StxxlFileWrapper& r)
    // { return (m_array == r.m_array ? (m_pos < r.m_pos) : (m_array < r.m_array)); }

    // bool operator>(const StxxlFileWrapper& r)
    // { return (m_array == r.m_array ? (m_pos > r.m_pos) : (m_array > r.m_array)); }

    // bool operator<=(const StxxlFileWrapper& r)
    // { return (m_array == r.m_array ? (m_pos <= r.m_pos) : (m_array <= r.m_array)); }

    // bool operator>=(const StxxlFileWrapper& r)
    // { return (m_array == r.m_array ? (m_pos >= r.m_pos) : (m_array >= r.m_array)); }

    difference_type operator+(const StxxlFileWrapper& r2) const
    { return (pos_ + r2.pos_); }

    difference_type operator-(const StxxlFileWrapper& r2) const
    { return (pos_ - r2.pos_); }
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
    size_t      m_pos;

public:
    typedef std::iterator<std::random_access_iterator_tag, ArrayItem> base_type;

    typedef std::random_access_iterator_tag iterator_category;

    typedef typename base_type::value_type value_type;
    typedef typename base_type::difference_type difference_type;
    typedef typename base_type::reference reference;
    typedef typename base_type::pointer pointer;

    StxxlVectorWrapper() : m_array(NULL), m_pos(0) {}

    StxxlVectorWrapper(std::vector<ArrayItem>* A, size_t p) : m_array(A), m_pos(p) {}

    StxxlVectorWrapper(const StxxlVectorWrapper& r) : m_array(r.m_array), m_pos(r.m_pos) {}

    StxxlVectorWrapper& operator=(const StxxlVectorWrapper& r)
    { m_array = r.m_array, m_pos = r.m_pos; return *this; }

    StxxlVectorWrapper& operator++()
    { ++m_pos; return *this; }

    StxxlVectorWrapper& operator--()
    { --m_pos; return *this; }

    StxxlVectorWrapper operator++(int)
    { return StxxlVectorWrapper(m_array, m_pos++); }

    StxxlVectorWrapper operator--(int)
    { return StxxlVectorWrapper(m_array, m_pos--); }

    StxxlVectorWrapper operator+(const difference_type& n) const
    { return StxxlVectorWrapper(m_array, m_pos + n); }

    StxxlVectorWrapper& operator+=(const difference_type& n)
    { m_pos += n; return *this; }

    StxxlVectorWrapper operator-(const difference_type& n) const
    { return StxxlVectorWrapper(m_array, m_pos - n); }

    StxxlVectorWrapper& operator-=(const difference_type& n)
    { m_pos -= n; return *this; }

    reference operator*() const
    { return m_array->at(m_pos); }

    pointer operator->() const
    { return &(m_array->at(m_pos)); }

    reference operator[](const difference_type& n) const
    { return m_array->at(n); }

    bool operator==(const StxxlVectorWrapper& r)
    { return (m_array == r.m_array) && (m_pos == r.m_pos); }

    bool operator!=(const StxxlVectorWrapper& r)
    { return (m_array != r.m_array) || (m_pos != r.m_pos); }

    bool operator<(const StxxlVectorWrapper& r)
    { return (m_array == r.m_array ? (m_pos < r.m_pos) : (m_array < r.m_array)); }

    bool operator>(const StxxlVectorWrapper& r)
    { return (m_array == r.m_array ? (m_pos > r.m_pos) : (m_array > r.m_array)); }

    bool operator<=(const StxxlVectorWrapper& r)
    { return (m_array == r.m_array ? (m_pos <= r.m_pos) : (m_array <= r.m_array)); }

    bool operator>=(const StxxlVectorWrapper& r)
    { return (m_array == r.m_array ? (m_pos >= r.m_pos) : (m_array >= r.m_array)); }

    difference_type operator+(const StxxlVectorWrapper& r2) const
    { return (m_pos + r2.m_pos); }

    difference_type operator-(const StxxlVectorWrapper& r2) const
    { return (m_pos - r2.m_pos); }
};


} //end namespace core
} //end namespace thrill

#endif // ITERATOR_WRAPPER_HEADER
