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

#ifndef ITERATOR_WRAPPER_HEADER
#define ITERATOR_WRAPPER_HEADER

#include <thrill/data/file.hpp>
#include <thrill/common/logger.hpp>

#include <vector>

namespace thrill {
namespace core {

static const bool debug = true;

template <typename ArrayItem>
struct IterStats {
    bool has_elem_ = false;
    bool is_valid_ = true;
    ArrayItem item_;
};

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
    std::shared_ptr<Reader> reader_;
    // Reader* reader_;
    std::size_t pos_;

    std::shared_ptr<IterStats<ArrayItem>> stats_;

public:
    typedef std::iterator<std::random_access_iterator_tag, ArrayItem> base_type;

    typedef std::random_access_iterator_tag iterator_category;

    typedef typename base_type::value_type value_type;
    typedef typename base_type::difference_type difference_type;
    typedef typename base_type::reference reference;
    typedef typename base_type::pointer pointer;

    void GetItem() const {
        assert(reader_->HasNext());
        stats_->item_ = reader_->Next<ArrayItem>();
        LOG << "NEXT GOT " << stats_->item_;
        stats_->has_elem_ = true;
    }

    void Invalidate() const {
        stats_->is_valid_ = false;
        stats_->has_elem_ = false;
    }

    void GetItemOrInvalidate() const {
        if (reader_->HasNext()) {
            GetItem();
        } else {
            Invalidate();
        }
    }

    StxxlFileWrapper() : file_(), pos_(0) {}

    StxxlFileWrapper(File* file, std::shared_ptr<Reader> reader, std::size_t pos, bool valid=true)
    // StxxlFileWrapper(File* file, Reader* reader, std::size_t pos, bool valid=true)
                     : file_(file),
                       reader_(reader),
                       pos_(pos),
                       stats_(std::make_shared<IterStats<ArrayItem>>(IterStats<ArrayItem>())) {

        stats_->is_valid_ = valid;

        if (stats_->is_valid_) {
            GetItemOrInvalidate();
        }

        LOG << "  Created iterator";
        LOG << "    " << std::left << std::setw(7) << "pos: " << pos_;
        LOG << "    " << std::left << std::setw(7) << "file: " << file_->ToString();
        LOG << "    " << std::left << std::setw(7) << "valid: " << std::boolalpha << stats_->is_valid_;
    }

    // StxxlFileWrapper(const StxxlFileWrapper& r) {}
    // StxxlFileWrapper& operator=(const StxxlFileWrapper& r) {}

    StxxlFileWrapper& operator++() {
        GetItemOrInvalidate();
        ++pos_;

        LOG << "  Operator++";
        LOG << "    " << std::left << std::setw(7) << "pos: " << pos_;
        LOG << "    " << std::left << std::setw(7) << "file: " << file_->ToString();
        LOG << "    " << std::left << std::setw(7) << "valid: " << std::boolalpha << stats_->is_valid_;
        return *this;
    }

    StxxlFileWrapper& operator--() {
        Invalidate();
        --pos_;

        LOG << "  Operator--";
        LOG << "    " << std::left << std::setw(7) << "pos: " << pos_;
        LOG << "    " << std::left << std::setw(7) << "file: " << file_->ToString();
        LOG << "    " << std::left << std::setw(7) << "valid: " << std::boolalpha << stats_->is_valid_;
        return *this;
    }

    StxxlFileWrapper operator++(int) {
        GetItemOrInvalidate();

        auto r = StxxlFileWrapper(file_, reader_, ++pos_, stats_.is_valid_);
        LOG << "  Operator++ (postfix)";
        LOG << "    " << std::left << std::setw(7) << "pos: " << r.pos_;
        LOG << "    " << std::left << std::setw(7) << "file: " << r.file_->ToString();
        LOG << "    " << std::left << std::setw(7) << "valid: " << std::boolalpha << r.stats_->is_valid_;
        return r;
    }

    // StxxlFileWrapper operator--(int) {}

    StxxlFileWrapper operator+(const difference_type& n) const {
        auto w = StxxlFileWrapper(file_, reader_, pos_+n, stats_->is_valid_);
        for (difference_type t = 0; t < n; ++t){
            w.GetItemOrInvalidate();
        }

        LOG << "  Operator+ " << n;
        LOG << "    " << std::left << std::setw(7) << "pos: " << w.pos_;
        LOG << "    " << std::left << std::setw(7) << "file: " << w.file_->ToString();
        LOG << "    " << std::left << std::setw(7) << "valid: " << std::boolalpha << w.stats_->is_valid_;
        return w;
    }

    StxxlFileWrapper& operator+=(const difference_type& n) {
        pos_ += n;
        for (difference_type t = 0; t < n; ++t){
            GetItemOrInvalidate();
        }

        LOG << "  Operator+= " << n;
        LOG << "    " << std::left << std::setw(7) << "pos: " << pos_;
        LOG << "    " << std::left << std::setw(7) << "file: " << file_->ToString();
        LOG << "    " << std::left << std::setw(7) << "valid: " << std::boolalpha << stats_->is_valid_;
        return *this;
    }

    StxxlFileWrapper operator-(const difference_type& n) const {
        auto w = StxxlFileWrapper(file_, reader_, pos_-n, false);
        LOG << "  Operator- " << n;
        LOG << "    " << std::left << std::setw(7) << "pos: " << w.pos_;
        LOG << "    " << std::left << std::setw(7) << "file: " << w.file_->ToString();
        LOG << "    " << std::left << std::setw(7) << "valid: " << std::boolalpha << w.stats_->is_valid_;
        return w;
    }

    // StxxlFileWrapper& operator-=(const difference_type& n) {}

    reference operator*() const {
        // assert(stats_->is_valid_);

        LOG << "  Operator* ";
        LOG << "    " << std::left << std::setw(7) << "pos: " << pos_;
        LOG << "    " << std::left << std::setw(7) << "file: " << file_->ToString();
        LOG << "    " << std::left << std::setw(7) << "valid: " << std::boolalpha << stats_->is_valid_;
        LOG << "    " << std::left << std::setw(7) << "item: " << stats_->item_;

        LOG << "RETURN " << stats_->item_;
        return stats_->item_;
    }
    // pointer operator->() const {
    // reference operator[](const difference_type& n) const {}

    bool operator==(const StxxlFileWrapper& r) {
        LOG << "  Operator== ";
        LOG << "    " << std::left << std::setw(7) << "pos: " << pos_;
        LOG << "    " << std::left << std::setw(7) << "pos2: " << r.pos_;
        LOG << "    " << std::left << std::setw(7) << "file: " << file_->ToString();
        LOG << "    " << std::left << std::setw(7) << "file2: " << r.file_->ToString();
        LOG << "    " << std::left << std::setw(7) << "result: " << std::boolalpha << ((file_ == r.file_) && (pos_ == r.pos_));
        return (file_ == r.file_) && (pos_ == r.pos_);
    }

    bool operator!=(const StxxlFileWrapper& r) {
        LOG << "  Operator!= ";
        LOG << "    " << std::left << std::setw(7) << "pos: " << pos_;
        LOG << "    " << std::left << std::setw(7) << "pos2: " << r.pos_;
        LOG << "    " << std::left << std::setw(7) << "file: " << file_->ToString();
        LOG << "    " << std::left << std::setw(7) << "file2: " << r.file_->ToString();
        LOG << "    " << std::left << std::setw(7) << "result: " << std::boolalpha << ((file_ != r.file_) || (pos_ != r.pos_));
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

    difference_type operator-(const StxxlFileWrapper& r2) const {
        LOG << "  Operator-  StxxlFileWrapper";
        LOG << "    " << std::left << std::setw(7) << "pos: " << pos_;
        LOG << "    " << std::left << std::setw(7) << "pos2: " << r2.pos_;
        LOG << "    " << std::left << std::setw(7) << "result: " << pos_ - r2.pos_;
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
