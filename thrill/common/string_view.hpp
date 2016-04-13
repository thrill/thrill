/*******************************************************************************
 * thrill/common/string_view.hpp
 *
 * A simplified string_view implementation to reduce the number of allocations
 * in the WordCount benchmark.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_STRING_VIEW_HEADER
#define THRILL_COMMON_STRING_VIEW_HEADER

#include <thrill/common/fast_string.hpp>

#include <algorithm>
#include <ostream>
#include <string>

namespace thrill {
namespace common {

/*!
 * StringView is a reference to a part of a string, consisting of only a char
 * pointer and a length. It does not have ownership of the substring and is used
 * mainly for temporary objects.
 */
class StringView
{
public:
    using iterator = const char*;

    //! Default constructor for a StringView. Doesn't do anything.
    StringView() = default;

    /*!
     * Creates a new StringView, given a const char* and the size.
     *
     * \param data pointer to start of data
     * \param size size of data in bytes.
     * \return new StringView object.
     */
    StringView(const char* data, size_t size) noexcept
        : data_(data), size_(size) { }

    /*!
     * Creates a new StringView, given a const iterator to a std::string and the
     * size.
     *
     * \param data iterator to start of data
     * \param size size of data in character.
     * \return new StringView object.
     */
    StringView(const std::string::const_iterator& data, size_t size) noexcept
        : data_(&(*data)), size_(size) { }

    /*!
     * Creates a new reference StringView, given two const iterators to a
     * std::string.
     *
     * \param begin iterator to start of data
     * \param end iterator to the end of data.
     * \return new StringView object.
     */
    StringView(const std::string::const_iterator& begin,
               const std::string::const_iterator& end) noexcept
        : StringView(begin, end - begin) { }

    //! Construct a StringView to the whole std::string.
    explicit StringView(const std::string& str) noexcept
        : StringView(str.begin(), str.end()) { }

    //! Returns a pointer to the start of the data.
    const char * data() const noexcept {
        return data_;
    }

    //! Returns a pointer to the beginning of the data.
    iterator begin() const noexcept {
        return data_;
    }

    //! Returns a pointer beyond the end of the data.
    iterator end() const noexcept {
        return data_ + size_;
    }

    //! Returns the size of this StringView
    size_t size() const noexcept {
        return size_;
    }

    //! Equality operator to compare a StringView with another StringView
    bool operator == (const StringView& other) const noexcept {
        return size_ == other.size_ &&
               std::equal(data_, data_ + size_, other.data_);
    }

    //! Inequality operator to compare a StringView with another StringView
    bool operator != (const StringView& other) const noexcept {
        return !(operator == (other));
    }

    //! Equality operator to compare a StringView with an std::string
    bool operator == (const std::string& other) const noexcept {
        return size_ == other.size() &&
               std::equal(data_, data_ + size_, other.data());
    }

    //! Inequality operator to compare a StringView with an std::string
    bool operator != (const std::string& other) const noexcept {
        return !(operator == (other));
    }

    //! make StringView ostreamable
    friend std::ostream& operator << (std::ostream& os, const StringView& sv) {
        return os.write(sv.data(), sv.size());
    }

    //! Returns the data of this StringView as an std::string
    std::string ToString() const {
        return std::string(data_, size_);
    }

    //! Returns the data of this StringView as an FastString
    FastString ToFastString() const {
        return FastString::Ref(data_, size_);
    }

    // operator std::string () const { return ToString(); }

private:
    //! pointer to character data
    const char* data_ = nullptr;
    //! size of data
    size_t size_ = 0;
};

static inline
bool operator == (const std::string& a, const StringView& b) noexcept {
    return b == a;
}

static inline
bool operator != (const std::string& a, const StringView& b) noexcept {
    return b != a;
}

/*!
 * Split the given string at each separator character into distinct substrings,
 * and call the given callback for each substring, represented by two iterators
 * begin and end. Multiple consecutive separators are considered individually
 * and will result in empty split substrings.
 *
 * \param str       string to split
 * \param sep       separator character
 * \param callback  callback taking begin and end iterator of substring
 * \param limit     maximum number of parts returned
 */
template <typename F>
static inline
void SplitView(
    const std::string& str, char sep, F&& callback,
    std::string::size_type limit = std::string::npos) {

    if (limit == 0)
    {
        callback(StringView(str.begin(), str.end()));
        return;
    }

    std::string::size_type count = 0;
    auto it = str.begin(), last = it;

    for ( ; it != str.end(); ++it)
    {
        if (*it == sep)
        {
            if (count == limit)
            {
                callback(StringView(last, str.end()));
                return;
            }
            callback(StringView(last, it));
            ++count;
            last = it + 1;
        }
    }
    callback(StringView(last, it));
}

} // namespace common
} // namespace thrill

namespace std {
template <>
struct hash<thrill::common::StringView>{
    size_t operator () (const thrill::common::StringView& sv) const {
        // simple string hash taken from: http://www.cse.yorku.ca/~oz/hash.html
        size_t hash = 5381;
        for (const char* ctr = sv.begin(); ctr != sv.end(); ++ctr) {
            // hash * 33 + c
            hash = ((hash << 5) + hash) + *ctr;
        }
        return hash;
    }
};

} // namespace std

#endif // !THRILL_COMMON_STRING_VIEW_HEADER

/******************************************************************************/
