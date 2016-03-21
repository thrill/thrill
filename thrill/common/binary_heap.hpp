/*******************************************************************************
 * thrill/common/binary_heap.hpp
 *
 * A simple binary heap priority queue, except that one can additionally find
 * and delete arbitrary items in O(n).
 *
 * The standard binary heap implementation methods were copied from libc++ under
 * the MIT license.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_BINARY_HEAP_HEADER
#define THRILL_COMMON_BINARY_HEAP_HEADER

#include <algorithm>
#include <functional>
#include <vector>

namespace thrill {
namespace common {

template <typename Type, typename Compare = std::less<Type> >
class BinaryHeap
{
public:
    using Container = std::vector<Type>;
    using value_type = Type;
    using const_reference = const Type &;
    using size_type = size_t;
    using difference_type = typename Container::difference_type;

    //! \name PQ Interface
    //! \{

    explicit BinaryHeap(const Compare& cmp = Compare())
        : cmp_(cmp) { }

    //! check if PQ is empty
    bool empty() const { return c_.empty(); }

    //! return number of items in the PQ
    size_type size() const { return c_.size(); }

    //! return reference to top item in PQ
    const_reference top() const { return c_.front(); }

    //! add an items in the PQ.
    template <typename ... Args>
    void emplace(Args&& ... args) {
        c_.emplace_back(std::forward<Args>(args) ...);
        push_heap(c_.begin(), c_.end(), cmp_);
    }

    //! remove the top item in the PQ
    void pop() {
        pop_heap(c_.begin(), c_.end(), cmp_);
        c_.pop_back();
    }

    //! \}

    //! \name Additional Methods
    //! \{

    //! direct access to heap container
    Container& container() { return c_; }

    //! iterate over all items, delete those for which f returns true. Takes
    //! time O(n). If you need to erase items frequently, use a different PQ.
    template <typename Functor>
    size_t erase(Functor&& f) {
        size_t result = 0;
        for (typename std::vector<Type>::iterator it = c_.begin();
             it < c_.end(); ++it)
        {
            if (!std::forward<Functor>(f)(*it)) continue;
            std::swap(*it, c_.back());
            sift_down(c_.begin(), c_.end(), cmp_, c_.size() - 1, it);
            c_.pop_back();
        }
        return result;
    }

    //! \}

    //! \name Free Binary Heap Methods
    //! \{

    template <typename Iterator>
    static void sift_up(Iterator first, Iterator last,
                        const Compare& comp, difference_type len) {
        if (len > 1)
        {
            len = (len - 2) / 2;
            Iterator ptr = first + len;
            if (comp(*ptr, *(--last)))
            {
                value_type t = std::move(*last);
                do
                {
                    *last = std::move(*ptr);
                    last = ptr;
                    if (len == 0)
                        break;
                    len = (len - 1) / 2;
                    ptr = first + len;
                } while (comp(*ptr, t));
                *last = std::move(t);
            }
        }
    }

    template <typename Iterator>
    static void push_heap(Iterator first, Iterator last, const Compare& comp) {
        sift_up(first, last, comp, last - first);
    }

    template <typename Iterator>
    static void sift_down(
        Iterator first, Iterator /* last */,
        const Compare& comp, difference_type len, Iterator start) {
        // left-child of start is at 2 * start + 1
        // right-child of start is at 2 * start + 2
        difference_type child = start - first;

        if (len < 2 || (len - 2) / 2 < child)
            return;

        child = 2 * child + 1;
        Iterator child_i = first + child;

        if ((child + 1) < len && comp(*child_i, *(child_i + 1))) {
            // right-child exists and is greater than left-child
            ++child_i, ++child;
        }

        // check if we are in heap-order
        if (comp(*child_i, *start))
            // we are, start is larger than it's largest child
            return;

        value_type top = std::move(*start);
        do        {
            // we are not in heap-order, swap the parent with it's largest child
            *start = std::move(*child_i);
            start = child_i;

            if ((len - 2) / 2 < child)
                break;

            // recompute the child based off of the updated parent
            child = 2 * child + 1;
            child_i = first + child;

            if ((child + 1) < len && comp(*child_i, *(child_i + 1))) {
                // right-child exists and is greater than left-child
                ++child_i, ++child;
            }
            // check if we are in heap-order
        } while (!comp(*child_i, top));
        *start = std::move(top);
    }

    template <typename Iterator>
    static void pop_heap(Iterator first, Iterator last,
                         const Compare& comp, difference_type len) {
        if (len > 1)
        {
            std::swap(*first, *(--last));
            sift_down(first, last, comp, len - 1, first);
        }
    }

    template <typename Iterator>
    static void pop_heap(Iterator first, Iterator last, const Compare& comp) {
        pop_heap(first, last, comp, last - first);
    }

    //! \}

private:
    //! array holding binary heap
    Container c_;

    //! compare
    Compare cmp_;
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_BINARY_HEAP_HEADER

/******************************************************************************/
