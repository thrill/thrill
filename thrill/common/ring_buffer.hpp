/*******************************************************************************
 * thrill/common/ring_buffer.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_RING_BUFFER_HEADER
#define THRILL_COMMON_RING_BUFFER_HEADER

#include <thrill/common/math.hpp>

#include <memory>
#include <vector>

namespace thrill {
namespace common {

/*!
 * A ring (circular) buffer of static (non-growing) size allocated via
 * std::vector. This data structure is mostly used by the DIA.Window()
 * transformation.
 *
 * Due to many modulo operations with capacity_, the capacity is rounded up to
 * the next power of two, even for powers of two! This is because otherwise
 * size() == end - begin == 0 after filling the ring buffer, and adding another
 * size_ member requires more book-keeping.
 */
template <typename Type, class Allocator = std::allocator<Type> >
class RingBuffer
{
public:
    using value_type = Type;
    using allocator_type = Allocator;

    using alloc_traits = std::allocator_traits<allocator_type>;

    using reference = typename allocator_type::reference;
    using const_reference = typename allocator_type::const_reference;
    using pointer = typename allocator_type::pointer;
    using const_pointer = typename allocator_type::const_pointer;

    using size_type = typename allocator_type::size_type;
    using difference_type = typename allocator_type::difference_type;

    // using iterator;
    // using const_iterator;
    // using reverse_iterator = std::reverse_iterator<iterator>;
    // using const_reverse_iterator = std::reverse_iterator<const_iterator>;

    explicit RingBuffer(size_t max_size,
                        const Allocator& alloc = allocator_type())
        : max_size_(max_size),
          alloc_(alloc),
          capacity_(RoundUpToPowerOfTwo(max_size + 1)),
          mask_(capacity_ - 1),
          data_(alloc_.allocate(capacity_)) { }

    //! copy-constructor: create new ring buffer
    RingBuffer(const RingBuffer& srb)
        : max_size_(srb.max_size_),
          alloc_(srb.alloc_),
          capacity_(srb.capacity_),
          mask_(srb.mask_),
          data_(alloc_.allocate(capacity_)) {
        // copy items using existing methods (we cannot just flat copy the array
        // due to item construction).
        for (size_t i = 0; i < srb.size(); ++i) {
            push_back(srb[i]);
        }
    }
    //! non-copyable: delete assignment operator
    RingBuffer& operator = (const RingBuffer&) = delete;
    //! move-constructor: default
    RingBuffer(RingBuffer&&) = default;
    //! move-assignment operator: default
    RingBuffer& operator = (RingBuffer&&) = default;

    ~RingBuffer() {
        clear();
        alloc_.deallocate(data_, capacity_);
    }

    //! \name Modifiers
    //! \{

    //! add element at the end
    void push_back(const value_type& t) {
        assert(size() + 1 <= max_size_);
        alloc_traits::construct(alloc_, std::addressof(data_[end_]), t);
        ++end_ &= mask_;
    }

    //! add element at the end
    void push_back(value_type&& t) {
        assert(size() + 1 <= max_size_);
        alloc_traits::construct(
            alloc_, std::addressof(data_[end_]), std::move(t));
        ++end_ &= mask_;
    }

    //! emplace element at the end
    template <typename ... Args>
    void emplace_back(Args&& ... args) {
        assert(size() + 1 <= max_size_);
        alloc_traits::construct(alloc_, std::addressof(data_[end_]),
                                std::forward<Args>(args) ...);
        ++end_ &= mask_;
    }

    //! add element at the beginning
    void push_front(const value_type& t) {
        assert(size() + 1 <= max_size_);
        --begin_ &= mask_;
        alloc_traits::construct(alloc_, std::addressof(data_[begin_]), t);
    }

    //! add element at the beginning
    void push_front(value_type&& t) {
        assert(size() + 1 <= max_size_);
        --begin_ &= mask_;
        alloc_traits::construct(
            alloc_, std::addressof(data_[begin_]), std::move(t));
    }

    //! emplace element at the beginning
    template <typename ... Args>
    void emplace_front(Args&& ... args) {
        assert(size() + 1 <= max_size_);
        --begin_ &= mask_;
        alloc_traits::construct(alloc_, std::addressof(data_[begin_]),
                                std::forward<Args>(args) ...);
    }

    //! remove element at the beginning
    void pop_front() {
        assert(!empty());
        alloc_traits::destroy(alloc_, std::addressof(data_[begin_]));
        ++begin_ &= mask_;
    }

    //! remove element at the end
    void pop_back() {
        assert(!empty());
        alloc_traits::destroy(alloc_, std::addressof(data_[begin_]));
        --end_ &= mask_;
    }

    //! reset buffer contents
    void clear() {
        while (begin_ != end_)
            pop_front();
    }

    //! \}

    //! \name Element access
    //! \{

    //! Returns a reference to the i-th element.
    reference operator [] (size_type i) {
        assert(i < size());
        return data_[(begin_ + i) & mask_];
    }
    //! Returns a reference to the i-th element.
    const_reference operator [] (size_type i) const {
        assert(i < size());
        return data_[(begin_ + i) & mask_];
    }

    //! Returns a reference to the first element.
    reference front() {
        assert(!empty());
        return data_[begin_];
    }
    //! Returns a reference to the first element.
    const_reference front() const {
        assert(!empty());
        return data_[begin_];
    }

    //! Returns a reference to the last element.
    reference back() {
        assert(!empty());
        return data_[(end_ - 1) & mask_];
    }
    //! Returns a reference to the last element.
    const_reference back() const {
        assert(!empty());
        return data_[(end_ - 1) & mask_];
    }

    //! \}

    //! \name Capacity
    //! \{

    //! return the number of items in the buffer
    size_type size() const noexcept {
        return (end_ - begin_) & mask_;
    }

    //! return the maximum number of items in the buffer.
    size_t max_size() const noexcept {
        return max_size_;
    }

    //! return actual capacity of the ring buffer.
    size_t capacity() const noexcept {
        return capacity_;
    }

    //! returns true if no items are in the buffer
    bool empty() const noexcept {
        return size() == 0;
    }

    //! \}

protected:
    //! target max_size of circular buffer prescribed by the user. Never equal
    //! to the data_.size(), which is rounded up to a power of two.
    size_t max_size_;

    //! used allocator
    allocator_type alloc_;

    //! capacity of data buffer. rounded up from max_size_ to next unequal power
    //! of two.
    size_t capacity_;

    //! one-bits mask for calculating modulo of capacity using AND-mask.
    size_t mask_;

    //! the circular buffer of static size.
    Type* data_;

    //! iterator at current begin of ring buffer
    size_type begin_ = 0;

    //! iterator at current begin of ring buffer
    size_type end_ = 0;
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_RING_BUFFER_HEADER

/******************************************************************************/
