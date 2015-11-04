/*******************************************************************************
 * thrill/io/typed_block.hpp
 *
 * Constructs a typed_block object containing as many elements elements plus
 * some metadata as fits into the given block size.
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002-2004 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008-2010 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_TYPED_BLOCK_HEADER
#define THRILL_IO_TYPED_BLOCK_HEADER

#include <thrill/common/config.hpp>
#include <thrill/io/bid.hpp>
#include <thrill/io/request.hpp>
#include <thrill/mem/aligned_alloc.hpp>

#ifndef STXXL_VERBOSE_TYPED_BLOCK
#define STXXL_VERBOSE_TYPED_BLOCK STXXL_VERBOSE2
#endif

namespace thrill {
namespace io {

//! \addtogroup mnglayer
//! \{

//! Block Manager Internals \internal
namespace mng_local {

//! \defgroup mnglayer_internals Internals
//! \ingroup mnglayer
//! Internals and support classes
//! \{

template <unsigned Bytes>
class filler_struct
{
    using byte_type = unsigned char;
    byte_type filler_array[Bytes];

public:
    filler_struct() {
        LOG0 << "[" << (void*)this << "] filler_struct is constructed";
    }
};

template <>
class filler_struct<0>
{
    using byte_type = unsigned char;

public:
    filler_struct() {
        LOG0 << "[" << (void*)this << "] filler_struct<> is constructed";
    }
};

//! Contains data elements for \c stxxl::typed_block , not intended for direct use.
template <typename Type, unsigned Size>
class element_block
{
public:
    using type = Type;
    using value_type = Type;
    using reference = Type &;
    using const_reference = const Type &;
    using pointer = type *;
    using iterator = pointer;
    using const_iterator = const type *;

    enum
    {
        size = Size //!< number of elements in the block
    };

    //! Array of elements of type Type
    value_type elem[size];

    element_block() {
        LOG0 << "[" << (void*)this << "] element_block is constructed";
    }

    //! An operator to access elements in the block
    reference operator [] (size_t i) {
        return elem[i];
    }

    //! Returns \c iterator pointing to the first element.
    iterator begin() {
        return elem;
    }

    //! Returns \c const_iterator pointing to the first element.
    const_iterator begin() const {
        return elem;
    }

    //! Returns \c const_iterator pointing to the first element.
    const_iterator cbegin() const {
        return begin();
    }

    //! Returns \c iterator pointing to the end element.
    iterator end() {
        return elem + size;
    }

    //! Returns \c const_iterator pointing to the end element.
    const_iterator end() const {
        return elem + size;
    }

    //! Returns \c const_iterator pointing to the end element.
    const_iterator cend() const {
        return end();
    }
};

//! Contains BID references for \c stxxl::typed_block , not intended for direct use.
template <typename Type, unsigned Size, unsigned RawSize, unsigned NBids = 0>
class block_w_bids : public element_block<Type, Size>
{
public:
    enum
    {
        raw_size = RawSize,
        nbids = NBids
    };

    using bid_type = BID<raw_size>;

    //! Array of BID references
    bid_type ref[nbids];

    //! An operator to access bid references
    bid_type& operator () (size_t i) {
        return ref[i];
    }

    block_w_bids() {
        LOG0 << "[" << (void*)this << "] block_w_bids is constructed";
    }
};

template <typename Type, unsigned Size, unsigned RawSize>
class block_w_bids<Type, Size, RawSize, 0>
    : public element_block<Type, Size>
{
public:
    enum
    {
        raw_size = RawSize,
        nbids = 0
    };

    using bid_type = BID<raw_size>;

    block_w_bids() {
        LOG0 << "[" << (void*)this << "] block_w_bids<> is constructed";
    }
};

//! Contains per block information for \c stxxl::typed_block , not intended for direct use.
template <typename Type, unsigned RawSize, unsigned NBids, typename MetaInfoType = void>
class block_w_info
    : public block_w_bids<Type, ((RawSize - sizeof(BID<RawSize>)* NBids - sizeof(MetaInfoType)) / sizeof(Type)), RawSize, NBids>
{
public:
    //! Type of per block information element.
    using info_type = MetaInfoType;

    //! Per block information element.
    info_type info;

    block_w_info() {
        LOG0 << "[" << (void*)this << "] block_w_info is constructed";
    }
};

template <typename Type, unsigned RawSize, unsigned NBids>
class block_w_info<Type, RawSize, NBids, void>
    : public block_w_bids<Type, ((RawSize - sizeof(BID<RawSize>)* NBids) / sizeof(Type)), RawSize, NBids>
{
public:
    using info_type = void;

    block_w_info() {
        LOG0 << "[" << (void*)this << "] block_w_info<> is constructed";
    }
};

//! Contains per block filler for \c stxxl::typed_block , not intended for direct use.
template <typename BaseType, unsigned FillSize = 0>
class add_filler : public BaseType
{
private:
    //! Per block filler element.
    filler_struct<FillSize> filler;

public:
    add_filler() {
        LOG0 << "[" << (void*)this << "] add_filler is constructed";
    }
};

template <typename BaseType>
class add_filler<BaseType, 0>
    : public BaseType
{
public:
    add_filler() {
        LOG0 << "[" << (void*)this << "] add_filler<> is constructed";
    }
};

//! Helper to compute the size of the filler , not intended for direct use.
template <typename Type, unsigned RawSize>
class expand_struct : public add_filler<Type, RawSize - sizeof(Type)>
{ };

//! \}

} // namespace mng_local

//! Block containing elements of fixed length.
//!
//! \tparam RawSize size of block in bytes
//! \tparam Type type of block's records
//! \tparam NRef number of block references (BIDs) that can be stored in the block (default is 0)
//! \tparam MetaInfoType type of per block information (default is no information - void)
//!
//! The data array of type Type is contained in the parent class \c stxxl::element_block, see related information there.
//! The BID array of references is contained in the parent class \c stxxl::block_w_bids, see related information there.
//! The "per block information" is contained in the parent class \c stxxl::block_w_info, see related information there.
//!  \warning If \c RawSize > 2MB object(s) of this type can not be allocated on the stack (as a
//! function variable for example), because Linux POSIX library limits the stack size for the
//! main thread to (2MB - system page size)
template <unsigned RawSize, typename Type, unsigned NRef = 0, typename MetaInfoType = void>
class typed_block
    : public mng_local::expand_struct<mng_local::block_w_info<Type, RawSize, NRef, MetaInfoType>, RawSize>
{
    using Base = mng_local::expand_struct<mng_local::block_w_info<Type, RawSize, NRef, MetaInfoType>, RawSize>;

public:
    using value_type = Type;
    using reference = value_type &;
    using const_reference = const value_type &;
    using pointer = value_type *;
    using iterator = pointer;
    using const_pointer = const value_type *;
    using const_iterator = const_pointer;

    enum constants
    {
        raw_size = RawSize,                                        //!< size of block in bytes
        size = Base::size,                                         //!< number of elements in block
        has_only_data = (raw_size == (size * sizeof(value_type)))  //!< no meta info, bids or (non-empty) fillers included in the block, allows value_type array addressing across block boundaries
    };

    using bid_type = BID<raw_size>;

    typed_block() {
        static_assert(sizeof(typed_block) == raw_size, "Incorrect block size!");
        LOG0 << "[" << (void*)this << "] typed_block is constructed";
#if 0
        assert(((long)this) % STXXL_BLOCK_ALIGN == 0);
#endif
    }

#if 0
    typed_block(const typed_block& tb) {
        STXXL_STATIC_ASSERT(sizeof(typed_block) == raw_size);
        LOG0 << "[" << (void*)this << "] typed_block is copy constructed from [" << (void*)&tb << "]";
        STXXL_UNUSED(tb);
    }
#endif

    /*! Writes block to the disk(s).
     *! \param bid block identifier, points the file(disk) and position
     *! \param on_cmpl completion handler
     *! \return \c pointer_ptr object to track status I/O operation after the call
     */
    request_ptr write(const bid_type& bid,
                      completion_handler on_cmpl = completion_handler()) {
        LOG0 << "BLC:write  " << bid;
        return bid.storage->awrite(this, bid.offset, raw_size, on_cmpl);
    }

    /*! Reads block from the disk(s).
     *! \param bid block identifier, points the file(disk) and position
     *! \param on_cmpl completion handler
     *! \return \c pointer_ptr object to track status I/O operation after the call
     */
    request_ptr read(const bid_type& bid,
                     completion_handler on_cmpl = completion_handler()) {
        LOG0 << "BLC:read   " << bid;
        return bid.storage->aread(this, bid.offset, raw_size, on_cmpl);
    }

    static void* operator new (size_t bytes) {
        size_t meta_info_size = bytes % raw_size;
        LOG0 << "typed::block operator new[]: bytes=" << bytes
             << ", meta_info_size=" << meta_info_size;

        void* result = mem::aligned_alloc<STXXL_BLOCK_ALIGN>(
            bytes - meta_info_size, meta_info_size);

#if STXXL_WITH_VALGRIND || STXXL_TYPED_BLOCK_INITIALIZE_ZERO
        memset(result, 0, bytes);
#endif
        return result;
    }

    static void* operator new[] (size_t bytes) {
        size_t meta_info_size = bytes % raw_size;
        LOG0 << "typed::block operator new[]: bytes=" << bytes
             << ", meta_info_size=" << meta_info_size;

        void* result = mem::aligned_alloc<STXXL_BLOCK_ALIGN>(
            bytes - meta_info_size, meta_info_size);

#if STXXL_WITH_VALGRIND || STXXL_TYPED_BLOCK_INITIALIZE_ZERO
        memset(result, 0, bytes);
#endif
        return result;
    }

    static void* operator new (size_t /*bytes*/, void* ptr) {     // construct object in existing memory
        return ptr;
    }

    static void operator delete (void* ptr) {
        mem::aligned_dealloc<STXXL_BLOCK_ALIGN>(ptr);
    }

    static void operator delete[] (void* ptr) {
        mem::aligned_dealloc<STXXL_BLOCK_ALIGN>(ptr);
    }

    static void operator delete (void*, void*)
    { }

#if 1
    // STRANGE: implementing destructor makes g++ allocate
    // additional 4 bytes in the beginning of every array
    // of this type !? makes aligning to 4K boundaries difficult
    //
    // http://www.cc.gatech.edu/grads/j/Seung.Won.Jun/tips/pl/node4.html :
    // "One interesting thing is the array allocator requires more memory
    //  than the array size multiplied by the size of an element, by a
    //  difference of delta for metadata a compiler needs. It happens to
    //  be 8 bytes long in g++."
    ~typed_block() {
        LOG0 << "[" << (void*)this << "] typed_block is destructed";
    }
#endif
};

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_TYPED_BLOCK_HEADER

/******************************************************************************/
