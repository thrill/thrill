/*******************************************************************************
 * thrill/io/linuxaio_request.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2011 Johannes Singler <singler@kit.edu>
 * Copyright (C) 2014 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_LINUXAIO_REQUEST_HEADER
#define THRILL_IO_LINUXAIO_REQUEST_HEADER

#include <thrill/io/linuxaio_file.hpp>

#if STXXL_HAVE_LINUXAIO_FILE

#include <linux/aio_abi.h>
#include <thrill/io/request.hpp>

namespace thrill {
namespace io {

//! \addtogroup reqlayer
//! \{

//! Request for an linuxaio_file.
class linuxaio_request final : public request
{
    template <class base_file_type>
    friend class fileperblock_file;

    //! control block of async request
    iocb cb;

    void fill_control_block();

public:
    linuxaio_request(
        const completion_handler& on_complete,
        io::file* file,
        void* buffer,
        offset_type offset,
        size_type bytes,
        ReadOrWriteType type)
        : request(on_complete, file, buffer, offset, bytes, type) {
        assert(dynamic_cast<linuxaio_file*>(file));
        LOG << "linuxaio_request[" << this << "]" << " linuxaio_request"
            << "(file=" << file << " buffer=" << buffer
            << " offset=" << offset << " bytes=" << bytes
            << " type=" << type << ")";
    }

    bool post();
    bool cancel() final;
    bool cancel_aio();
    void completed(bool posted, bool canceled);
    void completed(bool canceled) final { completed(true, canceled); }
};

//! \}

} // namespace io
} // namespace thrill

#endif // #if STXXL_HAVE_LINUXAIO_FILE

#endif // !THRILL_IO_LINUXAIO_REQUEST_HEADER

/******************************************************************************/
