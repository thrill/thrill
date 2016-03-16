/*******************************************************************************
 * thrill/io/serving_request.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/state.hpp>
#include <thrill/io/file_base.hpp>
#include <thrill/io/request.hpp>
#include <thrill/io/serving_request.hpp>

#include <iomanip>

namespace thrill {
namespace io {

ServingRequest::ServingRequest(
    const CompletionHandler& on_cmpl,
    io::FileBase* f,
    void* buf,
    offset_type off,
    size_type b,
    ReadOrWriteType t)
    : Request(on_cmpl, f, buf, off, b, t) {
#ifdef THRILL_CHECK_BLOCK_ALIGNING
    // Direct I/O requires file system block size alignment for file offsets,
    // memory buffer addresses, and transfer(buffer) size must be multiple
    // of the file system block size
    if (f->need_alignment())
        check_alignment();
#endif
}

void ServingRequest::serve() {
    check_nref();
    LOG << "serving_request::serve(): "
        << buffer_ << " @ ["
        << file_ << "|" << file_->get_allocator_id() << "]0x"
        << std::hex << std::setfill('0') << std::setw(8)
        << offset_ << "/0x" << bytes_
        << ((type_ == Request::READ) ? " READ" : " WRITE");

    try
    {
        file_->serve(buffer_, offset_, bytes_, type_);
    }
    catch (const IoError& ex)
    {
        save_error(ex.safe_message());
    }

    check_nref(true);

    completed(false);
}

} // namespace io
} // namespace thrill

/******************************************************************************/
