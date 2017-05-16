/*******************************************************************************
 * thrill/io/request.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2013-2014 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_REQUEST_HEADER
#define THRILL_IO_REQUEST_HEADER

#include <thrill/common/shared_state.hpp>
#include <thrill/io/exceptions.hpp>
#include <thrill/mem/pool.hpp>
#include <tlx/counting_ptr.hpp>
#include <tlx/delegate.hpp>

#include <cassert>
#include <memory>
#include <mutex>
#include <string>

namespace thrill {
namespace io {

//! \addtogroup io_layer_req
//! \{

class Request;
class FileBase;
using FileBasePtr = tlx::CountingPtr<FileBase>;

using CompletionHandler = tlx::delegate<void(Request*, bool)>;

//! Request object encapsulating basic properties like file and offset.
class Request : public tlx::ReferenceCounter
{
    friend class LinuxaioQueue;

public:
    //! type for offsets within a file
    using offset_type = size_t;
    //! type for block transfer sizes
    using size_type = size_t;

    enum ReadOrWriteType { READ, WRITE };

protected:
    static constexpr bool debug = false;

    CompletionHandler on_complete_;
    mem::safe_unique_ptr<IoError> error_;

protected:
    //! \name Base Parameter of an I/O Request
    //! \{

    //! file implementation to perform I/O with
    FileBasePtr file_;
    //! data buffer to transfer
    void* buffer_;
    //! offset within file
    offset_type offset_;
    //! number of bytes at buffer_ to transfer
    size_type bytes_;
    //! READ or WRITE
    ReadOrWriteType type_;

    //! \}

public:
    //! ctor: initialize
    Request(const CompletionHandler& on_complete,
            const FileBasePtr& file,
            void* buffer, offset_type offset, size_type bytes,
            ReadOrWriteType type);

    //! non-copyable: delete copy-constructor
    Request(const Request&) = delete;
    //! non-copyable: delete assignment operator
    Request& operator = (const Request&) = delete;
    //! move-constructor: default
    Request(Request&&) = default;
    //! move-assignment operator: default
    Request& operator = (Request&&) = default;

    virtual ~Request();

public:
    //! \name Accessors
    //! \{

    const FileBasePtr& file() const { return file_; }
    void * buffer() const { return buffer_; }
    offset_type offset() const { return offset_; }
    size_type bytes() const { return bytes_; }
    ReadOrWriteType type() const { return type_; }

    void check_alignment() const;

    //! Dumps properties of a request.
    std::ostream& print(std::ostream& out) const;

    //! Inform the request object that an error occurred during the I/O
    //! execution.
    void save_error(const mem::safe_string& msg) {
        error_ = mem::safe_make_unique<IoError>(msg);
    }

    //! return error if one occured
    IoError * error() const { return error_.get(); }

    //! Rises an exception if there was an error with the I/O.
    void check_error() {
        if (error_.get())
            throw *(error_.get());
    }

protected:
    void check_nref(bool after = false) {
        if (reference_count() < 2)
            check_nref_failed(after);
    }

private:
    void check_nref_failed(bool after);

    //! \}

    //! \name Request Completion State
    //! \{

public:
    //! states of request
    //! OP - operating, DONE - request served, READY2DIE - can be destroyed
    enum State { OP = 0, DONE = 1, READY2DIE = 2 };

    //! Suspends calling thread until completion of the request.
    void wait(bool measure_time = true);

    /*!
     * Cancel a request.
     *
     * The request is canceled unless already being processed.  However,
     * cancellation cannot be guaranteed.  Canceled requests must still be
     * waited for in order to ensure correct operation.  If the request was
     * canceled successfully, the completion handler will not be called.
     *
     * \return \c true iff the request was canceled successfully
     */
    virtual bool cancel();

    /*!
     * Polls the status of the request.
     *
     * \return \c true if request is completed, otherwise \c false
     */
    bool poll();

    /*!
     * Identifies the type of I/O implementation.
     *
     * \return pointer to null terminated string of characters, containing the
     * name of I/O implementation
     */
    const char * io_type() const;

protected:
    //! called by file implementations when the request completes
    virtual void completed(bool canceled);

private:
    //! state of the request.
    common::SharedState<State> state_ { OP };

    //! \}
};

//! make Request ostreamable
std::ostream& operator << (std::ostream& out, const Request& req);

//! deleter for Requests which are allocated from mem::g_pool.
class RequestDeleter
{
public:
    void operator () (Request* req) const;
};

//! A reference counting pointer for \c request.
using RequestPtr = tlx::CountingPtr<Request, RequestDeleter>;

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_REQUEST_HEADER

/******************************************************************************/
