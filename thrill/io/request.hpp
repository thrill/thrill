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

#include <thrill/common/counting_ptr.hpp>
#include <thrill/common/onoff_switch.hpp>
#include <thrill/common/state.hpp>
#include <thrill/io/completion_handler.hpp>
#include <thrill/io/exceptions.hpp>

#include <cassert>
#include <memory>
#include <mutex>
#include <set>

namespace thrill {
namespace io {

//! \addtogroup reqlayer
//! \{

#define STXXL_BLOCK_ALIGN 4096

class file;

//! Request object encapsulating basic properties like file and offset.
class request : public common::ReferenceCount
{
    friend class linuxaio_queue;

public:
    //! type for offsets within a file
    using offset_type = size_t;
    //! type for block transfer sizes
    using size_type = size_t;

    enum ReadOrWriteType { READ, WRITE };

protected:
    static const bool debug = false;

    completion_handler on_complete_;
    std::unique_ptr<io_error> error_;

protected:
    //! \name Base Parameter of an I/O Request
    //! \{

    //! file implementation to perform I/O with
    io::file* file_;
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
    request(const completion_handler& on_complete,
            file* file,
            void* buffer,
            offset_type offset,
            size_type bytes,
            ReadOrWriteType type);

    //! non-copyable: delete copy-constructor
    request(const request&) = delete;
    //! non-copyable: delete assignment operator
    request& operator = (const request&) = delete;
    //! move-constructor: default
    request(request&&) = default;
    //! move-assignment operator: default
    request& operator = (request&&) = default;

    virtual ~request();

public:
    //! \name Accessors
    //! \{

    io::file * file() const { return file_; }
    void * buffer() const { return buffer_; }
    offset_type offset() const { return offset_; }
    size_type bytes() const { return bytes_; }
    ReadOrWriteType type() const { return type_; }

    void check_alignment() const;

    //! Dumps properties of a request.
    std::ostream & print(std::ostream& out) const;

    //! Inform the request object that an error occurred during the I/O
    //! execution.
    void error_occured(const char* msg) {
        error_.reset(new io_error(msg));
    }

    //! Inform the request object that an error occurred during the I/O
    //! execution.
    void error_occured(const std::string& msg) {
        error_.reset(new io_error(msg));
    }

    //! Rises an exception if there were error with the I/O.
    void check_errors() {
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

    //! \name Waiters
    //! \{

public:
    //! add a waiter to notify on completion
    bool add_waiter(common::onoff_switch* sw);
    //! remove waiter to notify.
    void delete_waiter(common::onoff_switch* sw);
    //! returns number of waiters
    size_t num_waiters();

protected:
    //! called by the file implementation to notify all waiters
    void notify_waiters();

private:
    std::mutex waiters_mutex_;
    std::set<common::onoff_switch*> waiters_;

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
    common::state<State> state_ { OP };

    //! \}
};

static inline
std::ostream& operator << (std::ostream& out, const request& req) {
    return req.print(out);
}

//! A reference counting pointer for \c request.
using request_ptr = common::CountingPtr<request>;

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_REQUEST_HEADER

/******************************************************************************/
