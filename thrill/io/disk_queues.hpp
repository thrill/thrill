/*******************************************************************************
 * thrill/io/disk_queues.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008-2010 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
 * Copyright (C) 2014 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_DISK_QUEUES_HEADER
#define THRILL_IO_DISK_QUEUES_HEADER

#include <thrill/common/singleton.hpp>
#include <thrill/io/file_base.hpp>
#include <thrill/io/request.hpp>
#include <thrill/io/request_queue.hpp>

namespace thrill {
namespace io {

//! \addtogroup io_layer_req
//! \{

//! Encapsulates disk queues.
//! \remark is a singleton
class DiskQueues : public common::Singleton<DiskQueues>
{
    friend class common::Singleton<DiskQueues>;

    //! pimpl data class
    struct Data;
    std::unique_ptr<Data> d_;

protected:
    DiskQueues();

public:
    using DiskId = int64_t;

    ~DiskQueues();

    void MakeQueue(const FileBasePtr& file);

    void AddRequest(RequestPtr& req, DiskId disk);

    /*!
     * Cancel a request.  The specified request is canceled unless already being
     * processed.  However, cancelation cannot be guaranteed.  Cancelled
     * requests must still be waited for in order to ensure correct operation.
     *
     * \param req request to cancel
     * \param disk disk number for disk that \c req was scheduled on
     * \return \c true iff the request was canceled successfully
     */
    bool CancelRequest(Request* req, DiskId disk);

    RequestQueue * GetQueue(DiskId disk);

    //! Changes requests priorities.
    //! \param op one of:
    //! - READ, read requests are served before write requests within a disk queue
    //! - WRITE, write requests are served before read requests within a disk queue
    //! - NONE, read and write requests are served by turns, alternately
    void SetPriorityOp(RequestQueue::PriorityOp op);
};

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_DISK_QUEUES_HEADER

/******************************************************************************/
