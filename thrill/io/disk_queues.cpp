/*******************************************************************************
 * thrill/io/disk_queues.cpp
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

#include <thrill/io/disk_queues.hpp>

#include <thrill/io/iostats.hpp>
#include <thrill/io/linuxaio_file.hpp>
#include <thrill/io/linuxaio_queue.hpp>
#include <thrill/io/linuxaio_request.hpp>
#include <thrill/io/request_queue_impl_qw_qr.hpp>
#include <thrill/io/serving_request.hpp>

#include <map>

namespace thrill {
namespace io {

using RequestQueueMap = std::map<DiskQueues::DiskId, RequestQueue*>;

struct DiskQueues::Data {
    RequestQueueMap queues;
};

DiskQueues::DiskQueues()
    : d_(std::make_unique<Data>()) {
    // initialize stats before ourselves
    Stats::GetInstance();
}

DiskQueues::~DiskQueues() {
    // deallocate all queues
    for (auto & it : d_->queues)
        delete it.second;
}

void DiskQueues::MakeQueue(const FileBasePtr& file) {
    int queue_id = file->get_queue_id();

    RequestQueueMap::iterator qi = d_->queues.find(queue_id);
    if (qi != d_->queues.end())
        return;

    // create new request queue
#if THRILL_HAVE_LINUXAIO_FILE
    if (const LinuxaioFile* af =
            dynamic_cast<const LinuxaioFile*>(file.get())) {
        d_->queues[queue_id] = new LinuxaioQueue(af->desired_queue_length());
        return;
    }
#endif
    d_->queues[queue_id] = new RequestQueueImplQwQr();
}

void DiskQueues::AddRequest(RequestPtr& req, DiskId disk) {
#ifdef THRILL_HACK_SINGLE_IO_THREAD
    disk = 42;
#endif
    RequestQueueMap::iterator qi = d_->queues.find(disk);
    RequestQueue* q = nullptr;
    if (qi == d_->queues.end())
    {
        // create new request queue
#if THRILL_HAVE_LINUXAIO_FILE
        if (dynamic_cast<LinuxaioRequest*>(req.get()))
            q = d_->queues[disk] = new LinuxaioQueue(
                    dynamic_cast<LinuxaioFile*>(req->file().get())
                    ->desired_queue_length());
        else
#endif
        q = d_->queues[disk] = new RequestQueueImplQwQr();
    }
    else
        q = qi->second;

    q->AddRequest(req);
}

bool DiskQueues::CancelRequest(Request* req, DiskId disk) {
#ifdef THRILL_HACK_SINGLE_IO_THREAD
    disk = 42;
#endif
    if (d_->queues.find(disk) != d_->queues.end())
        return d_->queues[disk]->CancelRequest(req);
    else
        return false;
}

RequestQueue* DiskQueues::GetQueue(DiskId disk) {
    if (d_->queues.find(disk) != d_->queues.end())
        return d_->queues[disk];
    else
        return nullptr;
}

void DiskQueues::SetPriorityOp(RequestQueue::PriorityOp op) {
    for (auto & it : d_->queues)
        it.second->SetPriorityOp(op);
}

} // namespace io
} // namespace thrill

/******************************************************************************/
