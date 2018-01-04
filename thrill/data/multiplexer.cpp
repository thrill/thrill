/*******************************************************************************
 * thrill/data/multiplexer.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/data/multiplexer.hpp>

#include <thrill/data/cat_stream.hpp>
#include <thrill/data/mix_stream.hpp>
#include <thrill/data/multiplexer_header.hpp>
#include <thrill/data/stream.hpp>
#include <thrill/mem/aligned_allocator.hpp>

#include <tlx/math/round_to_power_of_two.hpp>

#include <algorithm>
#include <unordered_map>
#include <vector>

namespace thrill {
namespace data {

/******************************************************************************/
// Repository

/*!
 * A Repository holds obects that are shared among workers. Each object is
 * addressd via and Id. Workers can allocate new Id independetly but
 * deterministically (the repository will issue the same id sequence to all
 * workers).  Objects are created inplace via argument forwarding.
 */
template <typename Object>
class Repository
{
public:
    using Id = size_t;
    using ObjectPtr = tlx::CountingPtr<Object>;

    //! construct with initial ids 0.
    explicit Repository(size_t num_workers_per_node)
        : next_id_(num_workers_per_node, 0) { }

    //! Alllocates the next data target.
    //! Calls to this method alter the internal state -> order of calls is
    //! important and must be deterministic
    size_t AllocateId(size_t local_worker_id) {
        assert(local_worker_id < next_id_.size());
        return next_id_[local_worker_id]++;
    }

    //! Get object with given id, if it does not exist, create it.
    //! \param object_id of the object
    //! \param construction parameters forwards to constructor
    template <typename Subclass = Object, typename... Types>
    tlx::CountingPtr<Subclass>
    GetOrCreate(Id object_id, Types&& ... construction) {
        auto it = map_.find(object_id);

        if (it != map_.end()) {
            die_unless(dynamic_cast<Subclass*>(it->second.get()));
            return tlx::CountingPtr<Subclass>(
                dynamic_cast<Subclass*>(it->second.get()));
        }

        // construct new object
        tlx::CountingPtr<Subclass> value = tlx::make_counting<Subclass>(
            std::forward<Types>(construction) ...);

        map_.insert(std::make_pair(object_id, ObjectPtr(value)));
        return value;
    }

    template <typename Subclass = Object>
    tlx::CountingPtr<Subclass> GetOrDie(Id object_id) {
        auto it = map_.find(object_id);

        if (it != map_.end()) {
            die_unless(dynamic_cast<Subclass*>(it->second.get()));
            return tlx::CountingPtr<Subclass>(
                dynamic_cast<Subclass*>(it->second.get()));
        }

        die("object " + std::to_string(object_id) + " not in repository");
    }

    //! Remove id from map
    void EraseOrDie(Id object_id) {
        auto it = map_.find(object_id);
        if (it != map_.end()) {
            map_.erase(it);
            return;
        }
        die("object " + std::to_string(object_id) + " not in repository");
    }

    //! return mutable reference to map of objects.
    std::unordered_map<Id, ObjectPtr>& map() { return map_; }

private:
    //! Next ID to generate, one for each local worker.
    std::vector<size_t> next_id_;

    //! map containing value items
    std::unordered_map<Id, ObjectPtr> map_;
};

/******************************************************************************/
// Multiplexer

struct Multiplexer::Data {
    //! Streams have an ID in block headers. (worker id, stream id)
    Repository<StreamSetBase>         stream_sets_;

    //! array of number of open requests
    std::vector<std::atomic<size_t> > ongoing_requests_;

    explicit Data(size_t num_hosts, size_t workers_per_host)
        : stream_sets_(workers_per_host),
          ongoing_requests_(num_hosts) { }
};

Multiplexer::Multiplexer(mem::Manager& mem_manager, BlockPool& block_pool,
                         net::DispatcherThread& dispatcher, net::Group& group,
                         size_t workers_per_host)
    : mem_manager_(mem_manager),
      block_pool_(block_pool),
      dispatcher_(dispatcher),
      group_(group),
      workers_per_host_(workers_per_host),
      d_(std::make_unique<Data>(group_.num_hosts(), workers_per_host)) {

    num_parallel_async_ = group_.num_parallel_async();
    if (num_parallel_async_ == 0) {
        // one async at a time (for TCP and mock backends)
        num_parallel_async_ = 1;
    }
    else {
        // k/2 asyncs at a time (for MPI backend)
        num_parallel_async_ /= 2;
        // at least one
        num_parallel_async_ = std::max(size_t(1), num_parallel_async_);
    }

    // calculate send queue size limit for StreamData semaphores
    send_size_limit_ = block_pool.hard_ram_limit() / workers_per_host / 3;
    if (send_size_limit_ < 2 * default_block_size)
        send_size_limit_ = 2 * default_block_size;

    // launch initial async reads
    for (size_t id = 0; id < group_.num_hosts(); id++) {
        if (id == group_.my_host_rank()) continue;
        AsyncReadMultiplexerHeader(id, group_.connection(id));
    }
}

void Multiplexer::Close() {
    std::unique_lock<std::mutex> lock(mutex_);

    if (!d_->stream_sets_.map().empty()) {
        LOG1 << "Multiplexer::Close()"
             << " remaining_streams=" << d_->stream_sets_.map().size();
        die_unless(d_->stream_sets_.map().empty());
    }

    // destroy all still open Streams
    d_->stream_sets_.map().clear();

    closed_ = true;
}

Multiplexer::~Multiplexer() {
    if (!closed_)
        Close();

    group_.Close();
}

size_t Multiplexer::AllocateCatStreamId(size_t local_worker_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    return d_->stream_sets_.AllocateId(local_worker_id);
}

CatStreamDataPtr Multiplexer::GetOrCreateCatStreamData(
    size_t id, size_t local_worker_id, size_t dia_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    return IntGetOrCreateCatStreamData(id, local_worker_id, dia_id);
}

CatStreamPtr Multiplexer::GetNewCatStream(size_t local_worker_id, size_t dia_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    return tlx::make_counting<CatStream>(
        IntGetOrCreateCatStreamData(
            d_->stream_sets_.AllocateId(local_worker_id),
            local_worker_id, dia_id));
}

CatStreamDataPtr Multiplexer::IntGetOrCreateCatStreamData(
    size_t id, size_t local_worker_id, size_t dia_id) {
    CatStreamDataPtr ptr =
        d_->stream_sets_.GetOrCreate<CatStreamSet>(
            id, *this, send_size_limit_, id,
            workers_per_host_, dia_id)->Peer(local_worker_id);
    // update dia_id: the stream may have been created before the DIANode
    // associated with it.
    if (ptr->dia_id_ == 0)
        ptr->set_dia_id(dia_id);
    return ptr;
}

size_t Multiplexer::AllocateMixStreamId(size_t local_worker_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    return d_->stream_sets_.AllocateId(local_worker_id);
}

MixStreamDataPtr Multiplexer::GetOrCreateMixStreamData(
    size_t id, size_t local_worker_id, size_t dia_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    return IntGetOrCreateMixStreamData(id, local_worker_id, dia_id);
}

MixStreamPtr Multiplexer::GetNewMixStream(size_t local_worker_id, size_t dia_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    return tlx::make_counting<MixStream>(
        IntGetOrCreateMixStreamData(
            d_->stream_sets_.AllocateId(local_worker_id),
            local_worker_id, dia_id));
}

MixStreamDataPtr Multiplexer::IntGetOrCreateMixStreamData(
    size_t id, size_t local_worker_id, size_t dia_id) {
    MixStreamDataPtr ptr =
        d_->stream_sets_.GetOrCreate<MixStreamSet>(
            id, *this, send_size_limit_, id,
            workers_per_host_, dia_id)->Peer(local_worker_id);
    // update dia_id: the stream may have been created before the DIANode
    // associated with it.
    if (ptr->dia_id_ == 0)
        ptr->set_dia_id(dia_id);
    return ptr;
}

void Multiplexer::IntReleaseCatStream(size_t id, size_t local_worker_id) {

    tlx::CountingPtr<CatStreamSet> set =
        d_->stream_sets_.GetOrDie<CatStreamSet>(id);

    sLOG << "Multiplexer::IntReleaseCatStream() release"
         << "stream" << id << "local_worker_id" << local_worker_id;

    if (set->Release(local_worker_id)) {
        LOG << "Multiplexer::IntReleaseCatStream() destroy stream " << id;
        d_->stream_sets_.EraseOrDie(id);
    }
}

void Multiplexer::IntReleaseMixStream(size_t id, size_t local_worker_id) {

    tlx::CountingPtr<MixStreamSet> set =
        d_->stream_sets_.GetOrDie<MixStreamSet>(id);

    sLOG << "Multiplexer::IntReleaseMixStream() release"
         << "stream" << id << "local_worker_id" << local_worker_id;

    if (set->Release(local_worker_id)) {
        LOG << "Multiplexer::IntReleaseMixStream() destroy stream " << id;
        d_->stream_sets_.EraseOrDie(id);
    }
}

common::JsonLogger& Multiplexer::logger() {
    return block_pool_.logger();
}

/******************************************************************************/

void Multiplexer::AsyncReadMultiplexerHeader(size_t peer, Connection& s) {

    while (d_->ongoing_requests_[peer] < num_parallel_async_) {
        uint32_t seq = 42 + (s.rx_seq_.fetch_add(2) & 0xFFFF);
        dispatcher_.AsyncRead(
            s, seq, MultiplexerHeader::total_size,
            [this, peer, seq](Connection& s, net::Buffer&& buffer) {
                return OnMultiplexerHeader(peer, seq, s, std::move(buffer));
            });

        d_->ongoing_requests_[peer]++;
    }
}

void Multiplexer::OnMultiplexerHeader(
    size_t peer, uint32_t seq, Connection& s, net::Buffer&& buffer) {

    die_unless(d_->ongoing_requests_[peer] > 0);
    d_->ongoing_requests_[peer]--;

    // received invalid Buffer: the connection has closed?
    if (!buffer.IsValid()) return;

    net::BufferReader br(buffer);
    StreamMultiplexerHeader header = StreamMultiplexerHeader::Parse(br);

    LOG << "OnMultiplexerHeader() header"
        << " magic=" << unsigned(header.magic)
        << " size=" << header.size
        << " num_items=" << header.num_items
        << " first_item=" << header.first_item
        << " typecode_verify=" << header.typecode_verify
        << " stream_id=" << header.stream_id;

    // received stream id
    StreamId id = header.stream_id;
    size_t local_worker = header.receiver_local_worker;

    // round of allocation size to next power of two
    size_t alloc_size = header.size;
    if (alloc_size < THRILL_DEFAULT_ALIGN) alloc_size = THRILL_DEFAULT_ALIGN;
    alloc_size = tlx::round_up_to_power_of_two(alloc_size);

    if (header.magic == MagicByte::CatStreamBlock)
    {
        CatStreamDataPtr stream = GetOrCreateCatStreamData(
            id, local_worker, /* dia_id (unknown at this time) */ 0);
        stream->rx_net_bytes_ += buffer.size();

        if (header.IsEnd()) {
            sLOG << "end of stream on" << s << "in CatStream" << id
                 << "from worker" << header.sender_worker;

            stream->OnStreamBlock(
                header.sender_worker, header.seq, PinnedBlock());
        }
        else {
            sLOG << "stream header from" << s << "on CatStream" << id
                 << "from worker" << header.sender_worker
                 << "for local_worker" << local_worker
                 << "seq" << header.seq
                 << "size" << header.size;

            PinnedByteBlockPtr bytes = block_pool_.AllocateByteBlock(
                alloc_size, local_worker);
            sLOG << "new PinnedByteBlockPtr bytes=" << *bytes;

            d_->ongoing_requests_[peer]++;

            dispatcher_.AsyncRead(
                s, seq + 1, header.size, std::move(bytes),
                [this, peer, header, stream]
                    (Connection& s, PinnedByteBlockPtr&& bytes) {
                    OnCatStreamBlock(peer, s, header, stream, std::move(bytes));
                });
        }
    }
    else if (header.magic == MagicByte::MixStreamBlock)
    {
        MixStreamDataPtr stream = GetOrCreateMixStreamData(
            id, local_worker, /* dia_id (unknown at this time) */ 0);
        stream->rx_net_bytes_ += buffer.size();

        if (header.IsEnd()) {
            sLOG << "end of stream on" << s << "in MixStream" << id
                 << "from worker" << header.sender_worker;

            stream->OnStreamBlock(header.sender_worker, header.seq,
                                  PinnedBlock());
        }
        else {
            sLOG << "stream header from" << s << "on MixStream" << id
                 << "from worker" << header.sender_worker
                 << "for local_worker" << local_worker
                 << "seq" << header.seq
                 << "size" << header.size;

            PinnedByteBlockPtr bytes = block_pool_.AllocateByteBlock(
                alloc_size, local_worker);

            d_->ongoing_requests_[peer]++;

            dispatcher_.AsyncRead(
                s, seq + 1, header.size, std::move(bytes),
                [this, peer, header, stream]
                    (Connection& s, PinnedByteBlockPtr&& bytes) mutable {
                    OnMixStreamBlock(peer, s, header, stream, std::move(bytes));
                });
        }
    }
    else {
        die("Invalid magic byte in MultiplexerHeader");
    }

    AsyncReadMultiplexerHeader(peer, s);
}

void Multiplexer::OnCatStreamBlock(
    size_t peer, Connection& s, const StreamMultiplexerHeader& header,
    const CatStreamDataPtr& stream, PinnedByteBlockPtr&& bytes) {

    die_unless(d_->ongoing_requests_[peer] > 0);
    d_->ongoing_requests_[peer]--;

    sLOG << "Multiplexer::OnCatStreamBlock()"
         << "got block" << *bytes << "seq" << header.seq << "on" << s
         << "in CatStream" << header.stream_id
         << "from worker" << header.sender_worker;

    stream->OnStreamBlock(
        header.sender_worker, header.seq,
        PinnedBlock(std::move(bytes), /* begin */ 0, header.size,
                    header.first_item, header.num_items,
                    header.typecode_verify));

    if (header.is_last_block)
        stream->OnStreamBlock(header.sender_worker, header.seq + 1,
                              PinnedBlock());

    AsyncReadMultiplexerHeader(peer, s);
}

void Multiplexer::OnMixStreamBlock(
    size_t peer, Connection& s, const StreamMultiplexerHeader& header,
    const MixStreamDataPtr& stream, PinnedByteBlockPtr&& bytes) {

    die_unless(d_->ongoing_requests_[peer] > 0);
    d_->ongoing_requests_[peer]--;

    sLOG << "Multiplexer::OnMixStreamBlock()"
         << "got block" << *bytes << "seq" << header.seq << "on" << s
         << "in MixStream" << header.stream_id
         << "from worker" << header.sender_worker;

    stream->OnStreamBlock(
        header.sender_worker, header.seq,
        PinnedBlock(std::move(bytes), /* begin */ 0, header.size,
                    header.first_item, header.num_items,
                    header.typecode_verify));

    if (header.is_last_block)
        stream->OnStreamBlock(header.sender_worker, header.seq + 1,
                              PinnedBlock());

    AsyncReadMultiplexerHeader(peer, s);
}

CatStreamDataPtr Multiplexer::CatLoopback(
    size_t stream_id, size_t to_worker_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    return d_->stream_sets_.GetOrDie<CatStreamSet>(stream_id)
           ->Peer(to_worker_id);
}

MixStreamDataPtr Multiplexer::MixLoopback(
    size_t stream_id, size_t to_worker_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    return d_->stream_sets_.GetOrDie<MixStreamSet>(stream_id)
           ->Peer(to_worker_id);
}

} // namespace data
} // namespace thrill

/******************************************************************************/
