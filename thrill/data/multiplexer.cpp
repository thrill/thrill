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
#include <map>
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
    std::map<Id, ObjectPtr>& map() { return map_; }

private:
    //! Next ID to generate, one for each local worker.
    std::vector<size_t> next_id_;

    //! map containing value items
    std::map<Id, ObjectPtr> map_;
};

/******************************************************************************/
// Multiplexer

struct Multiplexer::Data {
    //! Streams have an ID in block headers. (worker id, stream id)
    Repository<StreamSetBase> stream_sets_;

    explicit Data(size_t workers_per_host)
        : stream_sets_(workers_per_host) { }
};

Multiplexer::Multiplexer(mem::Manager& mem_manager,
                         BlockPool& block_pool,
                         size_t workers_per_host, net::Group& group)
    : mem_manager_(mem_manager),
      block_pool_(block_pool),
      dispatcher_(
          mem_manager, group,
          "host " + mem::to_string(group.my_host_rank()) + " multiplexer"),
      group_(group),
      workers_per_host_(workers_per_host),
      d_(std::make_unique<Data>(workers_per_host)) {
    for (size_t id = 0; id < group_.num_hosts(); id++) {
        if (id == group_.my_host_rank()) continue;
        AsyncReadMultiplexerHeader(group_.connection(id));
    }
    (void)mem_manager_;     // silence unused variable warning.
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

    // terminate dispatcher, this waits for unfinished AsyncWrites.
    dispatcher_.Terminate();

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
            id, *this, id, workers_per_host_, dia_id)->Peer(local_worker_id);
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
            id, *this, id, workers_per_host_, dia_id)->Peer(local_worker_id);
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

//! expects the next MultiplexerHeader from a socket and passes to
//! OnMultiplexerHeader
void Multiplexer::AsyncReadMultiplexerHeader(Connection& s) {
    dispatcher_.AsyncRead(
        s, MultiplexerHeader::total_size,
        net::AsyncReadCallback::make<
            Multiplexer, &Multiplexer::OnMultiplexerHeader>(this));
}

void Multiplexer::OnMultiplexerHeader(Connection& s, net::Buffer&& buffer) {

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

            stream->OnCloseStream(header.sender_worker);

            AsyncReadMultiplexerHeader(s);
        }
        else {
            sLOG << "stream header from" << s << "on CatStream" << id
                 << "from worker" << header.sender_worker
                 << "for local_worker" << local_worker;

            PinnedByteBlockPtr bytes = block_pool_.AllocateByteBlock(
                alloc_size, local_worker);

            dispatcher_.AsyncRead(
                s, header.size, std::move(bytes),
                [this, header, stream](Connection& s, PinnedByteBlockPtr&& bytes) {
                    OnCatStreamBlock(s, header, stream, std::move(bytes));
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

            stream->OnCloseStream(header.sender_worker);

            AsyncReadMultiplexerHeader(s);
        }
        else {
            sLOG << "stream header from" << s << "on MixStream" << id
                 << "from worker" << header.sender_worker
                 << "for local_worker" << local_worker;

            PinnedByteBlockPtr bytes = block_pool_.AllocateByteBlock(
                alloc_size, local_worker);

            dispatcher_.AsyncRead(
                s, header.size, std::move(bytes),
                [this, header, stream](Connection& s, PinnedByteBlockPtr&& bytes) mutable {
                    OnMixStreamBlock(s, header, stream, std::move(bytes));
                });
        }
    }
    else {
        die("Invalid magic byte in MultiplexerHeader");
    }
}

void Multiplexer::OnCatStreamBlock(
    Connection& s, const StreamMultiplexerHeader& header,
    const CatStreamDataPtr& stream, PinnedByteBlockPtr&& bytes) {

    sLOG << "Multiplexer::OnCatStreamBlock()"
         << "got block on" << s
         << "in CatStream" << header.stream_id
         << "from worker" << header.sender_worker;

    stream->OnStreamBlock(
        header.sender_worker,
        PinnedBlock(std::move(bytes), 0, header.size,
                    header.first_item, header.num_items,
                    header.typecode_verify));

    if (header.is_last_block)
        stream->OnCloseStream(header.sender_worker);

    AsyncReadMultiplexerHeader(s);
}

void Multiplexer::OnMixStreamBlock(
    Connection& s, const StreamMultiplexerHeader& header,
    const MixStreamDataPtr& stream, PinnedByteBlockPtr&& bytes) {

    sLOG << "Multiplexer::OnMixStreamBlock()"
         << "got block on" << s
         << "in MixStream" << header.stream_id
         << "from worker" << header.sender_worker;

    stream->OnStreamBlock(
        header.sender_worker,
        PinnedBlock(std::move(bytes), 0, header.size,
                    header.first_item, header.num_items,
                    header.typecode_verify));

    if (header.is_last_block)
        stream->OnCloseStream(header.sender_worker);

    AsyncReadMultiplexerHeader(s);
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
