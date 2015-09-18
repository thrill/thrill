/*******************************************************************************
 * thrill/net/mpi/group.hpp
 *
 * A Thrill network layer Implementation which uses MPI to transmit messages to
 * peers. Since MPI implementations are very bad at multi-threading, this
 * implementation is not recommended: it sequentialized all calls to the MPI
 * library (such that it does not deadlock), which _requires_ a busy-waiting
 * loop for new messages.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_MPI_GROUP_HEADER
#define THRILL_NET_MPI_GROUP_HEADER

#include <thrill/net/dispatcher.hpp>
#include <thrill/net/group.hpp>

#include <mpi.h>

#include <algorithm>
#include <deque>
#include <mutex>
#include <string>
#include <vector>

namespace thrill {
namespace net {
namespace mpi {

//! \addtogroup net_mpi MPI Network API
//! \ingroup net
//! \{

class Group;
class Dispatcher;

//! The Grand MPI Library Invocation Mutex (The GMLIM)
extern std::mutex g_mutex;

class Exception : public net::Exception
{
public:
    explicit Exception(const std::string& what)
        : net::Exception(what) { }

    static std::string GetErrorString(int error_code) {
        char string[MPI_MAX_ERROR_STRING];
        int resultlen;
        MPI_Error_string(error_code, string, &resultlen);
        return std::string(string, resultlen);
    }

    Exception(const std::string& what, int error_code)
        : net::Exception(what + ": [" + std::to_string(error_code) + "] "
                         + GetErrorString(error_code)) { }
};

class Connection final : public net::Connection
{
public:
    //! construct from mpi::Group
    void Initialize(size_t group, size_t peer) {
        group_ = group;
        peer_ = peer;
    }

    //! \name Base Status Functions
    //! \{

    bool IsValid() const final { return true; }

    size_t peer() const { return peer_; }

    std::string ToString() const final {
        return "peer: " + std::to_string(peer_);
    }

    std::ostream & output_ostream(std::ostream& os) const final {
        return os << "[mpi::Connection"
                  << " group=" << group_
                  << " peer=" << peer_
                  << "]";
    }

    //! \}

    //! \name Send Functions
    //! \{

    void SyncSend(
        const void* data, size_t size, Flags /* flags */ = NoFlags) final {
        std::unique_lock<std::mutex> lock(g_mutex);

        int r = MPI_Send(const_cast<void*>(data), size, MPI_BYTE,
                         peer_, group_, MPI_COMM_WORLD);

        if (r != MPI_SUCCESS)
            throw Exception("Error during SyncSend", r);
    }

    ssize_t SendOne(
        const void* data, size_t size, Flags /* flags */ = NoFlags) final {
        std::unique_lock<std::mutex> lock(g_mutex);

        MPI_Request request;
        int r = MPI_Isend(const_cast<void*>(data), size, MPI_BYTE,
                          peer_, group_, MPI_COMM_WORLD, &request);

        if (r != MPI_SUCCESS)
            throw Exception("Error during SyncOne", r);

        MPI_Request_free(&request);

        return size;
    }

    //! \}

    //! \name Receive Functions
    //! \{

    void SyncRecv(void* out_data, size_t size) final {
        std::unique_lock<std::mutex> lock(g_mutex);

        MPI_Status status;
        int r = MPI_Recv(out_data, size, MPI_BYTE,
                         peer_, group_, MPI_COMM_WORLD, &status);
        if (r != MPI_SUCCESS)
            throw Exception("Error during MPI_Recv()", r);

        int count;
        r = MPI_Get_count(&status, MPI_BYTE, &count);
        if (r != MPI_SUCCESS)
            throw Exception("Error during MPI_Get_count()", r);

        if (static_cast<size_t>(count) != size)
            throw Exception("Error during SyncRecv: message truncated?");
    }

    ssize_t RecvOne(void* out_data, size_t size) final {
        SyncRecv(out_data, size);
        return size;
    }

    //! \}

protected:
    //! Group number used as MPI tag.
    size_t group_;

    //! Outgoing peer id of this Connection.
    size_t peer_;
};

class Group final : public net::Group
{
    static const bool debug = false;
    static const bool debug_data = true;

public:
    //! \name Base Functions
    //! \{

    //! Initialize a Group for the given size and rank
    Group(size_t my_rank, size_t group_tag, size_t group_size)
        : net::Group(my_rank),
          group_tag_(group_tag),
          conns_(group_size) {
        // create virtual connections
        for (size_t i = 0; i < group_size; ++i)
            conns_[i].Initialize(group_tag, i);
    }

    size_t group_tag() const { return group_tag_; }

    size_t num_hosts() const final { return conns_.size(); }

    net::Connection & connection(size_t peer) final {
        assert(peer < conns_.size());
        return conns_[peer];
    }

    void Close() final { }

    mem::mm_unique_ptr<net::Dispatcher> ConstructDispatcher(
        mem::Manager& mem_manager) const final;

    //! \}

    //! return hexdump or just [data] if not debugging
    static std::string maybe_hexdump(const void* data, size_t size) {
        if (debug_data)
            return common::hexdump(data, size);
        else
            return "[data]";
    }

protected:
    //! this group's MPI tag
    size_t group_tag_;

    //! vector of virtual connection objects to remote peers
    std::vector<Connection> conns_;
};

static inline void Deconstruct() {
    std::unique_lock<std::mutex> lock(g_mutex);

    MPI_Finalize();
}

/*!
 * Construct Group which connects to peers using MPI. As the MPI environment
 * already defines the connections, no hosts or parameters can be
 * given. Constructs group_count mpi::Group objects at once. Within each Group
 * this host has its MPI rank.
 */
static inline
bool Construct(size_t group_size,
               std::unique_ptr<Group>* groups, size_t group_count) {
    std::unique_lock<std::mutex> lock(g_mutex);

    int flag;
    int r = MPI_Initialized(&flag);
    if (r != MPI_SUCCESS)
        throw Exception("Error during MPI_Initialized()", r);

    if (!flag) {
        // fake command line
        int argc = 1;
        const char* argv[] = { "thrill", nullptr };

        int provided;
        int r = MPI_Init_thread(&argc, reinterpret_cast<char***>(&argv),
                                MPI_THREAD_SERIALIZED, &provided);
        if (r != MPI_SUCCESS)
            throw Exception("Error during MPI_Init_thread()", r);

        if (provided < MPI_THREAD_SERIALIZED)
            throw Exception("MPI_Init_thread() provided less than MPI_THREAD_SERIALIZED");

        // register atexit method
        atexit(&Deconstruct);
    }

    int my_rank;
    r = MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    if (r != MPI_SUCCESS)
        throw Exception("Error during MPI_Comm_rank()", r);

    int num_mpi_hosts;
    r = MPI_Comm_size(MPI_COMM_WORLD, &num_mpi_hosts);
    if (r != MPI_SUCCESS)
        throw Exception("Error during MPI_Comm_size()", r);

    if (group_size > static_cast<size_t>(num_mpi_hosts))
        throw Exception("mpi::Construct(): fewer MPI processes than hosts requested.");

    for (size_t i = 0; i < group_count; i++) {
        groups[i] = std::make_unique<Group>(my_rank, i, group_size);
    }

    return (static_cast<size_t>(my_rank) < group_size);
}

class Dispatcher final : public net::Dispatcher
{
    static const bool debug = false;

public:
    //! type for file descriptor readiness callbacks
    using Callback = AsyncCallback;

    //! constructor
    Dispatcher(mem::Manager& mem_manager,
               size_t group_tag, size_t group_size)
        : net::Dispatcher(mem_manager),
          group_tag_(group_tag) {
        watch_.reserve(group_size);
        for (size_t i = 0; i < group_size; ++i)
            watch_.emplace_back(mem_manager_);
    }

    //! Register a buffered read callback and a default exception callback.
    void AddRead(net::Connection& c, const Callback& read_cb) final {
        assert(dynamic_cast<Connection*>(&c));
        Connection& mc = static_cast<Connection&>(c);
        size_t p = mc.peer();
        assert(p < watch_.size());
        watch_[p].active = true;
        watch_[p].read_cb.emplace_back(read_cb);
    }

    //! Register a buffered write callback and a default exception callback.
    void AddWrite(net::Connection& c, const Callback& write_cb) final {
        assert(dynamic_cast<Connection*>(&c));
        Connection& mc = static_cast<Connection&>(c);
        size_t p = mc.peer();
        assert(p < watch_.size());
        watch_[p].active = true;
        watch_[p].write_cb.emplace_back(write_cb);
    }

    //! Register a buffered write callback and a default exception callback.
    void SetExcept(net::Connection& c, const Callback& except_cb) {
        assert(dynamic_cast<Connection*>(&c));
        Connection& mc = static_cast<Connection&>(c);
        size_t p = mc.peer();
        assert(p < watch_.size());
        watch_[p].active = true;
        watch_[p].except_cb = except_cb;
    }

    //! Cancel all callbacks on a given fd.
    void Cancel(net::Connection& c) final {
        assert(dynamic_cast<Connection*>(&c));
        Connection& mc = static_cast<Connection&>(c);
        size_t p = mc.peer();
        assert(p < watch_.size());

        if (watch_[p].read_cb.size() == 0 &&
            watch_[p].write_cb.size() == 0)
            LOG << "SelectDispatcher::Cancel() peer=" << p
                << " called with no callbacks registered.";

        Watch& w = watch_[p];
        w.read_cb.clear();
        w.write_cb.clear();
        w.except_cb = Callback();
        w.active = false;
    }

    //! Run one iteration of dispatching select().
    void DispatchOne(const std::chrono::milliseconds& /* timeout */) final {

        for (size_t i = 0; i < watch_.size(); ++i) {
            if (!watch_[i].active) continue;
            Watch& w = watch_[i];

            while (w.write_cb.size()) {
                sLOG << "calling writer.";
                if (w.write_cb.front()()) break;
                w.write_cb.pop_front();
            }

            if (w.read_cb.size() == 0 && w.write_cb.size() == 0) {
                w.active = false;
            }
        }

        int flag = 0;
        MPI_Status status;

        {
            std::unique_lock<std::mutex> lock(g_mutex);

            int r = MPI_Iprobe(MPI_ANY_SOURCE, group_tag_, MPI_COMM_WORLD,
                               &flag, &status);

            if (r != MPI_SUCCESS)
                throw Exception("Error during MPI_Iprobe()", r);
        }

        // check whether probe was successful
        if (flag == 0) return;

        // get the right watch
        int p = status.MPI_SOURCE;
        assert(p >= 0 && static_cast<size_t>(p) < watch_.size());

        Watch& w = watch_[p];

        if (!w.active) {
            sLOG << "Got Iprobe() for unwatched peer" << p;
            return;
        }

        sLOG << "Got iprobe for peer" << p;

        if (w.read_cb.size()) {
            // run read callbacks until one returns true (in which case it wants
            // to be called again), or the read_cb list is empty.
            while (w.read_cb.size() && w.read_cb.front()() == false) {
                w.read_cb.pop_front();
            }

            if (w.read_cb.size() == 0 && w.write_cb.size() == 0) {
                w.active = false;
            }
        }
        else {
            LOG << "Dispatcher: got Iprobe() for peer "
                << p << " without a read handler.";
        }
    }

    //! Interrupt the current select via self-pipe
    void Interrupt() final { }

protected:
    //! group_tag attached to this Dispatcher
    size_t group_tag_;

    //! callback vectors per peer
    struct Watch
    {
        //! boolean check whether any callbacks are registered
        bool                    active = false;
        //! queue of callbacks for fd.
        mem::mm_deque<Callback> read_cb, write_cb;
        //! only one exception callback for the fd.
        Callback                except_cb;

        explicit Watch(mem::Manager& mem_manager)
            : read_cb(mem::Allocator<Callback>(mem_manager)),
              write_cb(mem::Allocator<Callback>(mem_manager)) { }
    };

    //! handlers for each MPI peer.
    mem::mm_vector<Watch> watch_ { mem::Allocator<Watch>(mem_manager_) };

    //! Default exception handler
    static bool DefaultExceptionCallback() {
        throw Exception("SelectDispatcher() exception on socket!", errno);
    }
};

//! \}

} // namespace mpi
} // namespace net
} // namespace thrill

#endif // !THRILL_NET_MPI_GROUP_HEADER

/******************************************************************************/
