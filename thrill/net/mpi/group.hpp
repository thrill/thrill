/*******************************************************************************
 * thrill/net/mpi/group.hpp
 *
 * A Thrill network layer Implementation which uses MPI to transmit messages to
 * peers. Since MPI implementations are very bad at multi-threading, this
 * implementation is not recommended: it sequentialized all calls to the MPI
 * library (such that it does not deadlock), which _requires_ a busy-waiting
 * loop for new messages.
 *
 * Due to this restriction, the mpi::Group allows only **one Thrill host**
 * within a system process. We cannot start independent test threads as MPI
 * would not distinguish them.
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

#include <algorithm>
#include <string>
#include <vector>

namespace thrill {
namespace net {
namespace mpi {

//! \addtogroup net_mpi MPI Network API
//! \ingroup net
//! \{

class Dispatcher;

/*!
 * A derived exception class which looks up MPI error strings.
 */
class Exception : public net::Exception
{
public:
    explicit Exception(const std::string& what)
        : net::Exception(what) { }

    //! return the MPI error string
    static std::string GetErrorString(int error_code);

    Exception(const std::string& what, int error_code)
        : net::Exception(what + ": [" + std::to_string(error_code) + "] "
                         + GetErrorString(error_code)) { }
};

/*!
 * Virtual MPI connection class. As MPI has no real connections, this class is
 * just the integer which selected an MPI peer. Additionally, it contains the
 * group tag used to separate communication into groups.
 */
class Connection final : public net::Connection
{
    static const bool debug = false;

public:
    //! construct from group tag and MPI peer
    void Initialize(int group_tag, int peer) {
        group_tag_ = group_tag;
        peer_ = peer;
    }

    //! \name Base Status Functions
    //! \{

    bool IsValid() const final { return true; }

    //! return the MPI peer number
    int peer() const { return peer_; }

    std::string ToString() const final {
        return "peer: " + std::to_string(peer_);
    }

    std::ostream & OutputOstream(std::ostream& os) const final {
        return os << "[mpi::Connection"
                  << " group_tag_=" << group_tag_
                  << " peer_=" << peer_
                  << "]";
    }

    //! \}

    //! \name Send Functions
    //! \{

    void SyncSend(
        const void* data, size_t size, Flags /* flags */ = NoFlags) final;

    ssize_t SendOne(
        const void* data, size_t size, Flags /* flags */ = NoFlags) final;

    //! \}

    //! \name Receive Functions
    //! \{

    void SyncRecv(void* out_data, size_t size) final;

    ssize_t RecvOne(void* out_data, size_t size) final {
        SyncRecv(out_data, size);
        return size;
    }

    //! \}

protected:
    //! Group number used as MPI tag.
    int group_tag_;

    //! Outgoing peer id of this Connection.
    int peer_;
};

/*!
 * A net group backed by virtual MPI connection. As MPI already sets up
 * communication, not much is done. Each Group communicates using a unique MPI
 * tag, the group id. Each host's rank within the group is plaining its MPI
 * rank.
 */
class Group final : public net::Group
{
    static const bool debug = false;

public:
    //! \name Base Functions
    //! \{

    //! Initialize a Group for the given size and rank
    Group(size_t my_rank, int group_tag, size_t group_size)
        : net::Group(my_rank),
          group_tag_(group_tag),
          conns_(group_size) {
        // create virtual connections
        for (size_t i = 0; i < group_size; ++i)
            conns_[i].Initialize(group_tag, static_cast<int>(i));
    }

    //! return MPI tag used to communicate
    int group_tag() const { return group_tag_; }

    //! number of hosts configured.
    size_t num_hosts() const final { return conns_.size(); }

    net::Connection & connection(size_t peer) final {
        assert(peer < conns_.size());
        return conns_[peer];
    }

    void Close() final { }

    //! construct a mpi::Dispatcher exclusively for this Group.
    mem::mm_unique_ptr<net::Dispatcher> ConstructDispatcher(
        mem::Manager& mem_manager) const final;

    //! run a MPI_Barrier() for synchronization.
    void Barrier();

    //! \}

protected:
    //! this group's MPI tag
    int group_tag_;

    //! vector of virtual connection objects to remote peers
    std::vector<Connection> conns_;
};

/*!
 * Construct Group which connects to peers using MPI. As the MPI environment
 * already defines the connections, no hosts or parameters can be
 * given. Constructs group_count mpi::Group objects at once. Within each Group
 * this host has its MPI rank.
 *
 * To enable tests with smaller group sizes, the Construct method takes
 * group_size and returns a Group with *less* hosts than actual MPI processes!
 * Obviously, group_size must be less-or-equal to the number of processes
 * started with mpirun -np.
 */
bool Construct(size_t group_size,
               std::unique_ptr<Group>* groups, size_t group_count);

/*!
 * Return the number of MPI processes. This is the maximum group size.
 */
size_t NumMpiProcesses();

//! Return the rank of this process in the MPI COMM WORLD.
size_t MpiRank();

//! \}

} // namespace mpi
} // namespace net
} // namespace thrill

#endif // !THRILL_NET_MPI_GROUP_HEADER

/******************************************************************************/
