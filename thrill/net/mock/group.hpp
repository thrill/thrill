/*******************************************************************************
 * thrill/net/mock/group.hpp
 *
 * Implementation of a mock network which does no real communication. All
 * classes: Group, Connection, and Dispatcher are in this file since they are
 * tightly interdependent to provide thread-safety.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_MOCK_GROUP_HEADER
#define THRILL_NET_MOCK_GROUP_HEADER

#include <thrill/net/dispatcher.hpp>
#include <thrill/net/group.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace net {
namespace mock {

//! \addtogroup net_mock Mock Network API
//! \ingroup net
//! \{

class Group;
class Dispatcher;

/*!
 * A virtual connection through the mock network: each Group has p Connections
 * to its peers. The Connection hands over packets to the peers via SyncSend(),
 * and receives them as InboundMsg().
 */
class Connection final : public net::Connection
{
public:
    //! construct from mock::Group
    void Initialize(Group* group, size_t peer);

    //! Method which is called by other peers to enqueue a message.
    void InboundMsg(net::Buffer&& msg);

    //! \name Base Status Functions
    //! \{

    bool IsValid() const final { return true; }

    std::string ToString() const final;

    std::ostream& OutputOstream(std::ostream& os) const final;

    //! \}

    //! \name Send Functions
    //! \{

    void SyncSend(
        const void* data, size_t size, Flags /* flags */ = NoFlags) final;

    ssize_t SendOne(
        const void* data, size_t size, Flags flags = NoFlags) final;

    //! \}

    //! \name Receive Functions
    //! \{

    void SyncRecv(void* out_data, size_t size) final;

    ssize_t RecvOne(void* out_data, size_t size) final;

    //! \}

private:
    //! Reference to our group.
    Group* group_ = nullptr;

    //! Outgoing peer id of this Connection.
    size_t peer_ = size_t(-1);

    //! for access to watch lists and mutex.
    friend class Dispatcher;

    //! pimpl data struct with complex components
    struct Data;

    //! pimpl data struct with complex components
    std::unique_ptr<Data> d_;

    //! some-what internal function to extract the next packet from the queue.
    net::Buffer RecvNext();
};

/*!
 * The central object of a mock network: the Group containing links to other
 * mock Group forming the network. Note that there is no central object
 * containing all Groups.
 */
class Group final : public net::Group
{
    static constexpr bool debug = false;
    static constexpr bool debug_data = true;

public:
    //! \name Base Functions
    //! \{

    //! Initialize a Group for the given size  rank
    Group(size_t my_rank, size_t group_size);

    ~Group();

    size_t num_hosts() const final;

    net::Connection& connection(size_t peer) final;

    void Close() final;

    std::unique_ptr<net::Dispatcher> ConstructDispatcher(
        mem::Manager& mem_manager) const final;

    //! \}

    /*!
     * Construct a mock network with num_hosts peers and deliver Group contexts
     * for each of them.
     */
    static std::vector<std::unique_ptr<Group> > ConstructLoopbackMesh(
        size_t num_hosts);

    //! return hexdump or just [data] if not debugging
    static std::string MaybeHexdump(const void* data, size_t size);

private:
    //! vector of peers for delivery of messages.
    std::vector<Group*> peers_;

    //! vector of virtual connection objects to remote peers
    Connection* conns_;

    //! Send a buffer to peer tgt. Blocking, ... sort of.
    void Send(size_t tgt, net::Buffer&& msg);

    //! for access to Send()
    friend class Connection;
};

/*!
 * A virtual Dispatcher which waits for messages to arrive in the mock
 * network. It is implemented as a ConcurrentBoundedQueue, into which
 * Connections lay notification events.
 */
class Dispatcher final : public net::Dispatcher
{
    static constexpr bool debug = false;

public:
    //! type for file descriptor readiness callbacks
    using Callback = AsyncCallback;

    explicit Dispatcher(mem::Manager& mem_manager);

    //! \name Implementation of Virtual Methods
    //! \{

    void AddRead(net::Connection& _c, const Callback& read_cb) final;

    void AddWrite(net::Connection& _c, const Callback& write_cb) final;

    void Cancel(net::Connection&) final;

    void Notify(Connection* c);

    void Interrupt() final;

    void DispatchOne(const std::chrono::milliseconds& timeout) final;

    //! \}

private:
    //! pimpl data struct with complex components
    struct Data;

    //! pimpl data struct with complex components
    std::unique_ptr<Data> d_;

    //! callback vectors per watched connection
    struct Watch;

    //! lookup method
    Watch& GetWatch(Connection* c);
};

//! \}

} // namespace mock
} // namespace net
} // namespace thrill

#endif // !THRILL_NET_MOCK_GROUP_HEADER

/******************************************************************************/
