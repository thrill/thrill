/*******************************************************************************
 * c7a/net/net-group.cpp
 *
 * NetGroup is a collection of NetConnections providing simple MPI-like
 * collectives and point-to-point communication.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/net/net-group.hpp>
#include <c7a/net/net-dispatcher.hpp>

#include <deque>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace c7a {

namespace net {

template <typename Functional, size_t BufferSize = 0>
class NetReadBuffer
{
public:
    //! Construct buffered reader with callback
    NetReadBuffer(lowlevel::Socket& socket, size_t buffer_size = BufferSize,
                  const Functional& functional = Functional())
        : functional_(functional),
          buffer_(buffer_size, 0)
    {
        if (buffer_size == 0)
            functional_(socket, buffer_);
    }

    //! Should be called when the socket is readable
    bool operator () (lowlevel::Socket& s)
    {
        int r = s.recv_one(const_cast<char*>(buffer_.data() + size_),
                           buffer_.size() - size_);

        if (r < 0)
            throw NetException("NetReadBuffer() error in recv", errno);

        size_ += r;

        if (size_ == buffer_.size()) {
            functional_(s, buffer_);
            return false;
        }
        else {
            return true;
        }
    }

private:
    //! total size currently read
    size_t size_ = 0;

    //! functional object to call once data is complete
    Functional functional_;

    //! Receive buffer
    std::string buffer_;
};

NetGroup::NetGroup(ClientId my_rank,
                   const std::vector<NetEndpoint>& endpoints)
    : my_rank_(my_rank),
      connections_(endpoints.size())
{
    die_unless(my_rank_ < endpoints.size());

    using lowlevel::Socket;
    using lowlevel::SocketAddress;

    // resolve all endpoint addresses

    std::vector<SocketAddress> addrlist;

    for (const NetEndpoint& ne : endpoints)
    {
        addrlist.push_back(SocketAddress(ne.hostport));
        if (!addrlist.back().IsValid()) {
            throw NetException(
                      "Error resolving NetEndpoint " + ne.hostport
                      + ": " + addrlist.back().GetResolveError());
        }
    }

    // construct listen socket
    {
        Socket listenSocket = Socket::Create();
        listenSocket.SetReuseAddr();

        const SocketAddress& lsa = addrlist[my_rank_];

        if (listenSocket.bind(lsa) != 0)
            throw NetException("Could not bind listen socket to "
                               + lsa.ToStringHostPort(), errno);

        if (listenSocket.listen() != 0)
            throw NetException("Could not listen on socket "
                               + lsa.ToStringHostPort(), errno);

        listener_ = std::move(NetConnection(listenSocket));
    }

    // TODO(tb): this sleep waits for other clients to open their ports. do this
    // differently.
    sleep(1);

    // Transfer Welcome message to other clients and receive from all other to
    // synchronize group.

    struct WelcomeMsg
    {
        uint32_t c7a;
        ClientId id;
    };

    static const uint32_t c7a_sign = 0x0C7A0C7A;
    const WelcomeMsg my_welcome = { c7a_sign, my_rank_ };

    // construct list of welcome message read buffers, we really need a deque
    // here due to reallocation in vector

    unsigned int got_connections = 0;

    auto MsgReaderLambda =
        [this, &got_connections](
            NetConnection& c, const std::string& buffer)
        {
            LOG0 << "Message on " << c;
            die_unequal(buffer.size(), sizeof(WelcomeMsg));

            const WelcomeMsg* msg = reinterpret_cast<const WelcomeMsg*>(buffer.data());
            die_unequal(msg->c7a, c7a_sign);

            LOG0 << "client " << my_rank_ << " got signature "
                 << "from client " << msg->id;

            // assign connection.
            die_unless(!connections_[msg->id].GetSocket().IsValid());
            std::swap(connections_[msg->id], c);
            ++got_connections;
        };

    // Perform select loop waiting for incoming connections and fulfilled
    // outgoing connections.

    NetDispatcher disp;

    // initiate connections to all hosts with higher id.

    std::deque<NetConnection> netconn;

    for (ClientId id = my_rank_ + 1; id < addrlist.size(); ++id)
    {
        netconn.emplace_back(Socket::Create());
        NetConnection& nc = netconn.back();

        // initiate non-blocking TCP connection
        nc.SetNonBlocking(true);

        auto on_connect =
            [&, id](NetConnection& s) -> bool {
                int err = s.GetError();

                if (err != 0) {
                    throw NetException(
                              "OpenConnections() could not connect to client "
                              + std::to_string(id) + " via "
                              + addrlist[id].ToStringHostPort(), err);
                }

                LOG << "OpenConnections() " << my_rank_ << " connected"
                    << " fd=" << s.GetSocket().fd()
                    << " to=" << s.GetPeerAddress();

                // send welcome message
                s.SetNonBlocking(false);
                disp.AsyncWrite(s, &my_welcome, sizeof(my_welcome));
                LOG << "sent client " << my_welcome.id;

                // wait for welcome message from other side
                disp.AsyncRead(s, sizeof(my_welcome), MsgReaderLambda);

                return false;
            };

        if (nc.GetSocket().connect(addrlist[id]) == 0) {
            // connect() already successful? this should not be.
            on_connect(nc);
        }
        else if (errno == EINPROGRESS) {
            // connect is in progress, will wait for completion.
            disp.AddWrite(nc, on_connect);
        }
        else {
            throw NetException("Could not connect to client "
                               + std::to_string(id) + " via "
                               + addrlist[id].ToStringHostPort(), errno);
        }
    }

    // wait for incoming connections from lower ids

    unsigned int accepting = my_rank_;

    disp.AddRead(
        listener_,
        [&](NetConnection& s) -> bool
        {
                // new accept()able connection on listen socket
            die_unless(accepting > 0);

            netconn.emplace_back(s.GetSocket().accept());
            NetConnection& nc = netconn.back();

            LOG << "OpenConnections() " << my_rank_ << " accepted connection"
                << " fd=" << nc.GetSocket().fd()
                << " from=" << nc.GetPeerAddress();

            // send welcome message
            disp.AsyncWrite(nc, &my_welcome, sizeof(my_welcome));
            LOG << "sent client " << my_welcome.id;

            // wait for welcome message from other side
            disp.AsyncRead(nc, sizeof(my_welcome), MsgReaderLambda);

            // wait for more connections?
            return (--accepting > 0);
        });

    while (got_connections != addrlist.size() - 1)
    {
        disp.Dispatch();
    }

    LOG << "done";

    // output list of file descriptors connected to partners
    for (size_t i = 0; i != connections_.size(); ++i) {
        LOG << "NetGroup link " << my_rank_ << " -> " << i << " = fd "
            << connections_[i].GetSocket().fd();
    }
}

void NetGroup::ExecuteLocalMock(
    size_t num_clients,
    const std::function<void(NetGroup*)>& thread_function)
{
    using lowlevel::Socket;

    // construct a group of num_clients
    std::vector<std::unique_ptr<NetGroup> > group(num_clients);

    for (size_t i = 0; i != num_clients; ++i) {
        group[i] = std::unique_ptr<NetGroup>(new NetGroup(i, num_clients));
    }

    // construct a stream socket pair for (i,j) with i < j
    for (size_t i = 0; i != num_clients; ++i) {
        for (size_t j = i + 1; j < num_clients; ++j) {
            LOG << "doing Socket::CreatePair() for i=" << i << " j=" << j;

            std::pair<Socket, Socket> sp = Socket::CreatePair();

            group[i]->connections_[j] = std::move(NetConnection(sp.first));
            group[j]->connections_[i] = std::move(NetConnection(sp.second));
        }
    }

    // create a thread for each NetGroup object and run user program.
    std::vector<std::thread> threads(num_clients);

    for (size_t i = 0; i != num_clients; ++i) {
        threads[i] = std::thread(
            std::bind(thread_function, group[i].get()));
    }

    for (size_t i = 0; i != num_clients; ++i) {
        threads[i].join();
    }
}

} // namespace net

} // namespace c7a

/******************************************************************************/
