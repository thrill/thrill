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
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/net/net-group.hpp>
#include <c7a/net/select-dispatcher.hpp>

#include <thread>
#include <deque>

namespace c7a {

template <typename Functional, size_t BufferSize = 0>
class NetReadBuffer
{
public:
    //! Construct buffered reader with callback
    NetReadBuffer(Socket& socket, size_t buffer_size = BufferSize,
                  const Functional& functional = Functional())
        : functional_(functional),
          buffer_(buffer_size, 0)
    {
        if (buffer_size == 0)
            functional_(socket, buffer_);
    }

    //! Should be called when the socket is readable
    bool operator () (Socket& s)
    {
        int r = s.recv_one(const_cast<char*>(buffer_.data() + size_),
                           buffer_.size() - size_);

        if (r < 0)
            throw NetException("NetReadBuffer() error in recv", errno);

        size_ += r;

        if (size_ == buffer_.size()) {
            functional_(s, buffer_);
            return true;
        }
        else {
            return false;
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

    listenSocket_ = Socket::Create();
    listenSocket_.SetReuseAddr();

    const SocketAddress& lsa = addrlist[my_rank_];

    if (listenSocket_.bind(lsa) != 0)
        throw NetException("Could not bind listen socket to "
                           + lsa.ToStringHostPort(), errno);

    if (listenSocket_.listen() != 0)
        throw NetException("Could not listen on socket "
                           + lsa.ToStringHostPort(), errno);

    // TODO(tb): this sleep waits for other clients to open their ports. do this
    // differently.
    sleep(1);

    // number of clients from which we are expecting connections

    unsigned int accepting = my_rank_;

    // initiate connections to all hosts with higher id.

    std::vector<ClientId> connecting;

    unsigned int connected = 0;

    for (ClientId id = my_rank_ + 1; id < addrlist.size(); ++id)
    {
        connections_[id] = Socket::Create();

        Socket& socket = connections_[id].GetSocket();

        // initiate non-blocking TCP connection
        socket.SetNonBlocking(true);

        if (socket.connect(addrlist[id]) == 0) {
            // connect() already successful? this should not be.
            abort();
        }
        else if (errno == EINPROGRESS) {
            // connect is in progress, will wait for completion.
            connecting.push_back(id);
        }
        else {
            throw NetException("Could not connect to client "
                               + std::to_string(id) + " via "
                               + addrlist[id].ToStringHostPort(), errno);
        }
    }

    // Transfer Welcome message to other clients and receive from all other to
    // synchronize group.

    struct WelcomeMsg
    {
        uint32_t c7a;
        ClientId id;
    };

    static const uint32_t c7a_sign = 0x0C7A0C7A;
    static const WelcomeMsg my_welcome = { c7a_sign, my_rank_ };

    // construct list of welcome message read buffers, we really need a deque
    // here due to reallocation in vector

    auto MsgReaderLambda = [](Socket& s, const std::string& buffer) -> bool {
                               std::cout << "Message on " << s.GetFileDescriptor() << std::endl;
                           };

    typedef NetReadBuffer<decltype(MsgReaderLambda), sizeof(my_welcome)>
        WelcomeReadBuffer;

    std::deque<WelcomeReadBuffer> msg_reader;

    // Perform select loop waiting for incoming connections and fulfilled
    // outgoing connections.

    SelectDispatcher disp;

    // wait for incoming connections
    disp.HookRead(
        listenSocket_, [&](Socket& s) -> bool {
            // new accept()able connection on listen socket
            die_unless(accepting > 0);

            Socket ns = s.accept();
            LOG << "OpenConnections() accepted connection"
                << " from=" << ns.GetPeerAddress();

                                      // send welcome message - TODO(tb): this is a blocking send
            ns.send(&my_welcome, sizeof(my_welcome));

                                      // wait for welcome message from other side
            msg_reader.emplace_back(ns, sizeof(my_welcome), MsgReaderLambda);
            disp.HookRead(ns, msg_reader.back());

            return (--accepting > 0); // wait for more connection?
        });

    // wait for completed connect() calls
    for (ClientId& id : connecting)
    {
        Socket& socket = connections_[id].GetSocket();

        disp.HookWrite(
            socket, [id, &addrlist](Socket& s) -> bool {
                int err = s.GetError();

                if (err != 0) {
                    throw NetException(
                        "OpenConnections() could not connect to client "
                        + std::to_string(id) + " via "
                        + addrlist[id].ToStringHostPort(), err);
                }

                // send welcome message - TODO(tb): this is a blocking send
                s.SetNonBlocking(false);
                s.send(&my_welcome, sizeof(my_welcome));

                return false;
            });
    }

    std::vector<Socket*> read_set, write_set, except_set;

    while (accepting > 0 || connected != connecting.size())
    {
        disp.Dispatch(read_set, write_set, except_set);
    }
}

void NetGroup::ExecuteLocalMock(
    size_t num_clients,
    const std::function<void(NetGroup*)>& thread_function)
{
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

            group[i]->connections_[j] = NetConnection(sp.first);
            group[j]->connections_[i] = NetConnection(sp.second);
        }
    }

    // create a thread for each NetGroup object and run user program.
    std::vector<std::thread*> threads(num_clients);

    for (size_t i = 0; i != num_clients; ++i) {
        threads[i] = new std::thread(
            std::bind(thread_function, group[i].get()));
    }

    for (size_t i = 0; i != num_clients; ++i) {
        threads[i]->join();
        delete threads[i];
    }

    // close sockets allocated before
    for (size_t i = 0; i != num_clients; ++i) {
        group[i]->Close();
    }
}

} // namespace c7a

/******************************************************************************/
