/*******************************************************************************
 * c7a/net/channel-multiplexer.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_CHANNEL_MULTIPLEXER_HEADER
#define C7A_NET_CHANNEL_MULTIPLEXER_HEADER

#include <memory> //std::shared_ptr
#include <c7a/net/net-dispatcher.hpp>
#include <c7a/net/channel.hpp>

namespace c7a {

namespace net {

//! \ingroup net
//! \{

//! Multiplexes virtual Connections on NetDispatcher
//!
//! A worker as a TCP conneciton to each other worker to exchange large amounts
//! of data. Since multiple exchanges can occur at the same time on this single
//! connection we use multiplexing. The slices are called Blocks and are
//! indicated by a \ref StreamBlockHeader. Multiple Blocks form a Stream on a
//! single TCP connection. The multi plexer multiplexes all streams on all
//! sockets.
//!
//! All sockets are polled for headers. As soon as the a header arrives it is
//! either attached to an existing channel or a new channel instance is
//! created.
class ChannelMultiplexer
{
public:
    ChannelMultiplexer(NetDispatcher& dispatcher, int num_connections);

    //! Adds a connected TCP socket to another worker
    //! There must exist exactly one TCP connection to each worker.
    void AddSocket(Socket& s);

    //! Indicates if a channel exists with the given id
    bool HasChannel(int id);

    //! Returns the channel with the given ID or an onvalid pointer
    //! if the channel does not exist
    std::shared_ptr<Channel> PickupChannel(int id);

private:
    static const bool debug = true;
    typedef std::shared_ptr<Channel> ChannelPtr;

    //! Channels have an ID in block headers
    std::map<int, ChannelPtr> channels_;

    NetDispatcher& dispatcher_;
    int num_connections_;

    //! expects the next header from a socket
    void ExpectHeaderFrom(Socket& s);

    //! parses the channel id from a header and passes it to an existing
    //! channel or creates a new channel
    void ReadFirstHeaderPartFrom(Socket& s, const std::string& buffer);
};

} // namespace net

} // namespace c7a

#endif // !C7A_NET_CHANNEL_MULTIPLEXER_HEADER

/******************************************************************************/
