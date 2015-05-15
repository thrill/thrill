/*******************************************************************************
 * c7a/net/stream.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_STREAM_HEADER
#define C7A_NET_STREAM_HEADER

#include <stdlib.h> //free
#include <stdio.h>  //mempcy

#include <c7a/net/net_connection.hpp>

namespace c7a {
namespace net {
//! \ingroup net
//! \{

//! Block header is sent before a sequence of blocks
//! it indicates the number of elements and their
//! boundaries
//!
//! Provides a serializer and two partial deserializers
//! A StreamBlockHeader with num_elements = 0 marks the end of a stream
struct StreamBlockHeader {
    size_t      channel_id;
    size_t      expected_bytes;

    //! Reads the channel id and the number of elements in this block
    void        ParseHeader(const std::string& buffer)
    {
        memcpy(&channel_id, buffer.c_str(), sizeof(channel_id));
        memcpy(&expected_bytes, buffer.c_str() + sizeof(channel_id), sizeof(expected_bytes));
    }

    //! Serializes the whole block struct into a buffer
    std::string Serialize()
    {
        size_t size = sizeof(size_t) * (channel_id + 2);
        char* result = new char[size];
        char* offset0 = result;
        char* offset1 = offset0 + sizeof(channel_id);

        memcpy(offset0, &channel_id, sizeof(channel_id));
        memcpy(offset1, &expected_bytes, sizeof(expected_bytes));
        return std::string(result, size);
    }

    //! resets to a End-of-Stream block header
    void        Reset()
    {
        expected_bytes = 0;
    }

    //! Indicates if this is the end-of-stream block header
    bool        IsStreamEnd() const
    {
        return expected_bytes == 0;
    }

    //! Frees all memory of the block struct
    ~StreamBlockHeader()
    {
        Reset();
    }
};

//! A stream is one connection from one worker to another and contains of
//! 0 or more blocks.
//!
//! A stream is attached to a socket and has a current block header that can
//! be the end-of-stream header
//!
//! If a client does not want to send any data to the receiver, only a end-of-
//! stream header must be sent, since TCP connections are re-used for multiple
//! streams
class Stream
{
public:
    struct StreamBlockHeader header;
    NetConnection& socket;
    size_t elements_read = 0;
    size_t bytes_read = 0;

    //!attaches a stream to a socket and initializes the current header
    Stream(NetConnection& socket, struct StreamBlockHeader& header)
        : header(header),
          socket(socket) { }

    //! replaces the current head with the end-of-stream header
    void ResetHead()
    {
        elements_read = 0;
        bytes_read = 0;
        header.Reset();
    }

    //! indicates if all data of this stream has arrived
    bool IsFinished() const
    {
        return header.IsStreamEnd();
    }
};

//! \}
} // namespace net
} // namespace c7a

#endif // !C7A_NET_STREAM_HEADER

/******************************************************************************/
