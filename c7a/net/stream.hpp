/*******************************************************************************
 * c7a/net/stream.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_STREAM_HEADER
#define C7A_NET_STREAM_HEADER

#include <stdlib.h> //free
#include <stdio.h>  //mempcy

#include <c7a/net/socket.hpp>

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
    size_t      num_elements;
    size_t      * boundaries = nullptr;

    //! Reads the channel id and the number of elements in this block
    void        ParseIdAndNumElem(const std::string& buffer)
    {
        memcpy(&channel_id, buffer.c_str(), sizeof(channel_id));
        memcpy(&num_elements, buffer.c_str() + sizeof(channel_id), sizeof(num_elements));
        boundaries = new size_t[sizeof(size_t) * num_elements];
    }

    //! Reads the lengths of each element in this block
    void        ParseBoundaries(const std::string& buffer)
    {
        if (num_elements > 0) {
            memcpy(boundaries, buffer.c_str(), sizeof(size_t) * num_elements);
        }
    }

    //! Serializes the whole block struct into a buffer
    std::string Serialize()
    {
        size_t size = sizeof(size_t) * (num_elements + 2);
        char* result = new char[size];
        char* offset0 = result;
        char* offset1 = offset0 + sizeof(channel_id);
        char* offset2 = offset1 + sizeof(num_elements);

        memcpy(offset0, &channel_id, sizeof(channel_id));
        memcpy(offset1, &num_elements, sizeof(num_elements));
        if (boundaries)
            memcpy(offset2, boundaries, sizeof(*boundaries) * num_elements);
        return std::string(result, size);
    }

    //! resets to a End-of-Stream block header
    void        Reset()
    {
        num_elements = 0;
        //TODO delete boundaries w/o double-freeing
    }

    //! Indicates if this is the end-of-stream block header
    bool        IsStreamEnd() const
    {
        return num_elements == 0;
    }

    //! Frees all memory of the block struct
    ~StreamBlockHeader()
    {
        Reset();
        //if (boundaries)
        //delete [] boundaries;
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
struct Stream {
    struct StreamBlockHeader header;
    Socket&                  socket;
    int                      elements_read = 0;

    //!attaches a stream to a socket and initializes the current header
    Stream(Socket& socket, struct StreamBlockHeader header)
        : header(header),
          socket(socket) { }

    //! replaces the current head with the end-of-stream header
    void                     ResetHead()
    {
        elements_read = 0;
        header.Reset();
    }

    //! indicates if all data of this stream has arrived
    bool                     IsFinished() const
    {
        return header.IsStreamEnd();
    }
};

//! \}

} // namespace net

} // namespace c7a

#endif // !C7A_NET_STREAM_HEADER

/******************************************************************************/
