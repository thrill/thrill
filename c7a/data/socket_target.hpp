/*******************************************************************************
 * c7a/data/socket_target.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_SOCKET_TARGET_HEADER
#define C7A_DATA_SOCKET_TARGET_HEADER

#include <c7a/net/buffer.hpp>
#include <c7a/net/stream.hpp>
#include <c7a/common/logger.hpp>
#include <c7a/data/emitter_target.hpp>

namespace c7a {
namespace data {

//! SocketTarget is an EmitterTarget that sends data via a network socket to a Channel
//! The SocketTarget appends data as long as internal queue (4KB) is sufficient or Flush is called.
class SocketTarget : public EmitterTarget
{
public:
    SocketTarget(net::DispatcherThread* dispatcher,
                 net::Connection* connection, size_t channel_id)
        : dispatcher_(dispatcher),
          connection_(connection),
          id_(channel_id),
          closed_(false) { }

    //! Appends data to the SocketTarget.
    //! Data may be sent but may be delayed.
    void Append(BinaryBufferBuilder& buffer) override {
        //virtual does not hurt because not in tight loop
        if (buffer.size() == 0) {
            return;
        }
        SendHeader(buffer.size(), buffer.elements());

        net::Buffer payload_buf = buffer.ToBuffer();
        buffer.Detach();
        dispatcher_->AsyncWrite(*connection_, std::move(payload_buf));
    }

    //! Closes the connection
    void Close() override {
        assert(!closed_);
        closed_ = true;
        SendHeader(0, 0);
    }

protected:
    static const bool debug = false;
    //need pointers because child class does not initialize them.
    //TODO(ts) do not use raw pointers
    net::DispatcherThread* dispatcher_;
    net::Connection* connection_;
    size_t id_;
    bool closed_;

    void SendHeader(size_t num_bytes, size_t elements) {
        net::StreamBlockHeader header;
        header.channel_id = id_;
        header.expected_bytes = num_bytes;
        header.expected_elements = elements;
        net::Buffer header_buffer(&header, sizeof(header));
        dispatcher_->AsyncWrite(*connection_, std::move(header_buffer));
    }
};

//! LoopbackTarget is like a SocketTarget but skips the network stack for a loopback connection
//! DataManager offers a vector of emitters to send data to every worker. One of the workers is
//! the worker itself, thus a network connection to lokalhost can be avoided by using this
//! loopback target.
class LoopbackTarget : public EmitterTarget
{
public:
    //! Calling this callback is equivalent to sending a end-of-stream StreamBlockHeader
    typedef std::function<void ()> StreamCloser;

    //! Creates an instance
    //! \param chain the BufferChain to attach the data to
    //! \param closeCallback is called when Close is called
    LoopbackTarget(std::shared_ptr<BufferChain> chain, StreamCloser closeCallback)
        : chain_(chain),
          closeCallback_(closeCallback),
          closed_(false) { }

    //! Appends data directly to the target BufferChain
    void Append(BinaryBufferBuilder& buffer) override {
        //virtual does not hurt because not in tight loop
        chain_->Append(buffer);
    }

    //! Closes the LoopbackTarget. Can be called once.
    void Close() override {
        assert(!closed_);
        closeCallback_();
        closed_ = true;
    }

private:
    std::shared_ptr<BufferChain> chain_;
    StreamCloser closeCallback_;
    static const bool debug = false;
    bool closed_;
};

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_SOCKET_TARGET_HEADER

/******************************************************************************/
