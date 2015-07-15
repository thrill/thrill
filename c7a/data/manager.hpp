/*******************************************************************************
 * c7a/data/manager.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_MANAGER_HEADER
#define C7A_DATA_MANAGER_HEADER

#include <c7a/api/input_line_iterator.hpp>
#include <c7a/common/logger.hpp>
#include <c7a/data/buffer_chain.hpp>
#include <c7a/data/buffer_chain_manager.hpp>
#include <c7a/data/emitter.hpp>
#include <c7a/data/file.hpp>
#include <c7a/data/iterator.hpp>
#include <c7a/data/output_line_emitter.hpp>
#include <c7a/data/socket_target.hpp>
#include <c7a/data/channel.hpp>
#include <c7a/data/channel_multiplexer.hpp>

#include <functional>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>

namespace c7a {
namespace data {

struct BufferChain;

//! Identification for DIAs
typedef ChainId DIAId;

//! Manages all kind of memory for data elements
//!
//!
//! Provides Channel creation for sending / receiving data from other workers.
class Manager
{
public:
    Manager(net::DispatcherThread& dispatcher)
        : cmp_(dispatcher) { }

    //! non-copyable: delete copy-constructor
    Manager(const Manager&) = delete;
    //! non-copyable: delete assignment operator
    Manager& operator = (const Manager&) = delete;

    //! Connect net::Group. Forwarded To ChannelMultiplexer.
    void Connect(net::Group* group) {
        cmp_.Connect(group);
    }

    //! Closes all client connections. Forwarded To ChannelMultiplexer.
    void Close() {
        cmp_.Close();
    }

    //! returns iterator on requested partition or network channel.
    //!
    //! Data can be emitted into this partition / received on the channel even
    //! after the iterator was created.
    //!
    //! \param id ID of the DIA / Channel - determined by AllocateDIA() / AllocateNetworkChannel()
    template <class T>
    Iterator<T> GetIterator(const ChainId& id) {
        return Iterator<T>(*GetChainOrDie(id));
    }

    //! Returns the number of elements that are stored on this worker
    //! Returns -1 if the channel or dia was not closed (yet)
    //! This call is blocking until the DIA is closed
    //! throws if id is unknown
    size_t GetNumElements(const ChainId& id) {
        std::shared_ptr<BufferChain> chain = GetChainOrDie(id);
        if (!chain->IsClosed())
            chain->WaitUntilClosed();
        return chain->size();
    }

#if FIXUP_LATER
    //! Docu see net::ChannelMultiplexer::Scatter()
    template <class T>
    void Scatter(const ChainId& source, const ChainId& target, std::vector<size_t> offsets) {
        assert(source.type == LOCAL);
        assert(target.type == NETWORK);
        assert(dias_.Contains(source));
        cmp_.Scatter<T>(dias_.Chain(source), target, offsets);
    }
#endif      // FIXUP_LATER

    //! Returns a number that uniquely addresses a DIA
    //! Calls to this method alter the data managers state.
    //! Calls to this method must be in deterministic order for all workers!
    DIAId AllocateDIA() {
        return dias_.AllocateNext();
    }

    //! Returns a number that uniquely addresses a network channel
    //! Calls to this method alter the data managers state.
    //! Calls to this method must be in deterministic order for all workers!
    //! \param order_preserving indicates if the channel should preserve the order of the receiving packages
    ChannelId AllocateChannelId() {
        return cmp_.AllocateNext();
    }

    //! Returns a reference to an existing Channel.
    std::shared_ptr<net::Channel> GetChannel(const ChannelId id) {
        assert(cmp_.HasChannel(id));
        return std::move(cmp_.GetOrCreateChannel(id));
    }

    //! Returns a reference to a new Channel.
    std::shared_ptr<net::Channel> GetNewChannel() {
        return std::move(cmp_.GetOrCreateChannel(AllocateChannelId()));
    }

    //! Returns an emitter that can be used to fill a DIA
    //! Emitters can push data into DIAs even if an intertor was created before.
    //! Data is only visible to the iterator if the emitter was flushed.
    // template <class T>
    // Emitter<T> GetLocalEmitter(DIAId id) {
    //     assert(id.type == LOCAL);
    //     if (!dias_.Contains(id)) {
    //         throw std::runtime_error("target dia id unknown.");
    //     }
    //     return Emitter<T>(dias_.Chain(id));
    // }

    //! Returns a new File object containing a sequence of local Blocks.
    File GetFile() {
        return File();
    }

    template <class T>
    std::vector<Emitter> GetNetworkEmitters(ChannelId id) {
        assert(id.type == NETWORK);
        if (!cmp_.HasDataOn(id)) {
            throw std::runtime_error("target channel id unknown.");
        }
        return cmp_.OpenChannel(id);
    }

private:
    static const bool debug = false;
    net::ChannelMultiplexer cmp_;

    BufferChainManager dias_;

    std::shared_ptr<BufferChain> GetChainOrDie(const ChainId& id) {
        if (id.type == LOCAL) {
            if (!dias_.Contains(id)) {
                throw std::runtime_error("target dia id unknown.");
            }
            return dias_.Chain(id);
        }
        else {
            if (!cmp_.HasDataOn(id)) {
                throw std::runtime_error("target channel id unknown.");
            }
            return cmp_.AccessData(id);
        }
    }
};

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_MANAGER_HEADER

/******************************************************************************/
