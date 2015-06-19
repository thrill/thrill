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

#include <c7a/data/iterator.hpp>
#include <c7a/api/input_line_iterator.hpp>
#include <c7a/data/emitter.hpp>
#include <c7a/data/buffer_chain.hpp>
#include <c7a/data/output_line_emitter.hpp>

#include <map>
#include <functional>
#include <string>
#include <stdexcept>
#include <memory> //unique_ptr

#include <c7a/net/channel_multiplexer.hpp>
#include <c7a/data/socket_target.hpp>
#include <c7a/data/buffer_chain_manager.hpp>
#include <c7a/common/logger.hpp>

namespace c7a {
namespace data {

struct BufferChain;

//! Identification for DIAs
typedef ChainId DIAId;
using c7a::net::ChannelId;
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
        if (id.type == LOCAL) {
            if (!dias_.Contains(id)) {
                throw std::runtime_error("target dia id unknown.");
            }
            return Iterator<T>(*(dias_.Chain(id)));
        }
        else {
            if (!cmp_.HasDataOn(id)) {
                throw std::runtime_error("target channel id unknown.");
            }

            return Iterator<T>(*(cmp_.AccessData(id)));
        }
    }

    //! Returns a number that uniquely addresses a DIA
    //! Calls to this method alter the data managers state.
    //! Calls to this method must be in deterministic order for all workers!
    DIAId AllocateDIA() {
        return dias_.AllocateNext();
    }

    // TODO Get size of current partition
    size_t get_current_size(const ChainId & /* id */) {
        return 0;
    }

    //! Returns a number that uniquely addresses a network channel
    //! Calls to this method alter the data managers state.
    //! Calls to this method must be in deterministic order for all workers!
    ChannelId AllocateNetworkChannel() {
        return cmp_.AllocateNext();
    }

    //! Returns an emitter that can be used to fill a DIA
    //! Emitters can push data into DIAs even if an intertor was created before.
    //! Data is only visible to the iterator if the emitter was flushed.
    template <class T>
    Emitter<T> GetLocalEmitter(DIAId id) {
        assert(id.type == LOCAL);
        if (!dias_.Contains(id)) {
            throw std::runtime_error("target dia id unknown.");
        }
        return Emitter<T>(dias_.Chain(id));
    }

    template <class T>
    std::vector<Emitter<T> > GetNetworkEmitters(ChannelId id) {
        assert(id.type == NETWORK);
        if (!cmp_.HasDataOn(id)) {
            throw std::runtime_error("target channel id unknown.");
        }
        return cmp_.OpenChannel<T>(id);
    }

    //! Returns an OutputLineIterator with a given output file stream.
    template <typename T>
    OutputLineEmitter<T> GetOutputLineEmitter(std::ofstream& file) {
        return OutputLineEmitter<T>(file);
    }

private:
    static const bool debug = false;
    net::ChannelMultiplexer cmp_;

    BufferChainManager dias_;
};

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_MANAGER_HEADER

/******************************************************************************/
