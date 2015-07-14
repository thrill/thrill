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

#include <c7a/common/logger.hpp>
#include <c7a/data/file.hpp>
#include <c7a/data/iterator.hpp>
#include <c7a/data/output_line_emitter.hpp>
#include <c7a/data/socket_target.hpp>
#include <c7a/data/channel.hpp>
#include <c7a/data/channel_multiplexer.hpp>
#include <c7a/data/repository.hpp>

#include <functional>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>

namespace c7a {
namespace net {
class Group;
}
namespace data {

using FilePtr = std::shared_ptr<File>;

//! Identification for DIAs
using FilePtr = std::shared_ptr<File>
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

          //! Closes all client connections, files and everything.
          void Close() {
              for (auto& f : files) {
                  f.second.Close();
              }
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

          BlockReader<BlockSize> GetBlockReader(const DataId& id) {
              return BlockReader<BlockSize>(GetFileOrDie(id));
          }

#if FIXUP_LATER
          //! Docu see net::ChannelMultiplexer::Scatter()
          template <class T>
          void Scatter(const DataId& source, const DataId& target, std::vector<size_t> offsets) {
              assert(source.type == LOCAL);
              assert(target.type == NETWORK);
              assert(files_.Contains(source));
              cmp_.Scatter<T>(files_(source), target, offsets);
          }
#endif      // FIXUP_LATER

          //! Returns a number that uniquely addresses a File
          //! Calls to this method alter the data managers state.
          //! Calls to this method must be in deterministic order for all workers!
          DataId AllocateFileId() {
              return files_.AllocateNext();
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

          //! Returns a new, anonymous File object containing a sequence of local Blocks.
          FilePtr GetFile() {
              return File();
          }

          //! Returns a named file that has been allocated via \ref AllocateFileId()
          FilePtr GetFile(const DataId& id) {
              return data_(id);
          }

          template <class T>
          std::vector<BlockWriter<T> > GetNetworkEmitters(DataId id) {
              assert(id.type == NETWORK);
              if (!cmp_.HasDataOn(id)) {
                  throw std::runtime_error("target channel id unknown.");
              }
              return cmp_.OpenChannel(id);
          }

      private:
          static const bool debug = false;
          net::ChannelMultiplexer cmp_;
          DataRepository<File> data_;
      };
} // namespace data
} // namespace c7a

#endif // !C7A_DATA_MANAGER_HEADER

/******************************************************************************/
