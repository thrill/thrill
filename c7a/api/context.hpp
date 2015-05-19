/*******************************************************************************
 * c7a/api/context.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_CONTEXT_HEADER
#define C7A_API_CONTEXT_HEADER

#include <cassert>
#include <fstream>
#include <string>
#include <vector>

#include "../data/data_manager.hpp"

namespace c7a {
class Context {
public:
    Context() : net_dispatcher_(), cmp_(net_dispatcher_), data_manager_(cmp_) { }
    virtual ~Context() { }

    data::DataManager & get_data_manager() {
        return data_manager_;
    }
    int number_worker()
    {
        return number_worker_;
    }

private:
    net::NetDispatcher net_dispatcher_;
    net::ChannelMultiplexer cmp_;
    data::DataManager data_manager_;
    //stub
    int number_worker_ = 1;
};
} // namespace c7a

#endif // !C7A_API_CONTEXT_HEADER

/******************************************************************************/
