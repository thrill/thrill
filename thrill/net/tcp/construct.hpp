/*******************************************************************************
 * thrill/net/tcp/construct.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_TCP_CONSTRUCT_HEADER
#define THRILL_NET_TCP_CONSTRUCT_HEADER

#include <thrill/net/tcp/group.hpp>

#include <memory>
#include <string>
#include <vector>

namespace thrill {
namespace net {
namespace tcp {

//! \addtogroup net_tcp TCP Socket API
//! \{

//! Connect to peers via endpoints using TCP sockets. Construct a group_count
//! tcp::Group objects at once. Within each Group this host has my_rank.
void Construct(size_t my_rank,
               const std::vector<std::string>& endpoints,
               std::unique_ptr<Group>* groups, size_t group_count);

//! \}

} // namespace tcp
} // namespace net
} // namespace thrill

#endif // !THRILL_NET_TCP_CONSTRUCT_HEADER

/******************************************************************************/
