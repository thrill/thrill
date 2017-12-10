/*******************************************************************************
 * thrill/net/tcp/construct.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
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
void Construct(SelectDispatcher& dispatcher, size_t my_rank,
               const std::vector<std::string>& endpoints,
               std::unique_ptr<Group>* groups, size_t group_count);

//! Connect to peers via endpoints using TCP sockets. Construct a group_count
//! net::Group objects at once. Within each Group this host has my_rank.
std::vector<std::unique_ptr<net::Group> >
Construct(SelectDispatcher& dispatcher, size_t my_rank,
          const std::vector<std::string>& endpoints, size_t group_count);

//! \}

} // namespace tcp
} // namespace net
} // namespace thrill

#endif // !THRILL_NET_TCP_CONSTRUCT_HEADER

/******************************************************************************/
