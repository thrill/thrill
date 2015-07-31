/*******************************************************************************
 * c7a/common/types.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_TYPES_HEADER
#define C7A_COMMON_TYPES_HEADER

namespace c7a {
namespace common {

//thanks to http://stackoverflow.com/a/7127988
template <typename T>
struct is_pair : public std::false_type { };
template <typename S, typename T>
struct is_pair<std::pair<S, T> >
    : public std::true_type { };
} //namespace common
} //namespace c7a

#endif // !C7A_COMMON_TYPES_HEADER

/******************************************************************************/
