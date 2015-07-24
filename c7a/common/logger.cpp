/*******************************************************************************
 * c7a/common/logger.cpp
 *
 * Simple and less simple logging classes.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/common/logger.hpp>

namespace c7a {
namespace common {

std::mutex Logger<true>::mutex_;

//! access global singleton
ThreadNameDirectory & GetThreadDirectory() {
    static ThreadNameDirectory directory;
    return directory;
}

} // namespace common
} // namespace c7a

/******************************************************************************/
