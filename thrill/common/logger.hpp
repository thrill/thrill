/*******************************************************************************
 * thrill/common/logger.hpp
 *
 * Simple and less simple logging classes.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_LOGGER_HEADER
#define THRILL_COMMON_LOGGER_HEADER

#include <thrill/mem/allocator.hpp>
#include <thrill/mem/pool.hpp>
#include <tlx/logger.hpp>
#include <tlx/logger/array.hpp>
#include <tlx/logger/tuple.hpp>
#include <tlx/meta/call_foreach_tuple.hpp>

namespace thrill {
namespace common {

//! memory manager singleton for Logger
extern mem::Manager g_logger_mem_manager;

//! Defines a name for the current thread.
void NameThisThread(const std::string& name);

//! Returns the name of the current thread or 'unknown [id]'
std::string GetNameForThisThread();

/******************************************************************************/

/*!

\brief LOG and sLOG for development and debugging

This is a short description of how to use \ref LOG and \ref sLOG for rapid
development of modules with debug output, and how to **keep it afterwards**.

There are two classes Logger and SpacingLogger, but one does not use these
directly.

Instead there are the macros: \ref LOG and \ref sLOG that can be used as such:
\code
LOG << "This will be printed with a newline";
sLOG << "Print variables a" << a << "b" << b << "c" << c;
\endcode

There macros only print the lines if the boolean variable **debug** is
true. This variable is searched for in the scope of the LOG, which means it can
be set or overridden in the function scope, the class scope, from **inherited
classes**, or even the global scope.

\code
class MyClass
{
    static constexpr bool debug = true;

    void func1()
    {
        LOG << "Hello World";

        LOG0 << "This is temporarily disabled.";
    }

    void func2()
    {
        static constexpr bool debug = false;
        LOG << "This is not printed any more.";

        LOG1 << "But this is forced.";
    }
};
\endcode

There are two variation of \ref LOG and \ref sLOG : append 0 or 1 for
temporarily disabled or enabled debug lines. These macros are then \ref LOG0,
\ref LOG1, \ref sLOG0, and \ref sLOG1. The suffix overrides the debug variable's
setting.

After a module works as intended, one can just set `debug = false`, and all
debug output will disappear.

## Critique of LOG and sLOG

The macros are only for rapid module-based development. It cannot be used as an
extended logging system for our network framework, where logs of network
execution and communication are collected for later analysis. Something else is
needed here.

 */

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_LOGGER_HEADER

/******************************************************************************/
