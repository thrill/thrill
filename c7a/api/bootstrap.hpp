/*******************************************************************************
 * c7a/api/bootstrap.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_BOOTSTRAP_HEADER
#define C7A_API_BOOTSTRAP_HEADER

#include <c7a/api/context.hpp>

namespace c7a {

//! Executes the given job startpoint with a context instance
//! startpoint may be called multiple times with concurrent threads.
static int Execute(int argc, char* argv[], std::function<int(Context&)> job_startpoint)
{
    Context ctx;
    return job_startpoint(ctx);
}

} // namespace c7a

#endif // !C7A_API_BOOTSTRAP_HEADER

/******************************************************************************/
