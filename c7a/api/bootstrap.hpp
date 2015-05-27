/*******************************************************************************
 * c7a/api/bootstrap.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_BOOTSTRAP_HEADER
#define C7A_API_BOOTSTRAP_HEADER

#include <tuple>

#include <c7a/api/context.hpp>

namespace c7a {
namespace bootstrap {

std::tuple<int, size_t, std::vector<std::string> > ParseArgs(int argc, char* argv[]) {
    //replace with arbitrary compex implementation
    size_t my_rank;
    std::vector<std::string> endpoints;
    if (argc > 2) {
        my_rank = atoi(argv[1]);
        endpoints.assign(argv + 2, argv + argc);
    }
    else if (argc == 2) {
        std::cerr << "Wrong number of arguments. Must be 0 or > 1";
        return std::make_tuple(-1, my_rank, endpoints);
    }
    else {
        my_rank = 0;
        endpoints.push_back("127.0.0.1:1234");
    }
    return std::make_tuple(0, my_rank, endpoints);
}
}   //namespace bootstrap

//! Executes the given job startpoint with a context instance.
//! Startpoint may be called multiple times with concurrent threads and
//! different context instances.
//!
//! \returns result of word_count if bootstrapping was successfull, -1 otherwise.
static int Execute(int argc, char* argv[], std::function<int(Context&)> job_startpoint) {
    size_t my_rank;
    std::vector<std::string> endpoints;
    int result;
    std::tie(result, my_rank, endpoints) = bootstrap::ParseArgs(argc, argv);
    if (result != 0)
        return -1;

    if (my_rank >= endpoints.size()) {
        std::cerr << "endpoint list (" <<
            endpoints.size() <<
            " entries) does not include my rank (" <<
            my_rank << ")" << std::endl;
        return -1;
    }

    std::cout << "executing " << argv[0] << " with rank " << my_rank << " and endpoints ";
    for (const auto& ep : endpoints)
        std::cout << ep << " ";
    std::cout << std::endl;

    Context ctx;
    std::cout << "connecting to peers" << std::endl;
    ctx.job_manager().Connect(my_rank, net::NetEndpoint::ParseEndpointList(endpoints));
    ctx.job_manager().StartDispatcher();
    std::cout << "starting job" << std::endl;
    auto job_result = job_startpoint(ctx);
    ctx.job_manager().StopDispatcher();
    return job_result;
}

} // namespace bootstrap

#endif // !C7A_API_BOOTSTRAP_HEADER

/******************************************************************************/
