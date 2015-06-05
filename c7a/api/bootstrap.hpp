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

#include <tuple>

#include <c7a/api/context.hpp>
#include <c7a/common/stats_timer.hpp>
#include <c7a/common/cmdline_parser.hpp>

namespace c7a {
namespace bootstrap {

std::tuple<int, size_t, std::vector<std::string> > ParseArgs(int argc, char* argv[]) {
    //replace with arbitrary compex implementation
    size_t my_rank;
    std::vector<std::string> endpoints;
    c7a::common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    unsigned int rank = 1;
    clp.AddUInt('r', "rank", "R", rank,
                "Rank of this worker");

    std::vector<std::string> addr;
    clp.AddParamStringlist("addresses", addr,
                "List of all worker addresses.");

    if (!clp.Process(argc, argv)) {
        return std::make_tuple(-1, my_rank, endpoints);;
    }

    for (auto address : addr) {
        if (address.find(":") == std::string::npos) {
	    std::cerr << "Invalid address. No Portnumber detecable";
	    return std::make_tuple(-1, my_rank, endpoints);
        }
    }

    if (argc > 2) {
        my_rank = rank;
        endpoints.assign(addr.begin(), addr.end());
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

    //!True if program time should be taken and printed

    static const bool debug = false;

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

    LOG << "executing " << argv[0] << " with rank " << my_rank << " and endpoints";
    for (const auto& ep : endpoints)
       LOG << ep << " ";

    Context ctx;
    LOG << "connecting to peers";
    ctx.job_manager().Connect(my_rank, net::Endpoint::ParseEndpointList(endpoints));
    std::cout << "starting job" << std::endl;
    auto overall_timer = ctx.get_stats().CreateTimer("job::overall", "", true);
    auto job_result = job_startpoint(ctx);
    overall_timer->Stop();
    std::cout << "DONE! " << ctx.rank() << std::endl;
    return job_result;
}

} // namespace bootstrap

#endif // !C7A_API_BOOTSTRAP_HEADER

/******************************************************************************/
