/*******************************************************************************
 * benchmarks/groupby/groupby.cpp
 *
 * Part of Project Thrill.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/api/size.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/api/groupby_index.hpp>
#include <thrill/api/groupby.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/read_binary.hpp>

#include <iostream>
#include <iterator>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

using thrill::DIARef;
using thrill::Context;

using namespace thrill; // NOLINT

int main(int argc, char* argv[]) {
    common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    int n;
    clp.AddParamInt("n", n, "Iterations");

    std::string input;
    clp.AddParamString("input", input,
                       "input file pattern");

    int m;
    clp.AddParamInt("m", m,
                       "modulo");

    int host;
    clp.AddParamInt("h", host,
                       "numer of hosts");

    int worker;
    clp.AddParamInt("w", worker,
                       "number of workers");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    auto start_func = [n, m, &input, host, worker](api::Context& ctx) {
        thrill::common::StatsTimer<true> timer(false);

        auto modulo_keyfn = [m](size_t in) { return (in % m); };

        auto median_fn = [](auto& r, std::size_t) {
            std::vector<std::size_t> all;
            while (r.HasNext()) {
                all.push_back(r.Next());
            }
            std::sort(std::begin(all), std::end(all));
            return all[all.size() / 2 - 1];
        };

        std::size_t elem = 0;
        // group by to compute median
        timer.Start();
        for (int i = 0; i < n; i++) {
            api::ReadBinary<size_t>(ctx, input).GroupBy<size_t>(modulo_keyfn, median_fn).Size();
        }
        timer.Stop();

        LOG1 << "\n"
             << "RESULT"
             << " name=total"
             << " rank=" << ctx.my_rank()
             << " time=" << (double)timer.Milliseconds()/(double)n
             << " filename=" << input
             << " num_hosts=" << host
             << " num_worker=" << worker;

    };

    return api::Run(start_func);
}

/******************************************************************************/
