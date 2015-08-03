/*******************************************************************************
 * examples/word_count.cpp
 *
 * Runner program for WordCount example. See c7a/examples/word_count.hpp for the
 * source to the example.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/common/cmdline_parser.hpp>
#include <c7a/common/logger.hpp>
#include <c7a/examples/word_count.hpp>

using namespace c7a;

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    unsigned int elements = 1000;
    clp.AddUInt('s', "elements", "S", elements,
                "Create wordcount example with S generated words");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    std::function<int(api::Context&)> start_func =
        [elements](api::Context& ctx) {
            size_t uniques = examples::WordCountGenerated(ctx, elements);
            sLOG1 << "wrote counts of" << uniques << "unique words";
            return 0;
        };

    return api::ExecuteEnv(start_func);
}

/******************************************************************************/
