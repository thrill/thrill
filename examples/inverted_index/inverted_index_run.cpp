/*******************************************************************************
 * examples/inverted_index/inverted_index.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2018 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/inverted_index/inverted_index.hpp>
#include <tlx/cmdline_parser.hpp>

using namespace thrill;                   // NOLINT
using namespace examples::inverted_index; // NOLINT

int main(int argc, char* argv[]) {

    tlx::CmdlineParser clp;

    std::string output_path;
    clp.add_string('o', "output", output_path,
                   "output file pattern");

    size_t num_docs = 10;
    clp.add_size_t('d', "num_docs", num_docs, "Number of documents, default: 10");

    size_t num_words = 10;
    clp.add_size_t('w', "num_words", num_words, "Number of words per document, default: 10");

    if (!clp.process(argc, argv)) {
        return -1;
    }

    clp.print_result();

    return thrill::Run(
        [&](thrill::Context& ctx) { 
            InvertedIndex(ctx, output_path, num_docs, num_words); 
    });
}

/******************************************************************************/
