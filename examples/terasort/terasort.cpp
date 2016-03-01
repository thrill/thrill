/*******************************************************************************
 * examples/terasort/terasort.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/read_binary.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/write_binary.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

using namespace thrill;               // NOLINT

struct Record {
    uint8_t key[10];
    uint8_t value[90];

    bool operator < (const Record& b) const {
        return std::lexicographical_compare(key, key + 10, b.key, b.key + 10);
    }
} THRILL_ATTRIBUTE_PACKED;

struct RecordSigned {
    char key[10];
    char value[90];

    // this sorted by _signed_ characters, which is the same as what some
    // Java/Scala TeraSorts do.
    bool operator < (const RecordSigned& b) const {
        return std::lexicographical_compare(key, key + 10, b.key, b.key + 10);
    }
} THRILL_ATTRIBUTE_PACKED;

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    bool use_signed_char = false;
    clp.AddFlag('s', "signed_char", use_signed_char,
                "compare with signed chars to compare with broken Java "
                "implementations, default: false");

    std::string output;
    clp.AddString('o', "output", output,
                  "output file pattern");

    std::vector<std::string> input;
    clp.AddParamStringlist("input", input,
                           "input file pattern(s)");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    return api::Run(
        [&](api::Context& ctx) {
            ctx.enable_consume();
            if (use_signed_char)
                ReadBinary<RecordSigned>(ctx, input).Sort().WriteBinary(output);
            else
                ReadBinary<Record>(ctx, input).Sort().WriteBinary(output);
        });
}

/******************************************************************************/
