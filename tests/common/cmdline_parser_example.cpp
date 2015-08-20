/*******************************************************************************
 * tests/common/cmdline_parser_example.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

// [example]
#include <iostream>
#include <string>
#include <thrill/common/cmdline_parser.hpp>

int main(int argc, char* argv[]) {
    c7a::common::CmdlineParser cp;

    // add description and author
    cp.SetDescription("This may some day be a useful program, which solves "
                      "many serious problems of the real world and achives "
                      "global peace.");
    cp.SetAuthor("Timo Bingmann <tb@panthema.net>");

    // add an unsigned integer option --rounds <N>
    unsigned int rounds = 0;
    cp.AddUInt('r', "rounds", "N", rounds,
               "Run N rounds of the experiment.");

    // add a byte size argument which the user can enter like '1gi'
    uint64_t a_size = 0;
    cp.AddBytes('s', "size", a_size,
                "Number of bytes to process.");

    // add a required parameter
    std::string a_filename;
    cp.AddParamString("filename", a_filename,
                      "A filename to process");

    // process command line
    if (!cp.Process(argc, argv))
        return -1; // some error occurred and help was always written to user.

    std::cout << "Command line parsed okay." << std::endl;

    // output for debugging
    cp.PrintResult();

    // do something useful
}
// [example]

/******************************************************************************/
