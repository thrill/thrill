/*******************************************************************************
 * examples/word_count.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/api/bootstrap.hpp>

int word_count(c7a::Context& context);

int main(int argc, char* argv[]) {
    return c7a::Execute(argc, argv, word_count);
}

//! The WordCount user program
int word_count(c7a::Context& context) {
    return 0;
}

/******************************************************************************/
