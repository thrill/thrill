/*******************************************************************************
 * examples/word_count.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/api/bootstrap.hpp>
#include <c7a/api/dia.hpp>
#include "word_count_user_program.cpp"

int main(int argc, char* argv[]) {
    return c7a::Execute(argc, argv, word_count);
}

/******************************************************************************/
