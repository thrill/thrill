/*******************************************************************************
 * examples/word_count.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/api/bootstrap.hpp>
#include <c7a/api/dia.hpp>
#include "word_count_user_program.cpp"

int main(int argc, char* argv[]) {
  
  size_t elements = 1048576;
    std::function<int(c7a::Context&)> start_func = [elements](c7a::Context& ctx) {
        return word_count_generated(ctx, elements);
    };

    return c7a::Execute(argc, argv, word_count_generated);
}

/******************************************************************************/
