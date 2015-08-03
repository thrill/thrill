/*******************************************************************************
 * examples/word_count.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/api/context.hpp>
#include <c7a/api/dia.hpp>
#include <c7a/examples/word_count.hpp>

using namespace c7a;

int main(int argc, char* argv[]) {

    size_t elements = pow(2, 10);

    std::function<int(api::Context&)> start_func =
        [elements](api::Context& ctx) {
            return examples::WordCountGenerated(ctx, elements);
        };

    return api::ExecuteEnv(start_func);
}

/******************************************************************************/
