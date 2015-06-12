/*******************************************************************************
 * examples/wordcount/word_count_user_program.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/api/dia.hpp>
#include <c7a/common/string.hpp>



template <typename InStack>
auto word_count_user(c7a::DIARef<std::string, InStack> & input) {
   
    using WordPair = std::pair<std::string, int>;

    auto line_to_wordpairs =
        [](std::string line, auto emit) {
        for (const std::string& word : c7a::split(line, ' ')) {
            if (word.size() != 0)
                emit(WordPair(word, 1));
        }
    };

    auto key = [](WordPair in) {
                   return in.first;
               };

    auto red_fn =
        [](WordPair in1, WordPair in2) {
        return WordPair(in1.first, in1.second + in2.second);
    }; 
   
    auto word_pairs = input.FlatMap(line_to_wordpairs);

    return word_pairs.ReduceBy(key).With(red_fn);
}

//! The WordCount user program
int word_count(c7a::Context& ctx) {
    using c7a::Context;
    using WordPair = std::pair<std::string, int>;
   
    std::cout << "wordcount.in" << std::endl;
    auto lines = ReadLines(
        ctx,
        "wordcount.in",
        [](const std::string& line) {
            return line;
        });

    auto red_words = word_count_user(lines);

    red_words.WriteToFileSystem(
        "wordcount_" + std::to_string(ctx.rank()) + ".out",
        [](const WordPair& item) {
            return item.first + ": " + std::to_string(item.second);
        });

    return 0;
}

int word_count_generated(c7a::Context& ctx, size_t size) {
    using c7a::Context;
    using WordPair = std::pair<std::string, int>;

    auto lines = GenerateFromFile(
        ctx,
        "headwords",
        [](const std::string& line) {
            return line;
        },
        size);

    auto reduced_words = word_count_user(lines);

    reduced_words.WriteToFileSystem(
        "wordcount_" + std::to_string(ctx.rank()) + ".out",
        [](const WordPair& item) {
            return item.first + ": " + std::to_string(item.second);
        });
    return 0;
}

/******************************************************************************/
