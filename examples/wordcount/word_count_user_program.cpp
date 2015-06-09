/*******************************************************************************
 * examples/wordcount/word_count_user_program.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/api/dia.hpp>
#include <c7a/common/string.hpp>

//! The WordCount user program
int word_count(c7a::Context& ctx) {
    using c7a::Context;
    using WordPair = std::pair<std::string, int>;

    auto line_to_words =
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

    std::cout << "wordcount.in" << std::endl;
    auto lines = ReadFromFileSystem(
        ctx,
        "wordcount.in",
        [](const std::string& line) {
            return line;
        });

    auto word_pairs = lines.FlatMap(line_to_words);

    auto red_words = word_pairs.ReduceBy(key).With(red_fn);

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

    auto word_pair_gen = [](std::string line, auto emit) {
                             emit(WordPair(line, 1));
                         };

    auto key = [](WordPair in) {
                   return in.first;
               };

    auto red_fn =
        [](WordPair in1, WordPair in2) {
        return WordPair(in1.first, in1.second + in2.second);
                  };

    auto lines = GenerateFromFile(
        ctx,
        "headwords",
        [](const std::string& line) {
            return line;
        },
        size);

    auto word_pairs = lines.FlatMap(word_pair_gen);

    auto red_words = word_pairs.ReduceBy(key).With(red_fn);

    red_words.WriteToFileSystem(
        "wordcount_" + std::to_string(ctx.rank()) + ".out",
        [](const WordPair& item) {
            std::string str = item.first + ": " + std::to_string(item.second);
            //std::cout << str << std::endl;
            return str;
        });
    return 0;
}

/******************************************************************************/
