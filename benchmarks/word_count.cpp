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

int word_count(c7a::Context& context);

int main(int argc, char* argv[]) {
    return c7a::Execute(argc, argv, word_count);
}

//! The WordCount user program
int word_count(c7a::Context& ctx) {
    using c7a::Context;
    using WordPair = std::pair<std::string, int>;

    auto line_to_words = [](std::string line, std::function<void(WordPair)> emit) {
                             std::string word;
                             std::istringstream iss(line);
                             while (iss >> word) {
                                 WordPair wp = std::make_pair(word, 1);
                                 emit(wp);
                             }
                         };
    auto key = [](WordPair in) {
                   return in.first;
               };
    auto red_fn = [](WordPair in1, WordPair in2) {
                      WordPair wp = std::make_pair(in1.first, in1.second + in2.second);
                      return wp;
                  };

    //std::cout << ctx.get_current_dir() + "/tests/inputs/wordcount.in" << std::endl;
    auto lines = ReadFromFileSystem(
        ctx,
        ctx.get_current_dir() + "/tests/inputs/wordcount.in",
        [](const std::string& line) {
            return line;
        });

    auto word_pairs = lines.FlatMap(line_to_words);

    auto red_words = word_pairs.ReduceBy(key).With(red_fn);

    red_words.WriteToFileSystem(ctx.get_current_dir() + "/tests/outputs/wordcount.out",
                                [](const WordPair& item) {
                                    std::string str;
                                    str += item.first;
                                    str += ": ";
                                    str += std::to_string(item.second);
                                    //std::cout << str << std::endl;
                                    return str;
                                });

    return 0;
}

/******************************************************************************/
