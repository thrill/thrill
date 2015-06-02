/*******************************************************************************
 * examples/word_count_simple.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <random>
#include <thread>
#include <string>

#include <c7a/api/bootstrap.hpp>
#include <c7a/api/dia.hpp>
#include <c7a/common/cmdline_parser.hpp>

int word_count(c7a::Context& context);

int main(int argc, char* argv[]) {

    using c7a::Execute;

    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(30000, 65000);
    const size_t port_base = distribution(generator);

    c7a::common::CmdlineParser clp;

    //clp.SetVerboseProcess(false);

    unsigned int size = 1;
    clp.AddUInt('n', "size", "N", size,
                "Create wordcount example with N workers");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    std::vector<std::thread> threads(size);
    std::vector<char**> arguments(size);
    std::vector<std::vector<std::string> > strargs(size);

    for (size_t i = 0; i < size; i++) {

        arguments[i] = new char*[size + 2];
        strargs[i].resize(size + 2);

        for (size_t j = 0; j < size; j++) {
            strargs[i][j + 2] += "127.0.0.1:";
            strargs[i][j + 2] += std::to_string(port_base + j);
            arguments[i][j + 2] = const_cast<char*>(strargs[i][j + 2].c_str());
        }

        strargs[i][0] = "wordcount";
        arguments[i][0] = const_cast<char*>(strargs[i][0].c_str());
        strargs[i][1] = std::to_string(i);
        arguments[i][1] = const_cast<char*>(strargs[i][1].c_str());
        threads[i] = std::thread([=]() { Execute(size + 2, arguments[i], word_count); });
        //  threads.push_back(thread);
    }

    for (size_t i = 0; i < size; i++) {
        threads[i].join();
    }

    //c7a::Execute(argc, argv, word_count);
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

    std::cout << ctx.get_current_dir() + "/tests/inputs/wordcount.in" << std::endl;
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
                                    std::cout << str << std::endl;
                                    return str;
                                });
    return 0;
}

/******************************************************************************/
