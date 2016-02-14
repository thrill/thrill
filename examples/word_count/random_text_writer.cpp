/*******************************************************************************
 * examples/word_count/random_text_writer.cpp
 *
 * A C++ clone of org.apache.hadoop.examples.RandomTextWriter. The clone outputs
 * only text lines containing words. It uses the same words, but a different
 * underlying random generator.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/word_count/random_text_writer.hpp>
#include <thrill/common/cmdline_parser.hpp>

#include <random>
#include <string>
#include <vector>

using namespace thrill; // NOLINT

int main(int argc, char* argv[]) {

    common::CmdlineParser cp;

    unsigned min_words_key = 5, max_words_key = 10,
        min_words_value = 20, max_words_value = 100;

    unsigned seed = 123456;

    uint64_t totalbytes;

    cp.AddUInt('k', "min_words_key", "<N>", min_words_key,
               "minimum words in a key");
    cp.AddUInt('K', "max_words_key", "<N>", max_words_key,
               "maximum words in a key");

    cp.AddUInt('v', "min_words_value", "<N>", min_words_value,
               "minimum words in a value");
    cp.AddUInt('V', "max_words_value", "<N>", max_words_value,
               "maximum words in a value");

    cp.AddUInt('s', "seed", "<N>", seed,
               "random seed (default: 123456)");

    cp.AddParamBytes("totalbytes", totalbytes,
                     "total number of bytes to generate (approximately)");

    cp.SetVerboseProcess(false);
    if (!cp.Process(argc, argv)) {
        return -1;
    }

    cp.PrintResult(std::cerr);

    unsigned range_words_key = max_words_key - min_words_key;
    unsigned range_words_value = max_words_value - min_words_value;

    std::mt19937 prng(seed);

    uint64_t written_bytes = 0;

    while (written_bytes < totalbytes)
    {
        unsigned num_words_key = min_words_key + prng() % range_words_key;
        unsigned num_words_value = min_words_value + prng() % range_words_value;

        std::string key_words = RandomTextWriterGenerate(num_words_key, prng);
        std::string value_words = RandomTextWriterGenerate(num_words_value, prng);

        size_t out_size = key_words.size() + 1 + value_words.size() + 1;
        if (written_bytes + out_size > totalbytes) break;

        std::cout << key_words << '\t' << value_words << '\n';

        written_bytes += out_size;
    }

    return 0;
}

/******************************************************************************/
