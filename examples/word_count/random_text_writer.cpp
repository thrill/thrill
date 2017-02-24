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
#include <thrill/api/generate.hpp>
#include <thrill/api/write_lines.hpp>
#include <tlx/cmdline_parser.hpp>

#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <vector>

using namespace thrill;               // NOLINT
using namespace examples::word_count; // NOLINT

unsigned min_words_key = 5, max_words_key = 10,
    min_words_value = 20, max_words_value = 100;

unsigned seed = 123456;

uint64_t totalbytes;
bool tab_separator = false;

unsigned range_words_key, range_words_value;

static int Sequential(std::ostream& os) {

    std::mt19937 prng(seed);

    uint64_t written_bytes = 0;

    while (written_bytes < totalbytes)
    {
        unsigned num_words_key =
            min_words_key + static_cast<unsigned>(prng()) % range_words_key;
        unsigned num_words_value =
            min_words_value + static_cast<unsigned>(prng()) % range_words_value;

        std::string key_words = RandomTextWriterGenerate(num_words_key, prng);
        std::string value_words = RandomTextWriterGenerate(num_words_value, prng);

        size_t out_size = key_words.size() + 1 + value_words.size() + 1;
        if (written_bytes + out_size > totalbytes) break;

        if (tab_separator)
            os << key_words << '\t' << value_words << '\n';
        else
            os << key_words << value_words << '\n';

        written_bytes += out_size;
    }

    return 0;
}

static void Parallel(api::Context& ctx, const std::string& output) {

    size_t num_workers = ctx.num_workers();
    std::mt19937 prng(seed + ctx.my_rank());

    // generate sentinel value for each worker
    Generate(ctx, num_workers)
    // map sentinel to many lines
    .FlatMap<std::string>(
        [&](size_t /* index */, auto emit) {

            uint64_t written_bytes = 0;
            while (written_bytes < totalbytes / num_workers)
            {
                unsigned num_words_key =
                    min_words_key + static_cast<unsigned>(prng()) % range_words_key;
                unsigned num_words_value =
                    min_words_value + static_cast<unsigned>(prng()) % range_words_value;

                std::string key_words = RandomTextWriterGenerate(num_words_key, prng);
                std::string value_words = RandomTextWriterGenerate(num_words_value, prng);

                size_t out_size = key_words.size() + 1 + value_words.size() + 1;
                if (written_bytes + out_size > totalbytes) break;

                if (tab_separator)
                    emit(key_words + "\t" + value_words);
                else
                    emit(key_words + value_words);

                written_bytes += out_size;
            }
        })
    .WriteLines(output);
}

int main(int argc, char* argv[]) {

    tlx::CmdlineParser cp;

    cp.add_unsigned('k', "min_words_key", "<N>", min_words_key,
                    "minimum words in a key");
    cp.add_unsigned('K', "max_words_key", "<N>", max_words_key,
                    "maximum words in a key");

    cp.add_unsigned('v', "min_words_value", "<N>", min_words_value,
                    "minimum words in a value");
    cp.add_unsigned('V', "max_words_value", "<N>", max_words_value,
                    "maximum words in a value");

    cp.add_unsigned('s', "seed", "<N>", seed,
                    "random seed (default: 123456)");

    cp.add_bool(0, "tab-separator", tab_separator,
                "add TAB as separator of key/value (for compatbility)");

    cp.add_param_bytes("totalbytes", totalbytes,
                       "total number of bytes to generate (approximately)");

    bool parallel = false;
    cp.add_bool(1, "parallel", parallel,
                "run as Thrill parallel/distributed program");

    std::string output;
    cp.add_string('o', "output", "<path>", output, "output path");

    if (!cp.process(argc, argv)) {
        return -1;
    }

    cp.print_result(std::cerr);

    // calculate range of words
    range_words_key = max_words_key - min_words_key;
    range_words_value = max_words_value - min_words_value;

    if (!parallel) {
        if (output.size()) {
            std::ofstream of(output);
            return Sequential(of);
        }
        else {
            return Sequential(std::cout);
        }
    }
    else {
        return api::Run([&](api::Context& ctx) {
                            Parallel(ctx, output);
                        });
    }
}

/******************************************************************************/
