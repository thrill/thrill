/*******************************************************************************
 * examples/suffix_sorting/suffix_sorting.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2016 Florian Kurpicz <florian.kurpicz@tu-dortmund.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/suffix_sorting/bwt_generator.hpp>
#include <examples/suffix_sorting/prefix_doubling.hpp>
#include <examples/suffix_sorting/sa_checker.hpp>

#include <thrill/api/equal_to_dia.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/write_binary.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/uint_types.hpp>

#include <algorithm>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

using namespace thrill;
using namespace examples::suffix_sorting;

bool generate_bwt = false;

/*!
 * Class to encapsulate all suffix sorting algorithms
 */
class StartPrefixDoubling
{
public:
    StartPrefixDoubling(
        Context& ctx,
        const std::string& input_path, const std::string& input_copy_path,
        const std::string& output_path,
        size_t sizelimit,
        const std::string& pd_algorithm,
        bool text_output_flag,
        bool check_flag,
        bool input_verbatim,
        size_t sa_index_bytes)
        : ctx_(ctx),
          input_path_(input_path), input_copy_path_(input_copy_path),
          output_path_(output_path),
          sizelimit_(sizelimit),
          pd_algorithm_(pd_algorithm),
          text_output_flag_(text_output_flag),
          check_flag_(check_flag),
          input_verbatim_(input_verbatim),
          sa_index_bytes_(sa_index_bytes) { }

    void Run() {
        if (input_verbatim_) {
            // take path as verbatim text
            std::vector<uint8_t> input_vec(input_path_.begin(), input_path_.end());
            auto input_dia = EqualToDIA(ctx_, input_vec).Collapse();
            SwitchSuffixSortingIndexType(input_dia, input_vec.size());
        }
        else if (input_path_ == "unary") {
            if (sizelimit_ == std::numeric_limits<size_t>::max()) {
                LOG1 << "You must provide -s <size> for generated inputs.";
                return;
            }

            DIA<uint8_t> input_dia = Generate(
                ctx_, [](size_t /* i */) { return uint8_t('a'); }, sizelimit_);
            SwitchSuffixSortingIndexType(input_dia, sizelimit_);
        }
        else if (input_path_ == "random") {
            if (sizelimit_ == std::numeric_limits<size_t>::max()) {
                LOG1 << "You must provide -s <size> for generated inputs.";
                return;
            }

            // share prng in Generate (just random numbers anyway)
            std::default_random_engine prng(std::random_device { } ());

            DIA<uint8_t> input_dia =
                Generate(
                    ctx_,
                    [&prng](size_t /* i */) {
                        return static_cast<uint8_t>(prng());
                    },
                    sizelimit_)
                // the random input _must_ be cached, otherwise it will be
                // regenerated ... and contain new numbers.
                .Cache();
            SwitchSuffixSortingIndexType(input_dia, sizelimit_);
        }
        else {
            auto input_dia = ReadBinary<uint8_t>(ctx_, input_path_).Collapse();
            size_t input_size = input_dia.Size();
            SwitchSuffixSortingIndexType(input_dia, input_size);
        }
    }

    template <typename Index, typename InputDIA>
    void StartPrefixDoublingInput(
        const InputDIA& input_dia, uint64_t input_size) {

        DIA<Index> suffix_array;
        if (pd_algorithm_ == "de") {
            suffix_array = PrefixDoublingDementiev<Index>(input_dia.Keep(), input_size);
        }
        else {
            suffix_array = PrefixDoubling<Index>(input_dia.Keep(), input_size);
        }

        if (check_flag_) {
            LOG1 << "checking suffix array...";
            die_unless(CheckSA(input_dia.Keep(), suffix_array));
        }

        if (text_output_flag_) {
            suffix_array.Keep().Print("suffix_array");
        }

        if (output_path_.size()) {
            LOG1 << "writing suffix array to " << output_path_;
            suffix_array.Keep().WriteBinary(output_path_);
        }
        if (generate_bwt) {
            InputDIA bw_transform = GenerateBWT(input_dia, suffix_array);

            if (text_output_flag_) {
                bw_transform.Keep().Print("bw_transform");
            }
            if (output_path_.size()) {
                LOG1 << "writing Burrows–Wheeler transform to " << output_path_;
                bw_transform.WriteBinary(output_path_ + "bwt");
            }
        }
    }

    template <typename InputDIA>
    void SwitchSuffixSortingIndexType(const InputDIA& input_dia,
                                      uint64_t input_size) {

        if (input_copy_path_.size())
            input_dia.Keep().WriteBinary(input_copy_path_);

        if (sa_index_bytes_ == 4)
            return StartPrefixDoublingInput<uint32_t>(input_dia, input_size);
#ifndef NDEBUG
        else if (sa_index_bytes_ == 5)
            return StartPrefixDoublingInput<common::uint40>(input_dia, input_size);
        else if (sa_index_bytes_ == 6)
            return StartPrefixDoublingInput<common::uint48>(input_dia, input_size);
        else if (sa_index_bytes_ == 8)
            return StartPrefixDoublingInput<uint64_t>(input_dia, input_size);
#endif
        else
            die("Unsupported index byte size: " << sa_index_bytes_ <<
                ". Byte size has to be 4,5,6 or 8");
    }

protected:
    Context& ctx_;

    std::string input_path_;
    std::string input_copy_path_;
    std::string output_path_;
    uint64_t sizelimit_;

    std::string pd_algorithm_;

    bool text_output_flag_;
    bool check_flag_;
    bool input_verbatim_;

    size_t sa_index_bytes_;
};

int main(int argc, char* argv[]) {

    using namespace thrill; // NOLINT

    common::CmdlineParser cp;

    cp.SetDescription("A collection of prefix doubling suffix array construction algorithms.");
    cp.SetAuthor("Florian Kurpicz <florian.kurpicz@tu-dortmund.de>");

    std::string input_path, input_copy_path, output_path;
    std::string pd_algorithm;
    uint64_t sizelimit = std::numeric_limits<uint64_t>::max();
    bool text_output_flag = false;
    bool check_flag = false;
    bool input_verbatim = false;
    size_t sa_index_bytes = 4;

    cp.AddParamString("input", input_path,
                      "Path to input file (or verbatim text).\n"
                      "The special inputs 'random' and 'unary' generate "
                      "such text on-the-fly.");
    cp.AddFlag('c', "check", check_flag,
               "Check suffix array for correctness.");
    cp.AddFlag('t', "text", text_output_flag,
               "Print out suffix array [and if constructred Burrows–Wheeler "
               "transform] in readable text.");
    cp.AddString('i', "input-copy", input_copy_path,
                 "Write input text to given path.");
    cp.AddString('o', "output", output_path,
                 "Output suffix array [and if constructred Burrows–Wheeler "
                 "transform] to given path.");
    cp.AddFlag('v', "verbatim", input_verbatim,
               "Consider \"input\" as verbatim text to construct "
               "suffix array on.");
    cp.AddBytes('s', "size", sizelimit,
                "Cut input text to given size, e.g. 2 GiB. (TODO: not working)");
#if 0
    cp.AddFlag('d', "debug", examples::suffix_sorting::debug_print,
               "Print debug info.");
#endif
    cp.AddString('a', "algorithm", pd_algorithm,
                 "The prefix doubling algorithm which is used to construct the "
                 "suffix array. [fl]ick (default) and [de]mentiev are "
                 "available.");
    cp.AddSizeT('b', "bytes", sa_index_bytes,
                "Suffix array bytes per index: "
                "4 (32-bit) (default), 5 (40-bit), 6 (48-bit), 8 (64-bit)");
    cp.AddFlag('w', "bwt", generate_bwt,
               "Compute the Burrows–Wheeler transform in addition to the "
               "suffix array.");

    if (!cp.Process(argc, argv))
        return -1;

    return Run(
        [&](Context& ctx) {
            return StartPrefixDoubling(
                ctx,
                input_path, input_copy_path, output_path, sizelimit,
                pd_algorithm,
                text_output_flag,
                check_flag,
                input_verbatim,
                sa_index_bytes).Run();
        });
}

/******************************************************************************/
