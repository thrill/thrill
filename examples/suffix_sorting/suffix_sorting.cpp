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

#include <examples/suffix_sorting/check_sa.hpp>
#include <examples/suffix_sorting/construct_bwt.hpp>
#include <examples/suffix_sorting/dc3.hpp>
#include <examples/suffix_sorting/dc7.hpp>
#include <examples/suffix_sorting/prefix_doubling.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/equal_to_dia.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/read_binary.hpp>
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

using namespace thrill;                   // NOLINT
using namespace examples::suffix_sorting; // NOLINT

namespace examples {
namespace suffix_sorting {

bool debug_print = false;

} // namespace suffix_sorting
} // namespace examples

/*!
 * Class to encapsulate all suffix sorting algorithms
 */
class SuffixSorting
{
public:
    std::string input_path_;
    std::string input_copy_path_;
    std::string output_path_;
    uint64_t sizelimit_ = std::numeric_limits<uint64_t>::max();

    std::string algorithm_;

    bool text_output_flag_ = false;
    bool check_flag_ = false;
    bool input_verbatim_ = false;
    bool construct_bwt_ = false;

    size_t sa_index_bytes_ = 4;

    void Run(Context& ctx) const {
        ctx.enable_consume();

        if (input_verbatim_) {
            // take path as verbatim text
            std::vector<uint8_t> input_vec(input_path_.begin(), input_path_.end());
            auto input_dia = EqualToDIA(ctx, input_vec).Collapse();
            SwitchIndexType(input_dia, input_vec.size());
        }
        else if (input_path_ == "unary") {
            if (sizelimit_ == std::numeric_limits<size_t>::max()) {
                LOG1 << "You must provide -s <size> for generated inputs.";
                return;
            }

            DIA<uint8_t> input_dia = Generate(
                ctx, [](size_t /* i */) { return uint8_t('a'); }, sizelimit_);
            SwitchIndexType(input_dia, sizelimit_);
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
                    ctx,
                    [&prng](size_t /* i */) {
                        return static_cast<uint8_t>(prng());
                    },
                    sizelimit_)
                // the random input _must_ be cached, otherwise it will be
                // regenerated ... and contain new numbers.
                .Cache();
            SwitchIndexType(input_dia, sizelimit_);
        }
        else {
            auto input_dia = ReadBinary<uint8_t>(ctx, input_path_).Collapse();
            size_t input_size = input_dia.Keep().Size();
            SwitchIndexType(input_dia, input_size);
        }
    }

    template <typename InputDIA>
    void SwitchIndexType(const InputDIA& input_dia, uint64_t input_size) const {

        if (input_copy_path_.size())
            input_dia.Keep().WriteBinary(input_copy_path_);

        if (sa_index_bytes_ == 4)
            return StartInput<uint32_t>(input_dia, input_size);
        else
            die("Unsupported index byte size: " << sa_index_bytes_ <<
                ". Byte size has to be 4,5,6 or 8");
    }

    template <typename Index, typename InputDIA>
    void StartInput(const InputDIA& input_dia, uint64_t input_size) const {

        DIA<Index> suffix_array;
        if (algorithm_ == "dc3") {
            suffix_array = DC3<Index>(input_dia.Keep(), input_size);
        }
        else if (algorithm_ == "dc7") {
            suffix_array = DC7<Index>(input_dia.Keep(), input_size);
        }
        else if (algorithm_ == "de") {
            suffix_array = PrefixDoublingDementiev<Index>(input_dia.Keep(), input_size);
        }
        else {
            suffix_array = PrefixDoubling<Index>(input_dia.Keep(), input_size);
        }

        if (check_flag_) {
            if (input_dia.context().my_rank() == 0)
                LOG1 << "checking suffix array...";
            die_unless(CheckSA(input_dia.Keep(), suffix_array.Keep()));
        }

        if (text_output_flag_) {
            suffix_array.Keep().Print("suffix_array");
        }

        if (output_path_.size()) {
            if (input_dia.context().my_rank() == 0)
                LOG1 << "writing suffix array to " << output_path_;
            suffix_array.Keep().WriteBinary(output_path_);
        }

        if (construct_bwt_) {
            InputDIA bw_transform =
                ConstructBWT(input_dia.Keep(), suffix_array.Keep(), input_size);

            if (text_output_flag_) {
                bw_transform.Keep().Print("bw_transform");
            }
            if (output_path_.size()) {
                if (input_dia.context().my_rank() == 0) {
                    LOG1 << "writing Burrows–Wheeler transform to "
                         << output_path_ << "-bwt-";
                }
                bw_transform.WriteBinary(output_path_ + "-bwt-");
            }
        }
    }
};

int main(int argc, char* argv[]) {

    using namespace thrill; // NOLINT

    common::CmdlineParser cp;

    cp.SetDescription("A collection of suffix array construction algorithms.");
    cp.SetAuthor("Florian Kurpicz <florian.kurpicz@tu-dortmund.de>");
    cp.SetAuthor("Timo Bingmann <tb@panthema.net>");

    SuffixSorting ss;

    cp.AddParamString("input", ss.input_path_,
                      "Path to input file (or verbatim text).\n"
                      "The special inputs 'random' and 'unary' generate "
                      "such text on-the-fly.");

    cp.AddString('a', "algorithm", ss.algorithm_,
                 "The prefix doubling algorithm which is used to construct the "
                 "suffix array. Available: "
                 "[fl]ick (default), [de]mentiev, [dc3], and [dc7]");

    cp.AddSizeT('b', "bytes", ss.sa_index_bytes_,
                "Suffix array bytes per index: "
                "4 (32-bit) (default), 5 (40-bit), 6 (48-bit), 8 (64-bit)");

    cp.AddFlag('c', "check", ss.check_flag_,
               "Check suffix array for correctness.");

    cp.AddFlag('d', "debug", examples::suffix_sorting::debug_print,
               "Print debug info.");

    cp.AddString('i', "input-copy", ss.input_copy_path_,
                 "Write input text to given path.");

    cp.AddString('o', "output", ss.output_path_,
                 "Output suffix array [and if constructed Burrows–Wheeler "
                 "transform] to given path.");

    cp.AddBytes('s', "size", ss.sizelimit_,
                "Cut input text to given size, e.g. 2 GiB. (TODO: not working)");

    cp.AddFlag('t', "text", ss.text_output_flag_,
               "Print out suffix array [and if constructed Burrows–Wheeler "
               "transform] in readable text.");

    cp.AddFlag('v', "verbatim", ss.input_verbatim_,
               "Consider \"input\" as verbatim text to construct "
               "suffix array on.");

    cp.AddFlag('w', "bwt", ss.construct_bwt_,
               "Compute the Burrows–Wheeler transform in addition to the "
               "suffix array.");

    if (!cp.Process(argc, argv))
        return -1;

    return Run([&](Context& ctx) { return ss.Run(ctx); });
}

/******************************************************************************/
