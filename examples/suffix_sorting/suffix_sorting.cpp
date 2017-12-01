/*******************************************************************************
 * examples/suffix_sorting/suffix_sorting.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2016 Florian Kurpicz <florian.kurpicz@tu-dortmund.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/suffix_sorting/check_sa.hpp>
#include <examples/suffix_sorting/construct_bwt.hpp>
#include <examples/suffix_sorting/construct_lcp.hpp>
#include <examples/suffix_sorting/construct_wt.hpp>
#include <examples/suffix_sorting/dc3.hpp>
#include <examples/suffix_sorting/dc7.hpp>
#include <examples/suffix_sorting/prefix_doubling.hpp>
#include <examples/suffix_sorting/prefix_quadrupling.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/equal_to_dia.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/write_binary.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/uint_types.hpp>
#include <tlx/cmdline_parser.hpp>

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
    bool pack_input_ = false;
    bool lcp_computation_ = false;

    std::string output_bwt_;
    std::string output_wavelet_tree_;

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
                ctx, sizelimit_, [](size_t /* i */) { return uint8_t('a'); });
            SwitchIndexType(input_dia, sizelimit_);
        }
        else if (input_path_ == "random") {
            if (sizelimit_ == std::numeric_limits<size_t>::max()) {
                LOG1 << "You must provide -s <size> for generated inputs.";
                return;
            }

            // share prng in Generate (just random numbers anyway)
            std::mt19937 prng(
                std::random_device { } () + 4096 * ctx.my_rank());

            DIA<uint8_t> input_dia =
                Generate(
                    ctx, sizelimit_,
                    [&prng](size_t /* index */) {
                        return static_cast<uint8_t>(prng());
                    })
                // the random input _must_ be cached, otherwise it will be
                // regenerated ... and contain new numbers.
                .Cache();
            SwitchIndexType(input_dia, sizelimit_);
        }
        else if (input_path_ == "random10") {
            if (sizelimit_ == std::numeric_limits<size_t>::max()) {
                LOG1 << "You must provide -s <size> for generated inputs.";
                return;
            }

            // share prng in Generate (just random digits anyway)
            std::mt19937 prng(
                std::random_device { } () + 4096 * ctx.my_rank());

            DIA<uint8_t> input_dia =
                Generate(
                    ctx, sizelimit_,
                    [&prng](size_t /* index */) {
                        return static_cast<uint8_t>(
                            '0' + ((prng() >> 6) % 10));
                    })
                // the random input _must_ be cached, otherwise it will be
                // regenerated ... and contain new numbers.
                .Cache();
            SwitchIndexType(input_dia, sizelimit_);
        }
        else if (input_path_ == "random2") {
            if (sizelimit_ == std::numeric_limits<size_t>::max()) {
                LOG1 << "You must provide -s <size> for generated inputs.";
                return;
            }

            // share prng in Generate (just random digits anyway)
            std::mt19937 prng(
                std::random_device { } () + 4096 * ctx.my_rank());

            DIA<uint8_t> input_dia =
                Generate(
                    ctx, sizelimit_,
                    [&prng](size_t /* index */) {
                        return static_cast<uint8_t>(
                            '0' + ((prng() >> 6) % 2));
                    })
                // the random input _must_ be cached, otherwise it will be
                // regenerated ... and contain new numbers.
                .Cache();
            SwitchIndexType(input_dia, sizelimit_);
        }
        else {
            auto input_dia =
                ReadBinary<uint8_t>(ctx, input_path_, sizelimit_).Cache();
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
#if !THRILL_ON_TRAVIS
        else if (sa_index_bytes_ == 5)
            return StartInput<common::uint40>(input_dia, input_size);
        else if (sa_index_bytes_ == 8)
            return StartInput<uint64_t>(input_dia, input_size);
#endif
        else
            die("Unsupported index byte size: " << sa_index_bytes_ <<
                ". Byte size has to be 4, 5, or 8");
    }

    template <typename Index, typename InputDIA>
    void StartInput(const InputDIA& input_dia, uint64_t input_size) const {

        // generate or load input prior to starting timer
        input_dia.Execute();

        common::StatsTimerStart timer;

        DIA<Index> suffix_array;
        if (algorithm_ == "none") {
            suffix_array = Generate(
                input_dia.ctx(), 0, [](size_t index) { return Index(index); });
        }
        else if (algorithm_ == "dc3") {
            suffix_array = DC3<Index>(input_dia.Keep(), input_size, 256);
        }
        else if (algorithm_ == "dc7") {
            suffix_array = DC7<Index>(input_dia.Keep(), input_size, 256);
        }
        else if (algorithm_ == "pdw") {
            suffix_array = PrefixDoublingWindow<Index>(
                input_dia.Keep(), input_size, pack_input_);
        }
        else if (algorithm_ == "pds") {
            suffix_array = PrefixDoublingSorting<Index>(
                input_dia.Keep(), input_size, pack_input_);
        }
        else if (algorithm_ == "dis") {
            suffix_array = PrefixDoublingDiscarding<Index>(
                input_dia.Keep(), input_size, pack_input_);
        }
        else if (algorithm_ == "q") {
            suffix_array = PrefixQuadrupling<Index>(
                input_dia.Keep(), input_size, pack_input_);
        }
        else if (algorithm_ == "qd") {
            suffix_array = PrefixQuadruplingDiscarding<Index>(
                input_dia.Keep(), input_size, pack_input_);
        }
        else {
            die("Unknown algorithm \"" << algorithm_ << "\"");
        }

        suffix_array.Execute();
        timer.Stop();

        bool check_result = false;
        if (check_flag_) {
            if (input_dia.context().my_rank() == 0)
                LOG1 << "checking suffix array...";
            die_unless(CheckSA(input_dia.Keep(), suffix_array.Keep()));
            check_result = true;
        }

        if (input_dia.context().my_rank() == 0) {
            std::cerr << "RESULT"
                      << " algo=" << algorithm_
                      << " hosts=" << input_dia.context().num_hosts()
                      << " check_result=" << check_result
                      << " time=" << timer
                      << (getenv("RESULT") ? getenv("RESULT") : "")
                      << std::endl;
        }

        if (text_output_flag_) {
            suffix_array.Keep().Print("suffix_array");
        }

        if (output_path_.size()) {
            if (input_dia.context().my_rank() == 0)
                LOG1 << "writing suffix array to " << output_path_;
            suffix_array.Keep().WriteBinary(output_path_);
        }

        if (output_bwt_.size()) {
            InputDIA bw_transform =
                ConstructBWT(input_dia, suffix_array, input_size);

            if (text_output_flag_) {
                bw_transform.Keep().Print("bw_transform");
            }
            if (input_dia.context().my_rank() == 0) {
                LOG1 << "writing Burrows–Wheeler transform to "
                     << output_bwt_;
            }
            if (output_wavelet_tree_.size()) bw_transform.Keep();
            bw_transform.WriteBinary(output_bwt_);

            if (output_wavelet_tree_.size())
                ConstructWaveletTree(bw_transform, output_wavelet_tree_);
        }
        else if (output_wavelet_tree_.size()) {
            ConstructWaveletTree(
                ConstructBWT(input_dia, suffix_array, input_size),
                output_wavelet_tree_);
        }

        if (lcp_computation_) {
            auto bwt = ConstructBWT(input_dia, suffix_array, input_size);
            ConstructLCP(input_dia, suffix_array, bwt, input_size);
        }
    }
};

int main(int argc, char* argv[]) {

    using namespace thrill; // NOLINT

    tlx::CmdlineParser cp;

    cp.set_description("A collection of suffix array construction algorithms.");
    cp.set_author("Florian Kurpicz <florian.kurpicz@tu-dortmund.de>");
    cp.set_author("Timo Bingmann <tb@panthema.net>");

    SuffixSorting ss;

    cp.add_param_string("input", ss.input_path_,
                        "Path to input file (or verbatim text).\n"
                        "The special inputs "
                        "'random', 'random10', 'random2' and 'unary' "
                        "generate such text on-the-fly.");

    cp.add_string('a', "algorithm", ss.algorithm_,
                  "The algorithm which is used to construct the suffix array. "
                  "Available are: "
                  "[pdw]indow (default), [pds]orting, "
                  "prefix doubling with [dis]carding, "
                  "[q]uadrupling, [qd] quadrupling with carding, "
                  "[dc3], and [dc7], or [none] for skipping.");

    cp.add_size_t('b', "bytes", ss.sa_index_bytes_,
                  "Suffix array bytes per index: "
                  "4 (32-bit) (default), 5 (40-bit), 8 (64-bit)");

    cp.add_string('B', "bwt", ss.output_bwt_,
                  "Compute the Burrows–Wheeler transform in addition to the "
                  "suffix array, and write to file.");

    cp.add_bool('c', "check", ss.check_flag_,
                "Check suffix array for correctness.");

    cp.add_bool('d', "debug", examples::suffix_sorting::debug_print,
                "Print debug info.");

    cp.add_string('i', "input-copy", ss.input_copy_path_,
                  "Write input text to given path.");

    cp.add_string('o', "output", ss.output_path_,
                  "Output suffix array [and if constructed Burrows–Wheeler "
                  "transform] to given path.");

    cp.add_bytes('s', "size", ss.sizelimit_,
                 "Cut input text to given size, e.g. 2 GiB.");

    cp.add_bool('t', "text", ss.text_output_flag_,
                "Print out suffix array [and if constructed Burrows-Wheeler "
                "transform] in readable text.");

    cp.add_bool('v', "verbatim", ss.input_verbatim_,
                "Consider \"input\" as verbatim text to construct "
                "suffix array on.");

    cp.add_string('w', "wavelet", ss.output_wavelet_tree_,
                  "Compute the Wavelet Tree of the Burrows-Wheeler transform, "
                  "and write to file.");

    cp.add_bool('p', "packed", ss.pack_input_,
                "Fit as many characters of the input in the bytes used per index"
                " in the suffix array.");

    cp.add_bool('l', "lcp", ss.lcp_computation_,
                "Compute the LCP array in addition to the SA. Currently this "
                "requires the construction of the BWT.");

    if (!cp.process(argc, argv))
        return -1;

    return Run([&](Context& ctx) { return ss.Run(ctx); });
}

/******************************************************************************/
