/*******************************************************************************
 * examples/terasort/terasort.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/generate.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/write_binary.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/string.hpp>
#include <tlx/cmdline_parser.hpp>

#include <tlx/string/hexdump.hpp>
#include <tlx/string/parse_si_iec_units.hpp>

#include <algorithm>
#include <random>
#include <string>
#include <utility>
#include <vector>

using namespace thrill;               // NOLINT

struct Record {
    uint8_t key[10];
    uint8_t value[90];

    bool operator < (const Record& b) const {
        return std::lexicographical_compare(key, key + 10, b.key, b.key + 10);
    }
    friend std::ostream& operator << (std::ostream& os, const Record& c) {
        return os << tlx::hexdump(c.key, 10);
    }
} TLX_ATTRIBUTE_PACKED;

static_assert(sizeof(Record) == 100, "struct Record packing incorrect.");

struct RecordSigned {
    char key[10];
    char value[90];

    // this sorted by _signed_ characters, which is the same as what some
    // Java/Scala TeraSorts do.
    bool operator < (const RecordSigned& b) const {
        return std::lexicographical_compare(key, key + 10, b.key, b.key + 10);
    }
    friend std::ostream& operator << (std::ostream& os, const RecordSigned& c) {
        return os << tlx::hexdump(c.key, 10);
    }
} TLX_ATTRIBUTE_PACKED;

/*!
 * Generate a Record in a similar way as the "binary" version of Hadoop's
 * GenSort does. The underlying random generator is different.
 */
class GenerateRecord
{
public:
    Record operator () (size_t index) {
        Record r;

        // generate random key record
        for (size_t i = 0; i < 10; ++i)
            r.key[i] = static_cast<unsigned char>(rng_());

        uint8_t* v = r.value;

        // add 2 bytes "break"
        *v++ = 0x00;
        *v++ = 0x11;

        // fill values with hexadecimal representation of the record number
        static constexpr uint8_t hexdigits[16] = {
            '0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
        };
        uint64_t rec = index;
        for (size_t i = 0; i != 2 * sizeof(rec); ++i)
            *v++ = hexdigits[(rec >> (4 * i)) & 0x0F];
        for (size_t i = 0; i != 16; ++i)
            *v++ = '0';

        // add 4 bytes "break"
        *v++ = 0x88;
        *v++ = 0x99;
        *v++ = 0xAA;
        *v++ = 0xBB;

        // add 48 byte filler based on index
        for (size_t i = 0; i < 12; ++i) {
            uint8_t f = hexdigits[((20 + rec) >> (4 * i)) & 0x0F];
            *v++ = f;
            *v++ = f;
            *v++ = f;
            *v++ = f;
        }

        // add 4 bytes "break"
        *v++ = 0xCC;
        *v++ = 0xDD;
        *v++ = 0xEE;
        *v++ = 0xFF;

        assert(v == r.value + 90);

        return r;
    }

private:
    std::default_random_engine rng_ { std::random_device { } () };
};

int main(int argc, char* argv[]) {

    tlx::CmdlineParser clp;

    bool use_signed_char = false;
    clp.add_bool('s', "signed_char", use_signed_char,
                 "compare with signed chars to compare with broken Java "
                 "implementations, default: false");

    bool generate = false;
    clp.add_bool('g', "generate", generate,
                 "generate binary record on-the-fly for testing."
                 " size: first input pattern, default: false");

    bool generate_only = false;
    clp.add_bool('G', "generate-only", generate_only,
                 "write unsorted generated binary records to output.");

    std::string output;
    clp.add_string('o', "output", output,
                   "output file pattern");

    std::vector<std::string> input;
    clp.add_param_stringlist("input", input,
                             "input file pattern(s)");

    if (!clp.process(argc, argv)) {
        return -1;
    }

    clp.print_result();

    return api::Run(
        [&](api::Context& ctx) {
            ctx.enable_consume();

            common::StatsTimerStart timer;

            if (generate_only) {
                die_unequal(input.size(), 1u);
                // parse first argument like "100mib" size
                uint64_t size;
                die_unless(tlx::parse_si_iec_units(input[0].c_str(), &size));
                die_unless(!use_signed_char);

                Generate(ctx, size / sizeof(Record), GenerateRecord())
                .WriteBinary(output);
            }
            else if (generate) {
                die_unequal(input.size(), 1u);
                // parse first argument like "100mib" size
                uint64_t size;
                die_unless(tlx::parse_si_iec_units(input[0].c_str(), &size));
                die_unless(!use_signed_char);

                auto r =
                    Generate(ctx, size / sizeof(Record), GenerateRecord())
                    .Sort();

                if (output.size())
                    r.WriteBinary(output);
                else
                    r.Size();
            }
            else {
                if (use_signed_char) {
                    auto r = ReadBinary<RecordSigned>(ctx, input).Sort();

                    if (output.size())
                        r.WriteBinary(output);
                    else
                        r.Size();
                }
                else {
                    auto r = ReadBinary<Record>(ctx, input).Sort();

                    if (output.size())
                        r.WriteBinary(output);
                    else
                        r.Size();
                }
            }

            ctx.net.Barrier();
            if (ctx.my_rank() == 0) {
                auto traffic = ctx.net_manager().Traffic();
                LOG1 << "RESULT"
                     << " benchmark=terasort"
                     << " time=" << timer
                     << " traffic=" << traffic.total()
                     << " hosts=" << ctx.num_hosts();
            }
        });
}

/******************************************************************************/
