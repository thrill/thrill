/*******************************************************************************
 * thrill/core/duplicate_detection.hpp
 *
 * Duplicate detection
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_DUPLICATE_DETECTION_HEADER
#define THRILL_CORE_DUPLICATE_DETECTION_HEADER

#include <thrill/api/context.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/dynamic_bitset.hpp>

#include <algorithm>

namespace thrill {
namespace core {

class DuplicateDetection
{

private:
    void WriteEncodedHashes(data::CatStreamPtr stream_pointer,
                            const std::vector<size_t>& hashes,
                            size_t b,
                            size_t space_bound,
                            size_t num_workers,
                            size_t max_hash) {

        std::vector<data::CatStream::Writer> writers =
            stream_pointer->GetWriters();

        size_t j = 0;
        for (size_t i = 0; i < num_workers; ++i) {
            common::Range range_i = common::CalculateLocalRange(max_hash, num_workers, i);

            // TODO: Lower bound.
            core::DynamicBitset<size_t>
            golomb_code(space_bound, false, b);

            golomb_code.seek();

            size_t delta = 0;
            size_t num_elements = 0;

            if (j < hashes.size() && hashes[j] == 0) {
                golomb_code.golomb_in(0);
                ++num_elements;
                ++j;
            }

            for (            /*j is already set from previous workers*/
                ; j < hashes.size() && hashes[j] < range_i.end; ++j) {

                if (hashes[j] != delta) {
                    ++num_elements;
                    golomb_code.golomb_in(hashes[j] - delta);
                    delta = hashes[j];
                }
            }

            golomb_code.seek();

            writers[i].Put(golomb_code.byte_size() + sizeof(size_t));
            writers[i].Put(num_elements);
            writers[i].Append(golomb_code.GetGolombData(),
                              golomb_code.byte_size() + sizeof(size_t));
            writers[i].Close();
        }
    }

    void ReadEncodedHashesToVector(data::CatStreamPtr stream_pointer,
                                   std::vector<size_t>& target_vec,
                                   size_t b) {

        auto reader = stream_pointer->GetCatReader(/* consume */ true);

        size_t readindex = 0;
        while (reader.HasNext()) {

            size_t data_size = reader.template Next<size_t>();
            size_t num_elements = reader.template Next<size_t>();
            size_t* raw_data = new size_t[data_size];
            reader.Read(raw_data, data_size);

            core::DynamicBitset<size_t> golomb_code(raw_data,
                                                    common::IntegerDivRoundUp(data_size,
                                                                              sizeof(size_t)),
                                                    b, num_elements);
            golomb_code.seek();

            size_t last = 0;
            for (size_t i = 0; i < num_elements; ++i) {

                size_t new_elem = golomb_code.golomb_out() + last;
                target_vec.push_back(new_elem);
                last = new_elem;
            }
            delete[] raw_data;
            readindex++;
        }
    }

public:
    size_t FindDuplicates(std::vector<size_t>& duplicates,
                          std::vector<size_t>& hashes,
                          Context& context,
                          size_t unique_elements,
                          size_t dia_id) {

        size_t upper_bound_uniques = context.net.AllReduce(unique_elements);

        double fpr_parameter = 8;
        size_t b = (size_t)fpr_parameter;  //(size_t)(std::log(2) * fpr_parameter);
        size_t upper_space_bound = upper_bound_uniques * (2 + std::log2(fpr_parameter));
        size_t max_hash = upper_bound_uniques * fpr_parameter;

        for (size_t i = 0; i < hashes.size(); ++i) {
            hashes[i] = hashes[i] % max_hash;
        }

        std::sort(hashes.begin(), hashes.end());

        data::CatStreamPtr golomb_data_stream = context.GetNewCatStream(dia_id);

        WriteEncodedHashes(golomb_data_stream,
                           hashes, b,
                           upper_space_bound,
                           context.num_workers(),
                           max_hash);

        std::vector<size_t> hashes_dups;

        ReadEncodedHashesToVector(golomb_data_stream,
                                  hashes_dups, b);

        std::sort(hashes_dups.begin(), hashes_dups.end());

        core::DynamicBitset<size_t>
        duplicate_code(upper_space_bound, false, b);

        duplicate_code.seek(0);

        size_t delta = 0;
        size_t num_elements = 0;

        if (hashes_dups.size()) {
            if (hashes_dups.size() >= 2 && hashes_dups[0] == 0 && hashes_dups[1] == 0) {
                // case for duplicated 0, needs to be a special case as delta is 0 in the beginning
                duplicate_code.golomb_in(0);
                ++num_elements;
            }

            for (size_t j = 0; j < hashes_dups.size() - 1; ++j) {
                if ((hashes_dups[j] == hashes_dups[j + 1]) && (hashes_dups[j] != delta)) {
                    duplicate_code.golomb_in(hashes_dups[j] - delta);
                    delta = hashes_dups[j];
                    ++num_elements;
                }
            }
        }

        data::CatStreamPtr duplicates_stream = context.GetNewCatStream(dia_id);

        std::vector<data::CatStream::Writer> duplicate_writers =
            duplicates_stream->GetWriters();

        for (size_t i = 0; i < duplicate_writers.size(); ++i) {

            duplicate_writers[i].Put(duplicate_code.byte_size() + sizeof(size_t));
            duplicate_writers[i].Put(num_elements);
            duplicate_writers[i].Append(duplicate_code.GetGolombData(),
                                        duplicate_code.byte_size() + sizeof(size_t));
            duplicate_writers[i].Close();
        }

        ReadEncodedHashesToVector(duplicates_stream,
                                  duplicates, b);

        assert(std::is_sorted(duplicates.begin(), duplicates.end()));

        return max_hash;
    }
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_DUPLICATE_DETECTION_HEADER

/******************************************************************************/
