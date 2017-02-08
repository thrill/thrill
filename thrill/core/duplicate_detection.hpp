/*******************************************************************************
 * thrill/core/duplicate_detection.hpp
 *
 * Duplicate detection
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_DUPLICATE_DETECTION_HEADER
#define THRILL_CORE_DUPLICATE_DETECTION_HEADER

#include <thrill/api/context.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/delta_stream.hpp>
#include <thrill/core/golomb_bit_stream.hpp>
#include <thrill/core/multiway_merge.hpp>

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

namespace thrill {
namespace core {

/*!
 * Duplicate detection to identify all elements occuring only on one worker.
 * This information can be used to locally reduce uniquely-occuring elements.
 * Therefore this saves communication volume in operations such as api::Reduce()
 * or api::Join().
 *
 * Internally, this duplicate detection uses a golomb encoded distributed single
 * shot bloom filter to find duplicates and non-duplicates with as low
 * communication volume as possible. Due to the bloom filter's inherent
 * properties, this has false duplicates but no false non-duplicates.
 *
 * Should only be used when a large amount of uniquely-occuring elements are
 * expected.
 */
class DuplicateDetection
{
    static constexpr bool debug = false;

private:
    using GolombBitStreamWriter =
              core::GolombBitStreamWriter<data::CatStream::Writer>;

    using GolombBitStreamReader =
              core::GolombBitStreamReader<data::CatStream::Reader>;

    using GolumbDeltaWriter =
              core::DeltaStreamWriter<GolombBitStreamWriter, size_t, /* offset */ 1>;

    using GolumbDeltaReader =
              core::DeltaStreamReader<GolombBitStreamReader, size_t, /* offset */ 1>;

    /*!
     * Sends all hashes in the range
     * [max_hash / num_workers * p, max_hash / num_workers * (p + 1)) to worker
     * p. These hashes are encoded with a Golomb encoder in core.
     *
     * \param stream_pointer Pointer to data stream
     * \param hashes Sorted vector of all hashes modulo max_hash
     * \param golomb_param Golomb parameter
     * \param num_workers Number of workers in this Thrill process
     * \param max_hash Modulo for all hashes
     */
    void WriteEncodedHashes(const data::CatStreamPtr& stream_pointer,
                            const std::vector<size_t>& hashes,
                            size_t golomb_param,
                            size_t num_workers,
                            size_t max_hash) {

        std::vector<data::CatStream::Writer> writers =
            stream_pointer->GetWriters();

        size_t prev_hash = size_t(-1);

        for (size_t i = 0, j = 0; i < num_workers; ++i) {
            common::Range range_i =
                common::CalculateLocalRange(max_hash, num_workers, i);

            GolombBitStreamWriter golomb_writer(writers[i], golomb_param);
            GolumbDeltaWriter delta_writer(
                golomb_writer,
                /* initial */ size_t(-1) /* cancels with +1 bias */);

            for (   /* j is already set from previous workers */
                ; j < hashes.size() && hashes[j] < range_i.end; ++j) {
                // Send hash deltas to make the encoded bitset smaller.
                if (hashes[j] == prev_hash)
                    continue;
                delta_writer.Put(hashes[j]);
                prev_hash = hashes[j];
            }
        }
    }

    /*!
     * Reads a golomb encoded bitset from a data stream and returns it's
     * contents in form of a vector of hashes.
     *
     * \param stream_pointer Pointer to data stream
     * \param non_duplicates Target vector for hashes, should be empty beforehand
     * \param golomb_param Golomb parameter
     */
    void ReadEncodedHashesToVector(const data::CatStreamPtr& stream_pointer,
                                   std::vector<bool>& non_duplicates,
                                   size_t golomb_param) {

        std::vector<data::CatStream::Reader> readers =
            stream_pointer->GetReaders();

        for (data::CatStream::Reader& reader : readers)
        {
            GolombBitStreamReader golomb_reader(reader, golomb_param);
            GolumbDeltaReader delta_reader(
                golomb_reader, /* initial */ size_t(-1) /* cancels at +1 */);

            // Builds golomb encoded bitset from data received by the stream.
            while (delta_reader.HasNext()) {
                // Golomb code contains deltas, we want the actual values
                size_t hash = delta_reader.Next<size_t>();
                assert(hash < non_duplicates.size());
                non_duplicates[hash] = true;
            }
        }
    }

public:
    /*!
     * Identifies all hashes which occur on only a single worker.
     * Returns all local uniques in form of a vector of hashes.
     *
     * \param non_duplicates Empty vector, which contains all non-duplicate
     * hashes after this method
     * \param hashes Hashes for all elements on this worker.
     * \param context Thrill context, used for collective communication
     * \param dia_id Id of the operation, which calls this method. Used
     *   to uniquely identify the data streams used.
     *
     * \return Modulo used on all hashes. (Use this modulo on all hashes to
     *  identify possible non-duplicates)
     */
    size_t FindNonDuplicates(std::vector<bool>& non_duplicates,
                             std::vector<size_t>& hashes,
                             Context& context,
                             size_t dia_id) {

        // This bound could often be lowered when we have many duplicates.
        // This would however require a large amount of added communication.
        size_t upper_bound_uniques = context.net.AllReduce(hashes.size());

        // Golomb Parameters taken from original paper (Sanders, Schlag, Müller)

        // Parameter for false positive rate (FPR: 1/fpr_parameter)
        double fpr_parameter = 8;
        size_t golomb_param = (size_t)fpr_parameter;  //(size_t)(std::log(2) * fpr_parameter);
        size_t max_hash = upper_bound_uniques * fpr_parameter;

        for (size_t i = 0; i < hashes.size(); ++i) {
            hashes[i] = hashes[i] % max_hash;
        }

        std::sort(hashes.begin(), hashes.end());

        data::CatStreamPtr golomb_data_stream = context.GetNewCatStream(dia_id);

        WriteEncodedHashes(golomb_data_stream,
                           hashes, golomb_param,
                           context.num_workers(),
                           max_hash);

        // get inbound Golomb/delta-encoded hash stream

        std::vector<data::CatStream::Reader> readers =
            golomb_data_stream->GetReaders();

        std::vector<GolombBitStreamReader> g_readers;
        std::vector<GolumbDeltaReader> delta_readers;
        g_readers.reserve(context.num_workers());
        delta_readers.reserve(context.num_workers());

        for (auto& reader : readers) {
            g_readers.emplace_back(reader, golomb_param);
            delta_readers.emplace_back(
                g_readers.back(),
                /* initial */ size_t(-1) /* cancels with +1 bias */);
        }

        // multi-way merge hash streams and detect duplicates/notify uniques

        auto puller = make_multiway_merge_tree<size_t>(
            delta_readers.begin(), delta_readers.end());

        // create streams (delta/Golomb encoded) to notify workers of duplicates

        data::CatStreamPtr duplicates_stream = context.GetNewCatStream(dia_id);

        std::vector<data::CatStream::Writer> duplicates_writers =
            duplicates_stream->GetWriters();

        std::vector<GolombBitStreamWriter> duplicates_gbsw;
        std::vector<GolumbDeltaWriter> duplicates_dw;
        duplicates_gbsw.reserve(context.num_workers());
        duplicates_dw.reserve(context.num_workers());

        for (size_t i = 0; i < context.num_workers(); ++i) {
            duplicates_gbsw.emplace_back(duplicates_writers[i], golomb_param);
            duplicates_dw.emplace_back(
                duplicates_gbsw.back(),
                /* initial */ size_t(-1) /* cancels with +1 bias */);
        }

        // find all keys only occuring on a single worker and insert to
        // according bitset

        if (puller.HasNext())
        {
            std::pair<size_t, size_t> this_item;
            std::pair<size_t, size_t> next_item = puller.NextWithSource();

            while (puller.HasNext())
            {
                this_item = next_item;
                next_item = puller.NextWithSource();

                if (this_item.first != next_item.first) {
                    // this_item is a unique
                    sLOG << "!" << this_item.first << "->" << this_item.second;
                    duplicates_dw[this_item.second].Put(this_item.first);
                }
                else {
                    // this_item is a duplicate with next_item
                    sLOG << "=" << this_item.first << "-" << this_item.second;

                    // read more items into next_item until the key mismatches
                    while (puller.HasNext() &&
                           (next_item = puller.NextWithSource(),
                            next_item.first == this_item.first))
                    {
                        sLOG << "." << next_item.first << "-" << next_item.second;
                    }
                }
            }

            if (this_item.first != next_item.first) {
                // last item (next_item) is a unique
                sLOG << "!" << next_item.first << "->" << next_item.second;
                duplicates_dw[next_item.second].Put(next_item.first);
            }
        }

        // close duplicate delta writers
        duplicates_dw.clear();
        duplicates_gbsw.clear();
        duplicates_writers.clear();

        // read inbound duplicate hash bits into non_duplicates hash table
        assert(non_duplicates.size() == 0);
        non_duplicates.resize(max_hash);
        ReadEncodedHashesToVector(
            duplicates_stream, non_duplicates, golomb_param);

        return max_hash;
    }
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_DUPLICATE_DETECTION_HEADER

/******************************************************************************/
