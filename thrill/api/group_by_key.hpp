/*******************************************************************************
 * thrill/api/group_by_key.hpp
 *
 * DIANode for a groupby operation. Performs the actual groupby operation
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Huyen Chau Nguyen <hello@chau-nguyen.de>
 * Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_GROUP_BY_KEY_HEADER
#define THRILL_API_GROUP_BY_KEY_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/api/group_by_iterator.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/location_detection.hpp>
#include <thrill/core/reduce_functional.hpp>
#include <thrill/data/file.hpp>

#include <tlx/vector_free.hpp>

#include <algorithm>
#include <deque>
#include <functional>
#include <type_traits>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <vector>

namespace thrill {
namespace api {

/*!
 * \ingroup api_layer
 */
template <typename ValueType,
          typename KeyExtractor, typename GroupFunction, typename HashFunction,
          bool UseLocationDetection>
class GroupByNode final : public DOpNode<ValueType>
{
private:
    static constexpr bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

    using Key = typename common::FunctionTraits<KeyExtractor>::result_type;
    using ValueOut = ValueType;
    using ValueIn =
        typename common::FunctionTraits<KeyExtractor>::template arg_plain<0>;

    struct ValueComparator {
    public:
        explicit ValueComparator(const GroupByNode& node) : node_(node) { }

        bool operator () (const ValueIn& a, const ValueIn& b) const {
            return node_.key_extractor_(a) < node_.key_extractor_(b);
        }

    private:
        const GroupByNode& node_;
    };

    class HashCount
    {
    public:
        using HashType = size_t;
        using CounterType = uint8_t;

        size_t hash;
        CounterType count;

        static constexpr size_t counter_bits_ = 8 * sizeof(CounterType);

        HashCount operator + (const HashCount& b) const {
            assert(hash == b.hash);
            return HashCount { hash, common::AddTruncToType(count, b.count) };
        }

        HashCount& operator += (const HashCount& b) {
            assert(hash == b.hash);
            count = common::AddTruncToType(count, b.count);
            return *this;
        }

        bool operator < (const HashCount& b) const { return hash < b.hash; }

        //! method to check if this hash count should be broadcasted to all
        //! workers interested -- for GroupByKey -> always.
        bool NeedBroadcast() const {
            return true;
        }

        //! Read count from BitReader
        template <typename BitReader>
        void ReadBits(BitReader& reader) {
            count = reader.GetBits(counter_bits_);
        }

        //! Write count and dia_mask to BitWriter
        template <typename BitWriter>
        void WriteBits(BitWriter& writer) const {
            writer.PutBits(count, counter_bits_);
        }
    };

public:
    /*!
     * Constructor for a GroupByNode. Sets the DataManager, parent, stack,
     * key_extractor and reduce_function.
     */
    template <typename ParentDIA>
    GroupByNode(const ParentDIA& parent,
                const KeyExtractor& key_extractor,
                const GroupFunction& groupby_function,
                const HashFunction& hash_function = HashFunction())
        : Super(parent.ctx(), "GroupByKey", { parent.id() }, { parent.node() }),
          key_extractor_(key_extractor),
          groupby_function_(groupby_function),
          hash_function_(hash_function),
          location_detection_(parent.ctx(), Super::dia_id()),
          pre_file_(context_.GetFile(this)) {
        // Hook PreOp
        auto pre_op_fn = [=](const ValueIn& input) {
                             PreOp(input);
                         };
        // close the function stack with our pre op and register it at
        // parent node for output
        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    void StartPreOp(size_t /* parent_index */) final {
        emitters_ = stream_->GetWriters();
        pre_writer_ = pre_file_.GetWriter();
        if (UseLocationDetection)
            location_detection_.Initialize(DIABase::mem_limit_);
    }

    //! Send all elements to their designated PEs
    void PreOp(const ValueIn& v) {
        size_t hash = hash_function_(key_extractor_(v));
        if (UseLocationDetection) {
            pre_writer_.Put(v);
            location_detection_.Insert(HashCount { hash, 1 });
        }
        else {
            const size_t recipient = hash % emitters_.size();
            emitters_[recipient].Put(v);
        }
    }

    void StopPreOp(size_t /* parent_index */) final {
        pre_writer_.Close();
    }

    DIAMemUse PreOpMemUse() final {
        return DIAMemUse::Max();
    }

    DIAMemUse ExecuteMemUse() final {
        return DIAMemUse::Max();
    }

    DIAMemUse PushDataMemUse() final {
        if (files_.size() <= 1) {
            // direct push, no merge necessary
            return 0;
        }
        else {
            // need to perform multiway merging
            return DIAMemUse::Max();
        }
    }

    void Execute() override {
        if (UseLocationDetection) {
            std::unordered_map<size_t, size_t> target_processors;
            size_t max_hash = location_detection_.Flush(target_processors);
            auto file_reader = pre_file_.GetConsumeReader();
            while (file_reader.HasNext()) {
                ValueIn in = file_reader.template Next<ValueIn>();
                Key key = key_extractor_(in);

                size_t hr = hash_function_(key) % max_hash;
                auto target_processor = target_processors.find(hr);
                emitters_[target_processor->second].Put(in);
            }
        }
        // data has been pushed during pre-op -> close emitters
        emitters_.Close();

        MainOp();
    }

    void PushData(bool consume) final {
        LOG << "sort data";
        common::StatsTimerStart timer;
        const size_t num_runs = files_.size();
        if (num_runs == 0) {
            // nothing to push
        }
        else if (num_runs == 1) {
            // if there's only one run, call user funcs
            RunUserFunc(files_[0], consume);
        }
        else {
            // otherwise sort all runs using multiway merge
            size_t merge_degree, prefetch;

            // merge batches of files if necessary
            while (std::tie(merge_degree, prefetch) =
                       context_.block_pool().MaxMergeDegreePrefetch(files_.size()),
                   files_.size() > merge_degree)
            {
                sLOG1 << "Partial multi-way-merge of"
                      << merge_degree << "files with prefetch" << prefetch;

                // create merger for first merge_degree_ Files
                std::vector<data::File::ConsumeReader> seq;
                seq.reserve(merge_degree);

                for (size_t t = 0; t < merge_degree; ++t) {
                    seq.emplace_back(
                        files_[t].GetConsumeReader(/* prefetch */ 0));
                }

                StartPrefetch(seq, prefetch);

                auto puller = core::make_multiway_merge_tree<ValueIn>(
                    seq.begin(), seq.end(), ValueComparator(*this));

                // create new File for merged items
                files_.emplace_back(context_.GetFile(this));
                auto writer = files_.back().GetWriter();

                while (puller.HasNext()) {
                    writer.Put(puller.Next());
                }
                writer.Close();

                // this clear is important to release references to the files.
                seq.clear();

                // remove merged files
                files_.erase(files_.begin(), files_.begin() + merge_degree);
            }

            std::vector<data::File::Reader> seq;
            seq.reserve(num_runs);

            for (size_t t = 0; t < num_runs; ++t) {
                seq.emplace_back(
                    files_[t].GetReader(consume, /* prefetch */ 0));
            }

            StartPrefetch(seq, prefetch);

            LOG << "start multiwaymerge for real";
            auto puller = core::make_multiway_merge_tree<ValueIn>(
                seq.begin(), seq.end(), ValueComparator(*this));

            LOG << "run user func";
            if (puller.HasNext()) {
                // create iterator to pass to user_function
                auto user_iterator = GroupByMultiwayMergeIterator<
                    ValueIn, KeyExtractor, ValueComparator>(
                    puller, key_extractor_);

                while (user_iterator.HasNextForReal()) {
                    // call user function
                    const ValueOut res = groupby_function_(
                        user_iterator, user_iterator.GetNextKey());
                    // push result to callback functions
                    this->PushItem(res);
                }
            }
        }
        timer.Stop();
        LOG << "RESULT"
            << " name=multiwaymerge"
            << " time=" << timer
            << " multiwaymerge=" << (num_runs > 1);
    }

    void Dispose() override { }

private:
    KeyExtractor key_extractor_;
    GroupFunction groupby_function_;
    HashFunction hash_function_;

    core::LocationDetection<HashCount> location_detection_;

    data::CatStreamPtr stream_ { context_.GetNewCatStream(this) };
    data::CatStream::Writers emitters_;

    std::deque<data::File> files_;
    data::File sorted_elems_ { context_.GetFile(this) };
    size_t totalsize_ = 0;

    //! location detection and associated files
    data::File pre_file_;
    data::File::Writer pre_writer_;

    void RunUserFunc(data::File& f, bool consume) {
        auto r = f.GetReader(consume);
        if (r.HasNext()) {
            // create iterator to pass to user_function
            LOG << "get iterator";
            auto user_iterator = GroupByIterator<
                ValueIn, KeyExtractor, ValueComparator>(r, key_extractor_);
            LOG << "start running user func";
            while (user_iterator.HasNextForReal()) {
                // call user function
                const ValueOut res = groupby_function_(user_iterator,
                                                       user_iterator.GetNextKey());
                // push result to callback functions
                this->PushItem(res);
            }
            LOG << "finished user func";
        }
    }

    //! Sort and store elements in a file
    void FlushVectorToFile(std::vector<ValueIn>& v) {
        // sort run and sort to file
        std::sort(v.begin(), v.end(), ValueComparator(*this));
        totalsize_ += v.size();

        files_.emplace_back(context_.GetFile(this));
        data::File::Writer w = files_.back().GetWriter();
        for (const ValueIn& e : v) {
            w.Put(e);
        }
        w.Close();
    }

    //! Receive elements from other workers.
    void MainOp() {
        LOG << "running group by main op";

        std::vector<ValueIn> incoming;

        common::StatsTimerStart timer;
        // get incoming elements
        auto reader = stream_->GetCatReader(/* consume */ true);
        while (reader.HasNext()) {
            // if vector is full save to disk
            if (mem::memory_exceeded) {
                FlushVectorToFile(incoming);
                incoming.clear();
            }
            // store incoming element
            incoming.emplace_back(reader.template Next<ValueIn>());
        }
        FlushVectorToFile(incoming);
        tlx::vector_free(incoming);
        LOG << "finished receiving elems";
        stream_.reset();

        timer.Stop();

        LOG << "RESULT"
            << " name=mainop"
            << " time=" << timer
            << " number_files=" << files_.size();
    }
};

/******************************************************************************/

template <typename ValueType, typename Stack>
template <typename ValueOut, bool LocationDetectionValue,
          typename KeyExtractor, typename GroupFunction, typename HashFunction>
auto DIA<ValueType, Stack>::GroupByKey(
    const LocationDetectionFlag<LocationDetectionValue>&,
    const KeyExtractor& key_extractor,
    const GroupFunction& groupby_function,
    const HashFunction& hash_function) const {

    static_assert(
        std::is_same<
            typename std::decay<typename common::FunctionTraits<KeyExtractor>
                                ::template arg<0> >::type,
            ValueType>::value,
        "KeyExtractor has the wrong input type");

    using GroupByNode = api::GroupByNode<
        ValueOut, KeyExtractor, GroupFunction, HashFunction,
        LocationDetectionValue>;

    auto node = tlx::make_counting<GroupByNode>(
        *this, key_extractor, groupby_function, hash_function);

    return DIA<ValueOut>(node);
}

template <typename ValueType, typename Stack>
template <typename ValueOut, typename KeyExtractor,
          typename GroupFunction, typename HashFunction>
auto DIA<ValueType, Stack>::GroupByKey(
    const KeyExtractor& key_extractor,
    const GroupFunction& groupby_function,
    const HashFunction& hash_function) const {
    // forward to other method _without_ location detection
    return GroupByKey<ValueOut>(
        NoLocationDetectionTag, key_extractor, groupby_function, hash_function);
}

template <typename ValueType, typename Stack>
template <typename ValueOut, typename KeyExtractor, typename GroupFunction>
auto DIA<ValueType, Stack>::GroupByKey(
    const KeyExtractor& key_extractor,
    const GroupFunction& groupby_function) const {
    // forward to other method _without_ location detection
    return GroupByKey<ValueOut>(
        NoLocationDetectionTag, key_extractor, groupby_function,
        std::hash<typename FunctionTraits<KeyExtractor>::result_type>());
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_GROUP_BY_KEY_HEADER

/******************************************************************************/
