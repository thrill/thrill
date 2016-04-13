/*******************************************************************************
 * thrill/api/group_by_key.hpp
 *
 * DIANode for a groupby operation. Performs the actual groupby operation
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Huyen Chau Nguyen <hello@chau-nguyen.de>
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

#include <algorithm>
#include <functional>
#include <type_traits>
#include <typeinfo>
#include <utility>
#include <vector>

namespace thrill {
namespace api {

/*!
 * \ingroup api_layer
 */
template <typename ValueType, typename ParentDIA,
          typename KeyExtractor, typename GroupFunction, typename HashFunction>
class GroupByNode final : public DOpNode<ValueType>
{
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

public:
    /*!
     * Constructor for a GroupByNode. Sets the DataManager, parent, stack,
     * key_extractor and reduce_function.
     */
    GroupByNode(const ParentDIA& parent,
                const KeyExtractor& key_extractor,
                const GroupFunction& groupby_function,
                const HashFunction& hash_function = HashFunction())
        : Super(parent.ctx(), "GroupByKey", { parent.id() }, { parent.node() }),
          key_extractor_(key_extractor),
          groupby_function_(groupby_function),
          hash_function_(hash_function)
    {
        // Hook PreOp
        auto pre_op_fn = [=](const ValueIn& input) {
                             PreOp(input);
                         };
        // close the function stack with our pre op and register it at
        // parent node for output
        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    void StartPreOp(size_t /* id */) final {
        emitter_ = stream_->GetWriters();
    }

    //! Send all elements to their designated PEs
    void PreOp(const ValueIn& v) {
        const Key k = key_extractor_(v);
        const size_t recipient = hash_function_(k) % emitter_.size();
        emitter_[recipient].Put(v);
    }

    void StopPreOp(size_t /* id */) final {
        // data has been pushed during pre-op -> close emitters
        for (size_t i = 0; i < emitter_.size(); i++)
            emitter_[i].Close();
    }

    void Execute() override {
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
            LOG << "start multiwaymerge";
            std::vector<data::File::ConsumeReader> seq;
            seq.reserve(num_runs);

            for (size_t t = 0; t < num_runs; ++t)
                seq.emplace_back(files_[t].GetConsumeReader());

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
        LOG1 << "RESULT"
             << " name=multiwaymerge"
             << " time=" << timer.Milliseconds()
             << " multiwaymerge=" << (num_runs > 1);
    }

    void Dispose() override { }

private:
    KeyExtractor key_extractor_;
    GroupFunction groupby_function_;
    HashFunction hash_function_;

    data::CatStreamPtr stream_ { context_.GetNewCatStream(this) };
    std::vector<data::Stream::Writer> emitter_;
    std::vector<data::File> files_;
    data::File sorted_elems_ { context_.GetFile(this) };
    size_t totalsize_ = 0;

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

        data::File f = context_.GetFile(this);
        data::File::Writer w = f.GetWriter();
        for (const ValueIn& e : v) {
            w.Put(e);
        }
        w.Close();

        files_.emplace_back(std::move(f));
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
        std::vector<ValueIn>().swap(incoming);
        LOG << "finished receiving elems";
        stream_->Close();

        timer.Stop();

        LOG1 << "RESULT"
             << " name=mainop"
             << " time=" << timer
             << " number_files=" << files_.size();
    }
};

/******************************************************************************/

template <typename ValueType, typename Stack>
template <typename ValueOut, typename KeyExtractor,
          typename GroupFunction, typename HashFunction>
auto DIA<ValueType, Stack>::GroupByKey(
    const KeyExtractor &key_extractor,
    const GroupFunction &groupby_function) const {

    using DOpResult = ValueOut;

    static_assert(
        std::is_same<
            typename std::decay<typename common::FunctionTraits<KeyExtractor>
                                ::template arg<0> >::type,
            ValueType>::value,
        "KeyExtractor has the wrong input type");

    using GroupByNode = api::GroupByNode<
              DOpResult, DIA, KeyExtractor, GroupFunction, HashFunction>;

    auto node = common::MakeCounting<GroupByNode>(
        *this, key_extractor, groupby_function);

    return DIA<DOpResult>(node);
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_GROUP_BY_KEY_HEADER

/******************************************************************************/
