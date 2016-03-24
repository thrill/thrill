/*******************************************************************************
 * thrill/api/group_to_index.hpp
 *
 * DIANode for a groupby to indx operation. Performs the actual groupby
 * operation
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Huyen Chau Nguyen <hello@chau-nguyen.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_GROUP_TO_INDEX_HEADER
#define THRILL_API_GROUP_TO_INDEX_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/api/group_by_iterator.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>

#include <algorithm>
#include <functional>
#include <string>
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
          typename KeyExtractor, typename GroupFunction>
class GroupToIndexNode final : public DOpNode<ValueType>
{
    static constexpr bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

    using Key = typename common::FunctionTraits<KeyExtractor>::result_type;
    using ValueOut = ValueType;
    using ValueIn =
              typename common::FunctionTraits<KeyExtractor>::template arg_plain<0>;

    class ValueComparator
    {
    public:
        explicit ValueComparator(const GroupToIndexNode& node) : node_(node) { }

        bool operator () (const ValueIn& a, const ValueIn& b) const {
            return node_.key_extractor_(a) < node_.key_extractor_(b);
        }

    private:
        const GroupToIndexNode& node_;
    };

public:
    /*!
     * Constructor for a GroupToIndexNode. Sets the DataManager, parent, stack,
     * key_extractor and reduce_function.
     */
    GroupToIndexNode(const ParentDIA& parent,
                     const KeyExtractor& key_extractor,
                     const GroupFunction& groupby_function,
                     size_t result_size,
                     const ValueOut& neutral_element)
        : Super(parent.ctx(), "GroupToIndex",
                { parent.id() }, { parent.node() }),
          key_extractor_(key_extractor),
          groupby_function_(groupby_function),
          result_size_(result_size),
          key_range_(
              common::CalculateLocalRange(
                  result_size_, context_.num_workers(), context_.my_rank())),
          neutral_element_(neutral_element)
    {
        // Hook PreOp
        auto pre_op_fn = [this](const ValueIn& input) {
                             PreOp(input);
                         };
        // close the function stack with our pre op and register it at
        // parent node for output
        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    //! Send all elements to their designated PEs
    void PreOp(const ValueIn& v) {
        const Key k = key_extractor_(v);
        assert(k < result_size_);
        const size_t recipient = k * emitter_.size() / result_size_;
        assert(recipient < emitter_.size());
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
        sLOG1 << "GroupToIndexNode::PushData()";

        const size_t num_runs = files_.size();
        if (num_runs == 0) {
            // nothing to push
        }
        else if (num_runs == 1) {
            // if there's only one run, store it
            RunUserFunc(files_[0], consume);
        }
        else {
            // otherwise sort all runs using multiway merge
            std::vector<data::File::ConsumeReader> seq;
            seq.reserve(num_runs);

            for (size_t t = 0; t < num_runs; ++t)
                seq.emplace_back(files_[t].GetConsumeReader());

            auto puller = core::make_multiway_merge_tree<ValueIn>(
                seq.begin(), seq.end(), ValueComparator(*this));

            size_t curr_index = key_range_.begin;
            if (puller.HasNext()) {
                // create iterator to pass to user_function
                auto user_iterator = GroupByMultiwayMergeIterator<
                    ValueIn, KeyExtractor, ValueComparator>(
                    puller, key_extractor_);

                while (user_iterator.HasNextForReal()) {
                    if (user_iterator.GetNextKey() != curr_index) {
                        // push neutral element as result to callback functions
                        this->PushItem(neutral_element_);
                    }
                    else {
                        // call user function
                        const ValueOut res = groupby_function_(
                            user_iterator, user_iterator.GetNextKey());
                        // push result to callback functions
                        this->PushItem(res);
                    }
                    ++curr_index;
                }
            }
            while (curr_index < key_range_.end) {
                // push neutral element as result to callback functions
                this->PushItem(neutral_element_);
                ++curr_index;
            }
        }
    }

    void Dispose() override { }

private:
    KeyExtractor key_extractor_;
    GroupFunction groupby_function_;
    const size_t result_size_;
    const common::Range key_range_;
    ValueOut neutral_element_;
    size_t totalsize_ = 0;

    data::CatStreamPtr stream_ { context_.GetNewCatStream(this) };
    std::vector<data::CatStream::Writer> emitter_ { stream_->GetWriters() };
    std::vector<data::File> files_;

    void RunUserFunc(data::File& f, bool consume) {
        auto r = f.GetReader(consume);
        if (r.HasNext()) {
            // create iterator to pass to user_function
            auto user_iterator = GroupByIterator<
                ValueIn, KeyExtractor, ValueComparator>(r, key_extractor_);
            size_t curr_index = key_range_.begin;
            while (user_iterator.HasNextForReal()) {
                if (user_iterator.GetNextKey() != curr_index) {
                    // push neutral element as result to callback functions
                    this->PushItem(neutral_element_);
                }
                else {
                    // push result to callback functions
                    this->PushItem(
                        // call user function
                        groupby_function_(user_iterator,
                                          user_iterator.GetNextKey()));
                }
                ++curr_index;
            }
            while (curr_index < key_range_.end) {
                // push neutral element as result to callback functions
                this->PushItem(neutral_element_);
                ++curr_index;
            }
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
        LOG << "Running GroupBy MainOp";

        std::vector<ValueIn> incoming;

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

        stream_->Close();
    }
};

template <typename ValueType, typename Stack>
template <typename ValueOut,
          typename KeyExtractor,
          typename GroupFunction>
auto DIA<ValueType, Stack>::GroupToIndex(
    const KeyExtractor &key_extractor,
    const GroupFunction &groupby_function,
    const size_t result_size,
    const ValueOut &neutral_element) const {

    using DOpResult
              = ValueOut;

    static_assert(
        std::is_same<
            typename std::decay<typename common::FunctionTraits<KeyExtractor>
                                ::template arg<0> >::type,
            ValueType>::value,
        "KeyExtractor has the wrong input type");

    using GroupByNode
              = GroupToIndexNode<DOpResult, DIA, KeyExtractor, GroupFunction>;
    auto shared_node
        = std::make_shared<GroupByNode>(
        *this, key_extractor, groupby_function, result_size, neutral_element);

    return DIA<DOpResult>(shared_node);
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_GROUP_TO_INDEX_HEADER

/******************************************************************************/
