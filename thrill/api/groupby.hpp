/*******************************************************************************
 * thrill/api/groupby.hpp
 *
 * DIANode for a groupby operation. Performs the actual groupby operation
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Huyen Chau Nguyen <hello@chau-nguyen.de>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_GROUPBY_HEADER
#define THRILL_API_GROUPBY_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/api/groupby_iterator.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/iterator_wrapper.hpp>
#include <thrill/core/multiway_merge.hpp>

#include <algorithm>
#include <functional>
#include <string>
#include <type_traits>
#include <typeinfo>
#include <utility>
#include <vector>

namespace thrill {
namespace api {

template <typename ValueType, typename ParentDIA,
          typename KeyExtractor, typename GroupFunction, typename HashFunction>
class GroupByNode : public DOpNode<ValueType>
{
    static const bool debug = false;
    using Super = DOpNode<ValueType>;
    using Key = typename common::FunctionTraits<KeyExtractor>::result_type;
    using ValueOut = ValueType;
    using ValueIn = typename std::decay<typename common::FunctionTraits<KeyExtractor>
                                        ::template arg<0> >::type;

    using File = data::File;
    using Reader = typename File::Reader;
    using Writer = typename File::Writer;

    struct ValueComparator
    {
        explicit ValueComparator(const GroupByNode& info) : info_(info) { }
        const GroupByNode& info_;

        bool operator () (const ValueIn& i,
                          const ValueIn& j) {
            // REVIEW(ch): why is this a comparison by hash key? hash can have
            // collisions.
            auto i_cmp = info_.hash_function_(info_.key_extractor_(i));
            auto j_cmp = info_.hash_function_(info_.key_extractor_(j));
            return (i_cmp < j_cmp);
        }
    };

    using Super::context_;

public:
    /*!
     * Constructor for a GroupByNode. Sets the DataManager, parent, stack,
     * key_extractor and reduce_function.
     *
     * \param parent Parent DIA.
     * and this node
     * \param key_extractor Key extractor function
     * \param reduce_function Reduce function
     */
    GroupByNode(const ParentDIA& parent,
                const KeyExtractor& key_extractor,
                const GroupFunction& groupby_function,
                StatsNode* stats_node,
                const HashFunction& hash_function = HashFunction())
        : DOpNode<ValueType>(parent.ctx(), { parent.node() }, stats_node),
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
        auto lop_chain = parent.stack().push(pre_op_fn).emit();
        parent.node()->RegisterChild(lop_chain, this->type());
        stream_->OnClose([this]() {
                             this->WriteStreamStats(this->stream_);
                         });
    }

    //! Virtual destructor for a GroupByNode.
    virtual ~GroupByNode() { }

    /*!
     * Actually executes the reduce operation. Uses the member functions PreOp,
     * MainOp and PostOp.
     */
    void Execute() override {
        MainOp();
    }

    void PushData(bool consume) final {
        using Iterator = thrill::core::FileIteratorWrapper<ValueIn>;

        const size_t num_runs = files_.size();
        // if there's only one run, call user funcs
        if (num_runs == 1) {
            RunUserFunc(files_[0], consume);
        }       // otherwise sort all runs using multiway merge
        else {
            std::vector<std::pair<Iterator, Iterator> > seq;
            seq.reserve(num_runs);
            for (size_t t = 0; t < num_runs; ++t) {
                std::shared_ptr<Reader> reader = std::make_shared<Reader>(
                    files_[t].GetReader(consume));
                Iterator s = Iterator(&files_[t], reader, 0, true);
                Iterator e = Iterator(&files_[t], reader, files_[t].num_items(), false);
                seq.push_back(std::make_pair(std::move(s), std::move(e)));
            }

            auto puller = core::get_sequential_file_multiway_merge_tree<true, false>(
                seq.begin(),
                seq.end(),
                totalsize_,
                ValueComparator(*this));

            if (puller.HasNext()) {
                // create iterator to pass to user_function
                auto user_iterator = GroupByMultiwayMergeIterator<
                    ValueIn, KeyExtractor, ValueComparator>(puller, key_extractor_);
                while (user_iterator.HasNextForReal()) {
                    // call user function
                    const ValueOut res = groupby_function_(
                        user_iterator, user_iterator.GetNextKey());
                    // push result to callback functions
                    this->PushItem(res);
                }
            }
        }
    }

    void Dispose() override { }

    /*!
     * Produces a function stack, which only contains the PostOp function.
     * \return PostOp function stack
     */
    auto ProduceStack() {
        return FunctionStack<ValueType>();
    }

private:
    const KeyExtractor& key_extractor_;
    const GroupFunction& groupby_function_;
    const HashFunction& hash_function_;

    data::CatStreamPtr stream_ { context_.GetNewCatStream() };
    std::vector<data::Stream::Writer> emitter_ { stream_->OpenWriters() };
    std::vector<data::File> files_;
    data::File sorted_elems_ { context_.GetFile() };
    size_t totalsize_ = 0;

    void RunUserFunc(File& f, bool consume) {
        auto r = f.GetReader(consume);
        if (r.HasNext()) {
            // create iterator to pass to user_function
            auto user_iterator = GroupByIterator<ValueIn, KeyExtractor, ValueComparator>(r, key_extractor_);
            while (user_iterator.HasNextForReal()) {
                // call user function
                const ValueOut res = groupby_function_(user_iterator,
                                                       user_iterator.GetNextKey());
                // push result to callback functions
                this->PushItem(res);
            }
        }
    }

    /*
     * Send all elements to their designated PEs
     */
    void PreOp(const ValueIn& v) {
        const Key k = key_extractor_(v);
        const auto recipient = hash_function_(k) % emitter_.size();
        emitter_[recipient](v);
    }

    /*
     * Sort and store elements in a file
     */
    void FlushVectorToFile(std::vector<ValueIn>& v) {
        totalsize_ += v.size();

        // sort run and sort to file
        std::sort(v.begin(), v.end(), ValueComparator(*this));
        File f = context_.GetFile();
        {
            Writer w = f.GetWriter();
            for (const ValueIn& e : v) {
                w(e);
            }
            w.Close();
        }

        files_.emplace_back(std::move(f));
    }

    //! Receive elements from other workers.
    auto MainOp() {
        LOG << "running group by main op";

        const size_t FIXED_VECTOR_SIZE = 1000000000 / sizeof(ValueIn);
        // const size_t FIXED_VECTOR_SIZE = 4;
        std::vector<ValueIn> incoming;
        incoming.reserve(FIXED_VECTOR_SIZE);

        // close all emitters
        for (auto& e : emitter_) {
            e.Close();
        }

        // get incoming elements
        auto reader = stream_->OpenCatReader(true /* consume */);
        while (reader.HasNext()) {
            // if vector is full save to disk
            if (incoming.size() == FIXED_VECTOR_SIZE) {
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

/******************************************************************************/

template <typename ValueType, typename Stack>
template <typename ValueOut,
          typename KeyExtractor,
          typename GroupFunction,
          typename HashFunction>
auto DIA<ValueType, Stack>::GroupBy(
    const KeyExtractor &key_extractor,
    const GroupFunction &groupby_function) const {

    using DOpResult = ValueOut;

    static_assert(
        std::is_same<
            typename std::decay<typename common::FunctionTraits<KeyExtractor>
                                ::template arg<0> >::type,
            ValueType>::value,
        "KeyExtractor has the wrong input type");

    StatsNode* stats_node = AddChildStatsNode("GroupBy", DIANodeType::DOP);
    using GroupByNode
              = api::GroupByNode<DOpResult, DIA, KeyExtractor,
                                 GroupFunction, HashFunction>;
    auto shared_node
        = std::make_shared<GroupByNode>(
        *this, key_extractor, groupby_function, stats_node);

    auto groupby_stack = shared_node->ProduceStack();

    return DIA<DOpResult, decltype(groupby_stack)>(
        shared_node, groupby_stack, { stats_node });
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_GROUPBY_HEADER

/******************************************************************************/
