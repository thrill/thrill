/*******************************************************************************
 * thrill/api/groupby.hpp
 *
 * DIANode for a groupby operation. Performs the actual groupby operation
 *
 * Part of Project Thrill.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_GROUPBY_HEADER
#define THRILL_API_GROUPBY_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/iterator_wrapper.hpp>
#include <thrill/core/stxxl_multiway_merge.hpp>

#include <functional>
#include <string>
#include <type_traits>
#include <typeinfo>
#include <utility>
#include <vector>

namespace thrill {
namespace api {

template <typename ValueTypeIn>
class GroupByIterator {
public:
    GroupByIterator(data::File::Reader &r) : r_(r) {}

    bool HasNext() {
        return r_.HasNext();
    }

    ValueTypeIn Next() {
        return r_.template Next<ValueTypeIn>();
    }
private:
    data::File::Reader &r_;
};

template <typename ValueTypeIn, typename ParentDIARef,
          typename KeyExtractor, typename GroupFunction, typename HashFunction>
class GroupByNode : public DOpNode<ValueTypeIn>
{
    static const bool debug = false;
    using Super = DOpNode<ValueTypeIn>;
    using Key = typename common::FunctionTraits<KeyExtractor>::result_type;
    using ValueTypeOut = typename common::FunctionTraits<GroupFunction>::result_type;
    using ReduceArg = typename common::FunctionTraits<GroupFunction>
                      ::template arg<0>;
    using KeyValuePair = typename std::pair<Key, ValueTypeIn>;

    struct ValueComparator
    {
        ValueComparator(const GroupByNode& info) : info_(info) { }
        const GroupByNode& info_;

        bool operator () (const ValueTypeIn& i,
                          const ValueTypeIn& j) {
            auto i_cmp = info_.hash_function_(info_.key_extractor_(i));
            auto j_cmp = info_.hash_function_(info_.key_extractor_(j));
            return (i_cmp < j_cmp);
        }
    };
    // using Merger = typename stxxl::parallel::LoserTreeCopyBase<ValueTypeIn, ValueComparator>;

    using Super::context_;
    using Super::result_file_;

public:
    /*!
     * Constructor for a GroupByNode. Sets the DataManager, parent, stack,
     * key_extractor and reduce_function.
     *
     * \param parent Parent DIARef.
     * and this node
     * \param key_extractor Key extractor function
     * \param reduce_function Reduce function
     */
    GroupByNode(const ParentDIARef& parent,
                KeyExtractor key_extractor,
                GroupFunction groupby_function,
                StatsNode* stats_node,
                const HashFunction& hash_function = HashFunction())
        : DOpNode<ValueTypeIn>(parent.ctx(), { parent.node() }, "GroupBy", stats_node),
          key_extractor_(key_extractor),
          groupby_function_(groupby_function),
          hash_function_(hash_function),
          channel_(parent.ctx().GetNewChannel()),
          emitter_(channel_->OpenWriters())
    {
        // Hook PreOp
        auto pre_op_fn = [=](const ValueTypeIn& input) {
                             PreOp(input);
                         };
        // close the function stack with our pre op and register it at
        // parent node for output
        auto lop_chain = parent.stack().push(pre_op_fn).emit();
        parent.node()->RegisterChild(lop_chain, this->type());
        channel_->OnClose([this]() {
                              this->WriteChannelStats(this->channel_);
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

    void ProcessGroup(data::File& f) {
        auto r = f.GetReader();
        std::vector<std::function<void(const ValueTypeIn&)> > cbs;
        DIANode<ValueTypeIn>::callback_functions(cbs);
    }

    void PushData() override {
        // OMG THIS IS SO HACKY. THIS MUST BE POSSIBLE MORE ELEGANTLY
        // split sorted files to multiple files for each key
        std::vector<data::File> user_files;
        auto r = sorted_elems_.GetReader();
        if (r.HasNext()) {
            ValueTypeIn v1 = r.template Next<ValueTypeIn>();
            Key k1 = key_extractor_(v1);
            bool inserted = false;
            while (r.HasNext()) {
                user_files.push_back(data::File());
                {
                    auto w = user_files.back().GetWriter();
                    w(v1);
                    LOG << "Host " << context_.host_rank() << " added " << v1;
                    inserted = true;

                    while (inserted && r.HasNext()) {
                        ValueTypeIn v2 = r.template Next<ValueTypeIn>();
                        Key k2 = key_extractor_(v2);
                        inserted = false;
                        if (k2 == k1) {
                            w(v2);
                            LOG << "Host " << context_.host_rank() << " added " << v2;
                            inserted = true;
                        }
                        else {
                            v1 = v2;
                            k1 = k2;
                        }
                    }
                }
            }
        }

        // call user function
        for (auto t : user_files) {
            auto r = t.GetReader();
            data_.push_back(groupby_function_(GroupByIterator<ValueTypeIn>(r)));
        }

        // push data to callback functions
        for (size_t i = 0; i < data_.size(); i++) {
            for (auto func : DIANode<ValueTypeIn>::callbacks_) {
                LOG << "Host " << context_.host_rank() << " grouped to value " << data_[i];
                func(data_[i]);
            }
        }
    }

    void Dispose() override { }

    /*!
     * Produces a function stack, which only contains the PostOp function.
     * \return PostOp function stack
     */
    auto ProduceStack() {
        return FunctionStack<ValueTypeIn>();
    }

    /*!
     * Returns "[GroupByNode]" and its id as a string.
     * \return "[GroupByNode]"
     */
    std::string ToString() override {
        return "[GroupByNode] Id: " + result_file_.ToString();
    }

private:
    KeyExtractor key_extractor_;
    GroupFunction groupby_function_;
    HashFunction hash_function_;

    data::ChannelPtr channel_;
    std::vector<data::BlockWriter> emitter_;
    std::vector<data::File> files_;
    data::File sorted_elems_;
    std::vector<ValueTypeOut> data_;

    /*
     * Send all elements to their designated PEs
     */
    void PreOp(const ValueTypeIn& v) {
        Key k = key_extractor_(v);
        auto recipient = hash_function_(k) % emitter_.size();
        emitter_[recipient](v);
    }

    /*
     * Sort and store elements in a file
     */
    void FlushVectorToFile(std::vector<ValueTypeIn>& v) {
        // sort run and sort to file
        std::sort(v.begin(), v.end(), ValueComparator(*this));
        data::File f;
        {
            auto w = f.GetWriter();
            for (auto& e : v) {
                w(e);
            }
        }

        files_.push_back(f);
    }

    //! Receive elements from other workers.
    auto MainOp() {
        using Iterator = thrill::core::StxxlFileWrapper<ValueTypeIn>;
        using OIterator = thrill::core::StxxlFileOutputWrapper<int>;
        using File = data::File;
        using Reader = File::Reader;
        using Writer = File::Writer;

        LOG << ToString() << " running main op";

        const size_t FIXED_VECTOR_SIZE = 99999;
        std::vector<ValueTypeIn> incoming;
        incoming.reserve(FIXED_VECTOR_SIZE);

        // close all emitters
        for (auto& e : emitter_) {
            e.Close();
        }

        size_t totalsize = 0;

        // get incoming elements
        auto reader = channel_->OpenReader();
        while (reader.HasNext()) {
            // if vector is full save to disk
            if (incoming.size() == FIXED_VECTOR_SIZE) {
                FlushVectorToFile(incoming);
                incoming.clear();
            }
            // store incoming element
            auto elem = reader.template Next<ValueTypeIn>();
            incoming.push_back(elem);
            LOG << "Host " << context_.host_rank() << " received " << elem << " with key " << key_extractor_(incoming.back());
            ++totalsize;
        }
        FlushVectorToFile(incoming);

        // if there's only one run, store it
        if (files_.size() == 1) {
            sorted_elems_ = files_[0];
        } // otherwise sort all runs using multiway merge
        else {
            std::vector<std::pair<Iterator, Iterator> > seq;
            seq.reserve(files_.size());
            for (std::size_t t = 0; t < files_.size(); ++t) {
                auto reader = std::make_shared<Reader>(files_[t].GetReader());
                Iterator s = Iterator(&files_[t], reader, 0, true);
                Iterator e = Iterator(&files_[t], reader, files_[t].NumItems(), false);
                seq.push_back(std::make_pair(s, e));
            }

            {
                OIterator oiter(std::make_shared<Writer>(sorted_elems_.GetWriter()));

                stxxl::parallel::sequential_file_multiway_merge<true, false>(
                    std::begin(seq),
                    std::end(seq),
                    oiter,
                    totalsize,
                    ValueComparator(*this));
            }
        }
    }
};

/******************************************************************************/

template <typename ValueTypeIn, typename Stack>
template <typename KeyExtractor, typename GroupFunction, typename HashFunction>
auto DIARef<ValueTypeIn, Stack>::GroupBy(
    const KeyExtractor &key_extractor,
    const GroupFunction &groupby_function) const {

    using DOpResult
              = typename common::FunctionTraits<GroupFunction>::result_type;

    // TODO(cn) find correct assertions for input paarms

    StatsNode* stats_node = AddChildStatsNode("GroupBy", NodeType::DOP);
    using GroupByResultNode
              = GroupByNode<DOpResult, DIARef, KeyExtractor,
                            GroupFunction, HashFunction>;
    auto shared_node
        = std::make_shared<GroupByResultNode>(*this,
                                              key_extractor,
                                              groupby_function,
                                              stats_node);

    auto groupby_stack = shared_node->ProduceStack();

    return DIARef<DOpResult, decltype(groupby_stack)>(
        shared_node,
        groupby_stack,
        { stats_node });
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_GROUPBY_HEADER

/******************************************************************************/
