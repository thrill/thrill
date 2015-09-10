/*******************************************************************************
 * thrill/api/groupby.hpp
 *
 * DIANode for a groupby operation. Performs the actual groupby operation
 *
 * Part of Project Thrill.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
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

template <typename ValueType, typename ParentDIARef,
          typename KeyExtractor, typename GroupFunction, typename HashFunction>
class GroupByNode;


template <typename ValueType, typename KeyExtractor>
class GroupByIterator {
template <typename T1,
          typename T2,
          typename T3,
          typename T4,
          typename T5> friend class GroupByNode;
public:
    static const bool debug = false;
    using ValueIn = ValueType;
    using Key = typename common::FunctionTraits<KeyExtractor>::result_type;
    using Reader = typename data::File::Reader;

    GroupByIterator(Reader& reader, KeyExtractor &key_extractor)
                  : reader_(reader),
                    key_extractor_(key_extractor),
                    is_first_elem_(true),
                    is_reader_empty(false),
                    elem_(reader.template Next<ValueIn>()),
                    old_key_(key_extractor_(elem_)),
                    new_key_(old_key_) {
    }

    bool HasNext() {
        return (!is_reader_empty && old_key_ == new_key_) || is_first_elem_;
    }

    ValueIn Next() {
        assert(!is_reader_empty);
        auto elem = elem_;
        GetNextElem();
        return elem;
    }

protected:
    bool HasNextForReal() {
        is_first_elem_ = true;
        return !is_reader_empty;
    }

private:
    Reader& reader_;
    KeyExtractor& key_extractor_;
    bool is_first_elem_;
    bool is_reader_empty;
    ValueIn elem_;
    Key old_key_;
    Key new_key_;

    void GetNextElem() {
        is_first_elem_ = false;
        if (reader_.HasNext()) {
            elem_ = reader_.template Next<ValueIn>();
            old_key_ = new_key_;
            new_key_ = key_extractor_(elem_);
        } else {
            is_reader_empty = true;
        }
    }

    void SetFirstElem() {
        assert (reader_.HasNext());
        is_first_elem_ = true;
        elem_ = reader_.template Next<ValueIn>();
        old_key_ = key_extractor_(elem_);
        new_key_ = old_key_;
    }
};

template <typename ValueType, typename ParentDIARef,
          typename KeyExtractor, typename GroupFunction, typename HashFunction>
class GroupByNode : public DOpNode<ValueType>
{
    static const bool debug = false;
    using Super = DOpNode<ValueType>;
    using Key = typename common::FunctionTraits<KeyExtractor>::result_type;
    using ValueOut = ValueType;
    using GroupIterator = typename common::FunctionTraits<GroupFunction>
                      ::template arg<0>;
    using ValueIn = typename std::remove_reference<GroupIterator>::type::ValueIn;
    using Reader = typename data::File::Reader;

    struct ValueComparator
    {
        ValueComparator(const GroupByNode& info) : info_(info) { }
        const GroupByNode& info_;

        bool operator () (const ValueType& i,
                          const ValueType& j) {
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
        : DOpNode<ValueType>(parent.ctx(), { parent.node() }, stats_node),
          key_extractor_(key_extractor),
          groupby_function_(groupby_function),
          hash_function_(hash_function),
          channel_(parent.ctx().GetNewChannel()),
          emitter_(channel_->OpenWriters()),
          sorted_elems_(context_.GetFile())
    {
        // Hook PreOp
        auto pre_op_fn = [=](const ValueIn& input) {
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
        std::vector<std::function<void(const ValueType&)> > cbs;
        DIANode<ValueType>::callback_functions(cbs);
    }

    void PushData() override {
        auto r = sorted_elems_.GetReader();
        if (r.HasNext()) {
            // create iterator to pass to user_function
            auto user_iterator = GroupByIterator<ValueIn, KeyExtractor>(r, key_extractor_);
            while(user_iterator.HasNextForReal()){
                // call user function
                const ValueOut res = groupby_function_(user_iterator);
                // push result to callback functions
                for (auto func : DIANode<ValueType>::callbacks_) {
                    LOG << "grouped to value " << res;
                    func(res);
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

    // /*!
    //  * Returns "[GroupByNode]" and its id as a string.
    //  * \return "[GroupByNode]"
    //  */
    // std::string ToString() override {
    //     return "[GroupByNode] Id: " + result_file_.ToString();
    // }

private:
    KeyExtractor key_extractor_;
    GroupFunction groupby_function_;
    HashFunction hash_function_;

    data::ChannelPtr channel_;
    std::vector<data::Channel::Writer> emitter_;
    std::vector<data::File> files_;
    data::File sorted_elems_;

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
        // sort run and sort to file
        std::sort(v.begin(), v.end(), ValueComparator(*this));
        auto f = context_.GetFile();
        {
            auto w = f.GetWriter();
            for (const auto& e : v) {
                w(e);
            }
        }

        files_.push_back(f);
    }

    //! Receive elements from other workers.
    auto MainOp() {
        using Iterator = thrill::core::StxxlFileWrapper<ValueIn>;
        using OIterator = thrill::core::StxxlFileOutputWrapper<int>;
        using File = data::File;
        using Reader = File::Reader;
        using Writer = File::Writer;

        LOG << "running group by main op";

        const std::size_t FIXED_VECTOR_SIZE = 99999;
        std::vector<ValueIn> incoming;
        incoming.reserve(FIXED_VECTOR_SIZE);

        // close all emitters
        for (auto& e : emitter_) {
            e.Close();
        }

        std::size_t totalsize = 0;

        // get incoming elements
        auto reader = channel_->OpenReader();
        while (reader.HasNext()) {
            // if vector is full save to disk
            if (incoming.size() == FIXED_VECTOR_SIZE) {
                FlushVectorToFile(incoming);
                incoming.clear();
            }
            // store incoming element
            const auto elem = reader.template Next<ValueIn>();
            incoming.push_back(elem);
            // LOG << "received " << elem << " with key " << key_extractor_(incoming.back());
            ++totalsize;
        }
        FlushVectorToFile(incoming);

        const auto num_elems = files_.size();
        // if there's only one run, store it
        if (num_elems == 1) {
            auto w = sorted_elems_.GetWriter();
            auto r = files_[0].GetReader();
            {
                while(r.HasNext()) {
                    w(r.template Next<ValueIn>());
                }
            }
        } // otherwise sort all runs using multiway merge
        else {
            std::vector<std::pair<Iterator, Iterator> > seq;
            seq.reserve(num_elems);
            for (std::size_t t = 0; t < num_elems; ++t) {
                auto reader = std::make_shared<Reader>(files_[t].GetReader());
                Iterator s = Iterator(&files_[t], reader, 0, true);
                Iterator e = Iterator(&files_[t], reader, files_[t].num_items(), false);
                seq.push_back(std::make_pair(std::move(s), std::move(e)));
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

template <typename ValueType, typename Stack>
template <typename KeyExtractor, typename GroupFunction, typename HashFunction>
auto DIARef<ValueType, Stack>::GroupBy(
    const KeyExtractor &key_extractor,
    const GroupFunction &groupby_function) const {

    using DOpResult
              = typename common::FunctionTraits<GroupFunction>::result_type;

    static_assert(
        std::is_same<
            typename std::decay<typename common::FunctionTraits<KeyExtractor>
                                ::template arg<0> >::type,
            ValueType>::value,
        "KeyExtractor has the wrong input type");

    StatsNode* stats_node = AddChildStatsNode("GroupBy", DIANodeType::DOP);
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
