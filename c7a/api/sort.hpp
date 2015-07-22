/*******************************************************************************
 * c7a/api/sort.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_SORT_HEADER
#define C7A_API_SORT_HEADER

#include <c7a/api/context.hpp>
#include <c7a/api/dia.hpp>
#include <c7a/api/function_stack.hpp>
#include <c7a/common/logger.hpp>
#include <c7a/common/math.hpp>
#include <c7a/net/collective_communication.hpp>
#include <c7a/net/flow_control_channel.hpp>
#include <c7a/net/flow_control_manager.hpp>
#include <c7a/net/group.hpp>

#include <cmath>

namespace c7a {
namespace api {

//! \addtogroup api Interface
//! \{

namespace sort_local {

template <typename ValueType>
struct TreeBuilder
{
    ValueType* tree_;
    ValueType* samples_;
    size_t   index_ = 0;
    size_t   ssplitter;

    /*!
     * Target: tree. Size of 'number of splitter'
     * Source: sorted splitters. Size of 'number of splitter'
     * Number of splitter
     */
    TreeBuilder(ValueType* splitter_tree,
                ValueType* samples,
                size_t ssplitter)
        : tree_(splitter_tree),
          samples_(samples),
          ssplitter(ssplitter) {
        recurse(samples, samples + ssplitter, 1);
    }

    void     recurse(ValueType* lo, ValueType* hi, unsigned int treeidx) {

        // pick middle element as splitter
        ValueType* mid = lo + (ssize_t)(hi - lo) / 2;
        tree_[treeidx] = *mid;

        ValueType* midlo = mid, * midhi = mid + 1;

        if (2 * treeidx < ssplitter)
        {
            recurse(lo, midlo, 2 * treeidx + 0);
            recurse(midhi, hi, 2 * treeidx + 1);
        }
    }
};

template <class T1, typename CompareFunction>
class BucketEmitter
{
public:
    static bool Equal(CompareFunction compare_function,
                      const T1& ele1, const T1& ele2) {
        return !(compare_function(ele1, ele2) || compare_function(ele2, ele1));
    }

    //! round n down by k where k is a power of two.
    template <typename Integral>
    static inline size_t RoundDown(Integral n, Integral k) {
        return (n & ~(k - 1));
    }

    static void EmitToBuckets(
        const T1* const array,
        const size_t size,
        const T1* const tree, // Tree. sizeof |splitter|
        // Number of buckets: k = 2^{logK}
        size_t k,
        size_t logK,
        std::vector<data::BlockWriter>& emitters,
        // Number of actual workers to send to
        size_t actual_k,
        CompareFunction compare_function,
        const T1* const sorted_splitters,
        size_t prefix_elem,
        size_t total_elem) {

        // enlarge emitters array to next power of two to have direct access.
        assert(emitters.size() == actual_k);
        assert(actual_k <= k);

        while (emitters.size() < k)
            emitters.emplace_back(nullptr);

        std::swap(emitters[actual_k - 1], emitters[k - 1]);

        // classify all items (take two at once) and immediately transmit them.

        const size_t stepsize = 2;

        size_t i = 0;
        for ( ; i < RoundDown(size, stepsize); i += stepsize)
        {
            // take two items
            size_t j0 = 1;
            const T1& el0 = array[i];

            size_t j1 = 1;
            const T1& el1 = array[i + 1];

            // run items down the tree
            for (size_t l = 0; l < logK; l++)
            {
                j0 = j0 * 2 + !(compare_function(el0, tree[j0]));
                j1 = j1 * 2 + !(compare_function(el1, tree[j1]));
            }

            size_t b0 = j0 - k;
            size_t b1 = j1 - k;

            while (b0 && Equal(compare_function, el0, sorted_splitters[b0 - 1])
                   && (prefix_elem + i) * actual_k >= b0 * total_elem) {
                b0--;
            }
            assert(emitters[b0].IsValid());
            emitters[b0](el0);

            while (b1 && Equal(compare_function, el1, sorted_splitters[b1 - 1])
                   && (prefix_elem + i + 1) * actual_k >= b1 * total_elem) {
                b1--;
            }
            assert(emitters[b1].IsValid());
            emitters[b1](el1);
        }

        // last iteration of loop if we have an odd number of items.
        for ( ; i < size; i++)
        {
            size_t j = 1;
            for (size_t l = 0; l < logK; l++)
            {
                j = j * 2 + !(compare_function(array[i], tree[j]));
            }

            size_t b = j - k;

            while (b && Equal(compare_function, array[i], sorted_splitters[b - 1])
                   && (prefix_elem + i) * actual_k >= b * total_elem) {
                b--;
            }
            assert(emitters[b].IsValid());
            emitters[b](array[i]);
        }
    }
};

} // namespace sort_local

/*!
 * A DIANode which performs a Sort operation. Sort sorts a DIA according to a given
 * compare function
 *
 * \tparam ValueType Type of DIA elements
 * \tparam Stack Function stack, which contains the chained lambdas between the last and this DIANode.
 * \tparam CompareFunction Type of the compare function
 */
template <typename ValueType, typename ParentStack, typename CompareFunction>
class SortNode : public DOpNode<ValueType>
{
    static const bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;
    using Super::result_file_;

    using ParentInput = typename ParentStack::Input;

public:
    /*!
     * Constructor for a sort node.
     *
     * \param ctx Context for this operation
     * \param parent Previous DIANode in the computation chain
     * \param parent_stack Stack of lambda functions between parent and this node
     * \param compare_function Function comparing two elements.
     */
    SortNode(Context& ctx,
             const std::shared_ptr<DIANode<ParentInput> >& parent,
             const ParentStack& parent_stack,
             CompareFunction compare_function)
        : DOpNode<ValueType>(ctx, { parent }, "Sort"),
          compare_function_(compare_function),
          channel_id_samples_(ctx.data_manager().GetNewChannel()),
          emitters_samples_(channel_id_samples_->OpenWriters()),
          channel_id_data_(ctx.data_manager().GetNewChannel()),
          emitters_data_(channel_id_data_->OpenWriters()),
          parent_(parent)
    {
        // Hook PreOp(s)
        auto pre_op_fn = [=](const ValueType& input) {
                             PreOp(input);
                         };

        lop_chain_ = parent_stack.push(pre_op_fn).emit();

        parent_->RegisterChild(lop_chain_);
    }

    virtual ~SortNode() {
        parent_->UnregisterChild(lop_chain_);
    }

    //! Executes the sum operation.
    void Execute() override {
        this->StartExecutionTimer();
        MainOp();
        this->StopExecutionTimer();
    }

    /*!
     * Produces an 'empty' function stack, which only contains the identity
     * emitter function.
     *
     * \return Empty function stack
     */
    auto ProduceStack() {
        return FunctionStack<ValueType>();
    }

    /*!
     * Returns "[SortNode]" as a string.
     * \return "[SortNode]"
     */
    std::string ToString() override {
        return "[SortNode] Id:" + result_file_.ToString();
    }

private:
    //! The sum function which is applied to two elements.
    CompareFunction compare_function_;
    //! Local data
    std::vector<ValueType> data_;

    //!Emitter to send samples to process 0
    data::ChannelPtr channel_id_samples_;
    std::vector<data::BlockWriter> emitters_samples_;

    //!Emitters to send data to other workers specified by splitters.
    data::ChannelPtr channel_id_data_;
    std::vector<data::BlockWriter> emitters_data_;

    // epsilon
    static constexpr double desired_imbalance_ = 0.25;

    std::shared_ptr<DIANode<ParentInput> > parent_;
    common::delegate<void(ParentInput)> lop_chain_;

    void PreOp(ValueType input) {
        data_.push_back(input);
    }

    void FindAndSendSplitters(
        std::vector<ValueType>& splitters, size_t sample_size) {
        // Get samples from other workers
        size_t num_workers = context_.number_worker();

        std::vector<ValueType> samples;
        samples.reserve(sample_size * num_workers);
        auto reader = channel_id_samples_->OpenReader();

        while (reader.HasNext()) {
            samples.push_back(reader.template Next<ValueType>());
        }

        // Find splitters
        std::sort(samples.begin(), samples.end(), compare_function_);

        size_t splitting_size = samples.size() / num_workers;

        // Send splitters to other workers
        for (size_t i = 1; i < num_workers; i++) {
            splitters.push_back(samples[i * splitting_size]);
            for (size_t j = 1; j < num_workers; j++) {
                emitters_samples_[j](samples[i * splitting_size]);
            }
        }

        for (size_t j = 1; j < num_workers; j++) {
            emitters_samples_[j].Close();
        }
    }

    void MainOp() {
        net::FlowControlChannel& channel = context_.flow_control_channel();

        size_t prefix_elem = channel.PrefixSum(data_.size());
        size_t total_elem = channel.AllReduce(data_.size());

        size_t num_workers = context_.number_worker();
        size_t sample_size =
            common::IntegerLog2Ceil(total_elem) *
            (1 / (desired_imbalance_ * desired_imbalance_));

        LOG << prefix_elem << " elements, out of " << total_elem;

        std::random_device random_device;
        std::default_random_engine generator(random_device());
        std::uniform_int_distribution<int> distribution(0, data_.size() - 1);

        // Send samples to worker 0
        for (size_t i = 0; i < sample_size; i++) {
            size_t sample = distribution(generator);
            emitters_samples_[0](data_[sample]);
        }
        emitters_samples_[0].Close();

        // Get the ceiling of log(num_workers), as SSSS needs 2^n buckets.
        size_t ceil_log = common::IntegerLog2Ceil(num_workers);
        size_t workers_algo = 1 << ceil_log;
        size_t splitter_count_algo = workers_algo - 1;

        std::vector<ValueType> splitters;
        splitters.reserve(workers_algo);

        if (context_.rank() == 0) {
            FindAndSendSplitters(splitters, sample_size);
        }
        else {
            // Close unused emitters
            for (size_t j = 1; j < num_workers; j++) {
                emitters_samples_[j].Close();
            }
            auto reader = channel_id_samples_->OpenReader();
            while (reader.HasNext()) {
                splitters.push_back(reader.template Next<ValueType>());
            }
        }

        //code from SS2NPartition, slightly altered

        ValueType* splitter_tree = new ValueType[workers_algo + 1];

        // add sentinel splitters if fewer nodes than splitters.
        for (size_t i = num_workers; i < workers_algo; i++) {
            splitters.push_back(splitters.back());
        }

        sort_local::TreeBuilder<ValueType>(splitter_tree,
                                           splitters.data(),
                                           splitter_count_algo);

        sort_local::BucketEmitter<ValueType, CompareFunction>::EmitToBuckets(
            data_.data(),
            data_.size(),
            splitter_tree, // Tree. sizeof |splitter|
            workers_algo,  // Number of buckets
            ceil_log,
            emitters_data_,
            num_workers,
            compare_function_,
            splitters.data(),
            prefix_elem,
            total_elem);

        //end of SS2N

        for (size_t i = 0; i < emitters_data_.size(); i++)
            emitters_data_[i].Close();

        data_.clear();

        auto reader = channel_id_data_->OpenReader();

        while (reader.HasNext()) {
            data_.push_back(reader.template Next<ValueType>());
        }

        LOG << "node " << context_.rank() << " : " << data_.size();

        std::sort(data_.begin(), data_.end(), compare_function_);

        for (size_t i = 0; i < data_.size(); i++) {
            for (auto func : DIANode<ValueType>::callbacks_) {
                func(data_[i]);
            }
        }
        std::vector<ValueType>().swap(data_);
    }

    void PostOp() { }
};

template <typename ValueType, typename Stack>
template <typename CompareFunction>
auto DIARef<ValueType, Stack>::Sort(const CompareFunction &compare_function) const {

    using SortResultNode
              = SortNode<ValueType, Stack, CompareFunction>;

    static_assert(
        std::is_convertible<
            ValueType,
            typename common::FunctionTraits<CompareFunction>::template arg<0>
            >::value ||
        std::is_same<CompareFunction, std::less<ValueType> >::value,
        "CompareFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            ValueType,
            typename common::FunctionTraits<CompareFunction>::template arg<1> >::value ||
        std::is_same<CompareFunction, std::less<ValueType> >::value,
        "CompareFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            typename common::FunctionTraits<CompareFunction>::result_type,
            bool>::value,
        "CompareFunction has the wrong output type (should be bool)");

    auto shared_node
        = std::make_shared<SortResultNode>(node_->context(),
                                           node_,
                                           stack_,
                                           compare_function);

    auto sort_stack = shared_node->ProduceStack();

    return DIARef<ValueType, decltype(sort_stack)>(
        std::move(shared_node), sort_stack);
}

//! \}

} // namespace api
} // namespace c7a

#endif // !C7A_API_SORT_HEADER

/******************************************************************************/
