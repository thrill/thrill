/*******************************************************************************
 * c7a/api/sort.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
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
#include <c7a/net/collective_communication.hpp>
#include <c7a/net/flow_control_channel.hpp>
#include <c7a/net/flow_control_manager.hpp>
#include <c7a/net/group.hpp>

#include <cmath>

namespace c7a {
namespace api {

namespace {

template <typename ValueType>
struct TreeBuilder
{

    ValueType * tree_;
    ValueType * samples_;
    size_t    index_;
    size_t    ssplitter;

    TreeBuilder(ValueType* splitter_tree, // Target: tree. Size of 'number of splitter'
                ValueType* samples,       // Source: sorted splitters. Size of 'number of splitter'
                size_t ssplitter)         // Number of splitter
        : tree_(splitter_tree),
          samples_(samples),
          index_(0),
          ssplitter(ssplitter) {
        recurse(samples, samples + ssplitter, 1);
    }

    ssize_t   snum(ValueType* s) const {
        return (ssize_t)(s - samples_);
    }

    ValueType recurse(ValueType* lo, ValueType* hi, unsigned int treeidx) {

        // pick middle element as splitter
        ValueType* mid = lo + (ssize_t)(hi - lo) / 2;

        ValueType mykey = tree_[treeidx] = *mid;

        ValueType* midlo = mid, * midhi = mid + 1;

        if (2 * treeidx < ssplitter)
        {
            recurse(lo, midlo, 2 * treeidx + 0);

            return recurse(midhi, hi, 2 * treeidx + 1);
        }
        else
        {
            return mykey;
        }
    }
};	

template <class T1, typename CompareFunction>
struct BucketEmitter
{
    static bool Equal(CompareFunction compare_function, const T1& ele1, const T1& ele2) {
        return !(compare_function(ele1, ele2) || compare_function(ele2, ele1));
    }

	static size_t RoundDown(size_t ele, size_t by) {
		return ((ele) & ~((by) - 1));
	}

    static void emitToBuckets(
        const T1* const a,
        const size_t n,
        const T1* const treearr, // Tree. sizeof |splitter|
        size_t k,                // Number of buckets
        size_t logK,
        std::vector<data::BlockWriter>& emitters,
        size_t actual_k,
        CompareFunction compare_function,
        const T1* const sorted_splitters,
        size_t prefix_elem,
        size_t total_elem) {

        const size_t stepsize = 2;

        size_t i = 0;
        for ( ; i < RoundDown(n, stepsize); i += stepsize)
        {

            size_t j0 = 1;
            const T1& el0 = a[i];
            size_t j1 = 1;
            const T1& el1 = a[i + 1];

            for (size_t l = 0; l < logK; l++)
            {

                j0 = j0 * 2 + !(compare_function(el0, treearr[j0]));
                j1 = j1 * 2 + !(compare_function(el1, treearr[j1]));
            }

            size_t b0 = j0 - k;
            size_t b1 = j1 - k;

            //TODO(an): Remove this ugly workaround as soon as emitters are movable.
            //Move emitter[actual_k] to emitter[splitter_count] before calling this.
            if (b0 >= actual_k) {
                b0 = actual_k - 1;
            }

            while (b0 && Equal(compare_function, el0, sorted_splitters[b0 - 1])
                   && (prefix_elem + i) * actual_k > b0 * total_elem) {
                b0--;
            }
            emitters[b0](el0);
            if (b1 >= actual_k) {
                b1 = actual_k - 1;
            }
            while (b1 && Equal(compare_function, el1, sorted_splitters[b1 - 1])
                   && (prefix_elem + i + 1) * actual_k > b1 * total_elem) {
                b1--;
            }
            emitters[b1](el1);
        }
        for ( ; i < n; i++)
        {

            size_t j = 1;
            for (size_t l = 0; l < logK; l++)
            {
                j = j * 2 + !(compare_function(a[i], treearr[j]));
            }
            size_t b = j - k;

            if (b >= actual_k) {
                b = actual_k - 1;
            }
            while (b && Equal(compare_function, a[i], sorted_splitters[b - 1])
                   && (prefix_elem + i) * actual_k > b * total_elem) {
                b--;
            }
            emitters[b](a[i]);
        }
    }
};

}

//! \addtogroup api Interface
//! \{

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
             std::shared_ptr<DIANode<ParentInput> > parent,
             const ParentStack& parent_stack,
             CompareFunction compare_function)
        : DOpNode<ValueType>(ctx, { parent }, "Sort"),
          compare_function_(compare_function),
          channel_id_samples_(ctx.data_manager().GetNewChannel()),
          emitters_samples_(channel_id_samples_->OpenWriters()),
          channel_id_data_(ctx.data_manager().GetNewChannel()),
          emitters_data_(channel_id_data_->OpenWriters())
    {
        // Hook PreOp(s)
        auto pre_op_fn = [ = ](const ValueType& input) {
                             PreOp(input);
                         };

        auto lop_chain = parent_stack.push(pre_op_fn).emit();

        parent->RegisterChild(lop_chain);
    }

    virtual ~SortNode() { }

    //! Executes the sum operation.
    void Execute() override {
        this->StartExecutionTimer();
        MainOp();
        this->StopExecutionTimer();
    }

    void PushData() override { }

    void Dispose() override { }

    /*!
     * Produces an 'empty' function stack, which only contains the identity
     * emitter function.
     *
     * \return Empty function stack
     */
    auto ProduceStack() {
        // Hook Identity
        auto id_fn = [ = ](ValueType t, auto emit_func) {
                         return emit_func(t);
                     };

        return MakeFunctionStack<ValueType>(id_fn);
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

    //epsilon
    double desired_imbalance = 0.25;
    
    void PreOp(ValueType input) {
        data_.push_back(input);
    }

    void FindAndSendSplitters(std::vector<ValueType>& splitters) {
        //Get samples from other workers
        size_t num_workers = context_.number_worker();
        size_t samplesize = std::ceil(log2((double)data_.size()) *
                                      (1 / (desired_imbalance *
                                            desired_imbalance)));

        std::vector<ValueType> samples;
        samples.reserve(samplesize * num_workers);
        auto reader = channel_id_samples_->OpenReader();

        while (reader.HasNext()) {
            samples.push_back(reader.template Next<ValueType>());
        }

        //Find splitters
        std::sort(samples.begin(), samples.end(), compare_function_);

        size_t splitting_size = samples.size() / num_workers;

        //Send splitters to other workers
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
        size_t samplesize = std::ceil(log2((double)total_elem) *
                                      (1 / (desired_imbalance * desired_imbalance)));

        LOG << prefix_elem << " elements, out of " << total_elem;

        std::random_device random_device;
        std::default_random_engine generator(random_device());
        std::uniform_int_distribution<int> distribution(0, data_.size() - 1);

        //Send samples to worker 0
        for (size_t i = 0; i < samplesize; i++) {
            size_t sample = distribution(generator);
            emitters_samples_[0](data_[sample]);
        }
        emitters_samples_[0].Close();

        //Get the ceiling of log(num_workers), as SSSS needs 2^n buckets.
        double log_workers = std::log2(num_workers);
        const bool powof2 = (std::ceil(log_workers) - log_workers) < 0.0000001;
        size_t ceil_log = std::ceil(log_workers);
        size_t workers_algo = 1 << ceil_log;
        size_t splitter_count_algo = workers_algo - 1;

        std::vector<ValueType> splitters;
        splitters.reserve(workers_algo);

        if (context_.rank() == 0) {
            FindAndSendSplitters(splitters);
        }
        else {
            //Close unused emitters
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

        if (!powof2) {
            for (size_t i = num_workers; i < workers_algo; i++) {
                splitters.push_back(splitters.back());
            }
        }

        TreeBuilder<ValueType>(splitter_tree,
                                     splitters.data(),
                                     splitter_count_algo);

        BucketEmitter<ValueType, CompareFunction>::emitToBuckets(
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

        for (size_t i = 0; i < emitters_data_.size(); i++) {
            emitters_data_[i].Close();
        }

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
        std::is_same<
            typename common::FunctionTraits<CompareFunction>::template arg<0>,
            ValueType>::value ||
        std::is_same<CompareFunction, std::less<ValueType> >::value,
        "CompareFunction has the wrong input type");

    static_assert(
        std::is_same<
            typename common::FunctionTraits<CompareFunction>::template arg<1>,
            ValueType>::value ||
        std::is_same<CompareFunction, std::less<ValueType> >::value,
        "CompareFunction has the wrong input type");

    static_assert(
        std::is_same<
            typename common::FunctionTraits<CompareFunction>::result_type,
            bool>::value ||
        std::is_same<CompareFunction, std::less<ValueType> >::value,
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
