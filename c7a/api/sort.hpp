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

#include <c7a/api/function_stack.hpp>
#include <c7a/api/dia.hpp>
#include <c7a/api/context.hpp>
#include <c7a/net/group.hpp>
#include <c7a/net/collective_communication.hpp>
#include <c7a/net/flow_control_channel.hpp>
#include <c7a/net/flow_control_manager.hpp>
#include <c7a/common/logger.hpp>

#include <cmath>

namespace c7a {
namespace api {

template <typename ValueType, typename ParentStack, typename CompareFunction>
class SortNode : public DOpNode<ValueType>
{
    static const bool debug = true;

    using Super = DOpNode<ValueType>;
    using Super::context_;
    using Super::data_id_;

    using ParentInput = typename ParentStack::Input;

public:
    SortNode(Context& ctx,
	     std::shared_ptr<DIANode<ParentInput> > parent,
	     const ParentStack& parent_stack,
	     CompareFunction compare_function)
        : DOpNode<ValueType>(ctx, { parent }),
          compare_function_(compare_function),
	  channel_id_samples_(ctx.data_manager().AllocateNetworkChannel()),
	  emitters_samples_(ctx.data_manager().
			    template GetNetworkEmitters<ValueType>(channel_id_samples_)),
	  channel_id_data_(ctx.data_manager().AllocateNetworkChannel()),
	  emitters_data_(ctx.data_manager().
			 template GetNetworkEmitters<ValueType>(channel_id_data_))
    {
        // Hook PreOp(s)
        auto pre_op_fn = [=](const ValueType& input) {
                             PreOp(input);
                         };

        auto lop_chain = parent_stack.push(pre_op_fn).emit();

        parent->RegisterChild(lop_chain);
    }

    virtual ~SortNode() { }

    //! Executes the sum operation.
    void Execute() override {
        MainOp();
    }

    /*!
     * Produces an 'empty' function stack, which only contains the identity
     * emitter function.
     *
     * \return Empty function stack
     */
    auto ProduceStack() {
        // Hook Identity
        auto id_fn = [=](ValueType t, auto emit_func) {
                         return emit_func(t);
                     };

        return MakeFunctionStack<ValueType>(id_fn);
    }

    /*!
     * Returns "[SortNode]" as a string.
     * \return "[SortNode]"
     */
    std::string ToString() override {
        return "[PrefixSumNode] Id:" + data_id_.ToString();
    }

private:
    //! The sum function which is applied to two elements.
    CompareFunction compare_function_;
    //! Local data
    std::vector<ValueType> data_;

    //!Emitter to send samples to process 0
    data::ChannelId channel_id_samples_;
    std::vector<data::Emitter<ValueType> > emitters_samples_;

    //!Emitters to send data to other workers specified by splitters.
    data::ChannelId channel_id_data_;
    std::vector<data::Emitter<ValueType> > emitters_data_;

    //epsilon
    double desired_imbalance = 0.25;

    void PreOp(ValueType input) {
        //LOG << "Input: " << input;
        data_.push_back(input);
    }

    void MainOp() {
        //LOG << "MainOp processing";

	size_t samplesize = std::ceil(log2((double) data_.size()) *
				      (1 / (desired_imbalance * desired_imbalance)));
	
	std::random_device random_device;
	std::default_random_engine generator(random_device());
	std::uniform_int_distribution<int> distribution(0, data_.size() - 1);

	//Send samples to worker 0
	for (size_t i = 0; i < samplesize; i++) {
	    size_t sample = distribution(generator);
	    emitters_samples_[0](data_[sample]);
	}
	emitters_samples_[0].Close();
	
	std::vector<ValueType> splitters;
	splitters.reserve(context_.number_worker() - 1);
	
	if (context_.rank() == 0) {
	    //Get samples
	    std::vector<ValueType> samples;
	    samples.reserve(samplesize * context_.number_worker());
	    auto it = context_.data_manager().
		template GetIterator<ValueType>(channel_id_samples_);
	    do {
		it.WaitForMore();
		while (it.HasNext()) {
		    samples.push_back(it.Next());
		}
	    } while (!it.IsFinished());

	    //Find splitters
	    std::sort(samples.begin(), samples.end(), compare_function_);
	    
	    size_t splitting_size = samples.size() / context_.number_worker();
	    
	    //Send splitters to other workers
	    for (size_t i = 1; i < context_.number_worker(); i++) {
		splitters.push_back(samples[i * splitting_size]);
		for (size_t j = 1; j < context_.number_worker(); j++) {
		    emitters_samples_[j](samples[i * splitting_size]);
		}
	    }

	    for (size_t j = 1; j < context_.number_worker(); j++) {
		emitters_samples_[j].Close();
	    }
	} else {
	    //Close unused emitters
	    for (size_t j = 1; j < context_.number_worker(); j++) {
		emitters_samples_[j].Close();
	    }
	    auto it = context_.data_manager().
		template GetIterator<ValueType>(channel_id_samples_);
	    do {
		it.WaitForMore();
		while (it.HasNext()) {
		    splitters.push_back(it.Next());
		}
	    } while (!it.IsFinished());
	}

        for (ValueType ele : data_) {
            bool sent = false;
            for (size_t i = 0; i < splitters.size() && !sent; i++) {
                if (compare_function_(ele, splitters[i])) {
                    emitters_data_[i](ele);
                    sent = true;
                    break;
                }   
            }
            if (!sent) {
                emitters_data_[splitters.size()](ele);
            }
        }

        for (size_t i = 0; i < emitters_data_.size(); i++) {
            emitters_data_[i].Close();
        }

        data_.clear();
                
        auto it = context_.data_manager().
            template GetIterator<ValueType>(channel_id_data_);

        do {
            it.WaitForMore();
            while (it.HasNext()) {
                data_.push_back(it.Next());
            }
        } while (!it.IsFinished());

        
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
auto DIARef<ValueType, Stack>::Sort(const CompareFunction & compare_function) const {

    using SortResultNode
              = SortNode<ValueType, Stack, CompareFunction>;

    static_assert(
        std::is_same<
            typename common::FunctionTraits<CompareFunction>::template arg<0>,
            ValueType>::value ||
        std::is_same<CompareFunction, common::LessThan<ValueType> >::value,
        "CompareFunction has the wrong input type");

    static_assert(
        std::is_same<
            typename common::FunctionTraits<CompareFunction>::template arg<1>,
            ValueType>::value ||
        std::is_same<CompareFunction, common::LessThan<ValueType> >::value,
        "CompareFunction has the wrong input type");

    static_assert(
        std::is_same<
            typename common::FunctionTraits<CompareFunction>::result_type,
            ValueType>::value ||
        std::is_same<CompareFunction, common::LessThan<ValueType> >::value,
        "CompareFunction has the wrong input type");

    auto shared_node
        = std::make_shared<SortResultNode>(node_->context(),
                                          node_,
                                          stack_,
                                          compare_function);

    auto sort_stack = shared_node->ProduceStack();

    return DIARef<ValueType, decltype(sort_stack)>(
        std::move(shared_node), sort_stack);
}

} // namespace api
} // namespace c7a

#endif // !C7A_API_SORT_NODE_HEADER

/******************************************************************************/
