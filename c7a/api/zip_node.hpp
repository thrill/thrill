/*******************************************************************************
 * c7a/api/zip_node.hpp
 *
 * DIANode for a reduce operation. Performs the actual reduce operation
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_ZIP_NODE_HEADER
#define C7A_API_ZIP_NODE_HEADER

#include <c7a/api/dop_node.hpp>
#include <c7a/api/context.hpp>
#include <c7a/api/function_stack.hpp>
#include <c7a/common/logger.hpp>
#include <c7a/net/collective_communication.hpp>

#include <unordered_map>
#include <functional>
#include <string>
#include <vector>

namespace c7a {

//! \addtogroup api Interface
//! \{

/*!
 * A DIANode which performs a Zip operation. Zip combines two DIAs
 * element-by-element. The ZipNode stores the zip_function UDF. The chainable
 * LOps are stored in the Stack.
 *
 * \tparam Output Output type of the Zip operation.
 *
 * \tparam Stack1 Function stack, which contains the chained lambdas between the
 * last and this DIANode for first input DIA.
 *
 * \tparam Stack2 Function stack, which contains the chained lambdas between the
 * last and this DIANode for secondt input DIA.
 *
 * \tparam Zip_Function Type of the ZipFunction.
 */
template <typename Input1, typename Input2, typename Output,
          typename Stack1, typename Stack2, typename ZipFunction>
class TwoZipNode : public DOpNode<Output>
{
    static const bool debug = false;

    using ZipArg0 = typename FunctionTraits<ZipFunction>::template arg<0>;
    using ZipArg1 = typename FunctionTraits<ZipFunction>::template arg<1>;

public:
    /*!
     * Constructor for a ZipNode.
     *
     * \param ctx Reference to the Context, which gives iterators for data
     * \param parent1 First parent of the ZipNode
     * \param parent2 Second parent of the ZipNode
     * \param stack1 Function stack with all lambdas between the parent and this node for first DIA
     * \param stack2 Function stack with all lambdas between the parent and this node for second DIA
     * \param zip_function Zip function used to zip elements.
     */
    TwoZipNode(Context& ctx,
               DIANode<Input1>* parent1,
               DIANode<Input2>* parent2,
               Stack1& stack1,
               Stack2& stack2,
               ZipFunction zip_function)
        : DOpNode<Output>(ctx, { parent1, parent2 }),
          stack1_(stack1),
          stack2_(stack2),
          zip_function_(zip_function)
    {
        // Hook PreOp(s)
        auto pre_op1_fn = [=](Input1 input) {
                              PreOp(input);
                          };
        auto pre_op2_fn = [=](Input2 input) {
                              PreOpSecond(input);
                          };
        auto lop_chain1 = stack1_.push(pre_op1_fn).emit();
        auto lop_chain2 = stack2_.push(pre_op2_fn).emit();

        parent1->RegisterChild(lop_chain1);
        parent2->RegisterChild(lop_chain2);

        // Setup Emitters
        num_dias_ = 2;
        for (size_t i = 0; i < num_dias_; ++i) {
            id_.push_back(context_.get_data_manager().AllocateDIA());
        }
        emit1_ = context_.get_data_manager().template GetLocalEmitter<ZipArg0>(id_[0]);
        emit2_ = context_.get_data_manager().template GetLocalEmitter<ZipArg1>(id_[1]);
    }

    /*!
     * Actually executes the zip operation. Uses the member functions PreOp,
     * MainOp and PostOp.
     */
    void execute() override {
        MainOp();
        // get data from data manager
        auto it1 = context_.get_data_manager().template GetIterator<ZipArg0>(id_[0]);
        auto it2 = context_.get_data_manager().template GetIterator<ZipArg1>(id_[1]);
        do {
            it1.WaitForMore();
            it2.WaitForMore();
            // Iterate over smaller DIA
            while (it1.HasNext() && it2.HasNext()) {
                auto item = std::make_pair(it1.Next(), it2.Next());
                for (auto func : DIANode<Output>::callbacks_) {
                    func(item);
                }
            }
        } while (!it1.IsClosed() && !it2.IsClosed());
    }

    /*!
     * TODO(an): I have no idea...
     */
    auto ProduceStack() {
        // Hook PostOp
        auto post_op_fn = [=](Output elem, std::function<void(Output)> emit_func) {
                              return PostOp(elem, emit_func);
                          };

        FunctionStack<> stack;
        return stack.push(post_op_fn);
    }

    /*!
     * Returns "[ZipNode]" as a string.
     * \return "[ZipNode]"
     */
    std::string ToString() override {
        return "[ZipNode]";
    }

private:
    //! operation context
    using DOpNode<Output>::context_;

    //! Local stacks
    Stack1 stack1_;
    Stack2 stack2_;
    //! Zip function
    ZipFunction zip_function_;
    //! Emitter
    std::vector<data::DIAId> id_;
    data::Emitter<ZipArg0> emit1_;
    data::Emitter<ZipArg1> emit2_;
    //! Number of DIAs
    size_t num_dias_;

    //! Zip PreOp does nothing. First part of Zip is a PrefixSum, which needs a
    //! global barrier.
    void PreOp(ZipArg0 input) {
        emit1_(input);
    }

    // TODO(an): Theoretically we need two PreOps?
    void PreOpSecond(ZipArg1 input) {
        emit2_(input);
    }

    //!Receive elements from other workers.
    void MainOp() {
        net::Group flow_group = context_.get_flow_net_group();
        data::Manager data_manager = context_.get_data_manager();
        size_t workers = context_.number_worker();

        // Offsets to declare which target gets which block
        std::vector<size_t> blocks(num_dias_, 0);

        for (size_t i = 0; i < num_dias_; ++i) {
            size_t prefix = data_manager.get_current_size(id_[i]);
            net::PrefixSum(flow_group, prefix);
            size_t total = data_manager.get_current_size(id_[i]);
            // TODO: flow_group.TotalSum(prefix);
            size_t size = data_manager.get_current_size(id_[i]);
            size_t per_pe = total / workers;
            size_t target = prefix / per_pe;
            size_t block = std::min(per_pe - prefix % per_pe, size);

            while (block <= size) {
                blocks[target] = block;
                target++;
                size -= block;
            }

            // TODO(ts): Send blocks to other targets
        }
    }

    //! Use the ZipFunction to Zip workers
    void PostOp(Output input, std::function<void(Output)> emit_func) {
        emit_func(zip_function_(input.first, input.second));
    }
};

} // namespace c7a

//! \}

#endif // !C7A_API_ZIP_NODE_HEADER

/******************************************************************************/
