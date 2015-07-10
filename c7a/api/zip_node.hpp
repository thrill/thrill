/*******************************************************************************
 * c7a/api/zip_node.hpp
 *
 * DIANode for a reduce operation. Performs the actual reduce operation
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_ZIP_NODE_HEADER
#define C7A_API_ZIP_NODE_HEADER

#include <c7a/api/dop_node.hpp>
#include <c7a/api/context.hpp>
#include <c7a/api/function_stack.hpp>
#include <c7a/common/logger.hpp>
#include <c7a/net/collective_communication.hpp>

#include <functional>
#include <string>
#include <vector>

namespace c7a {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * A DIANode which performs a Zip operation. Zip combines two DIAs
 * element-by-element. The ZipNode stores the zip_function UDF. The chainable
 * LOps are stored in the Stack.
 *
 * <pre>
 *                ParentStack1 ParentStack2
 *                 +--------+   +--------+
 *                 |        |   |        |  A ParentStackX is called with
 *                 |        |   |        |  ParentInputX, and must deliver
 *                 |        |   |        |  a ZipArgX item.
 *               +-+--------+---+--------+-+
 *               | | PreOp1 |   | PreOp2 | |
 *               | +--------+   +--------+ |
 * DIARef<T> --> |           Zip           |
 *               |        +-------+        |
 *               |        |PostOp |        |
 *               +--------+-------+--------+
 *                        |       | New DIARef<T>::stack_ is started
 *                        |       | with PostOp to chain next nodes.
 *                        +-------+
 * </pre>
 *
 * \tparam ValueType Output type of the Zip operation.
 *
 * \tparam ParentStack1 Function stack, which contains the chained lambdas
 * between the last and this DIANode for first input DIA.
 *
 * \tparam ParentStack2 Function stack, which contains the chained lambdas
 * between the last and this DIANode for second input DIA.
 *
 * \tparam ZipFunction Type of the ZipFunction.
 */
template <typename ValueType,
          typename ParentStack1, typename ParentStack2,
          typename ZipFunction>
class TwoZipNode : public DOpNode<ValueType>
{
    static const bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

    using ZipArg0 =
              typename common::FunctionTraits<ZipFunction>::template arg<0>;
    using ZipArg1 =
              typename common::FunctionTraits<ZipFunction>::template arg<1>;

    using ParentInput1 = typename ParentStack1::Input;
    using ParentInput2 = typename ParentStack2::Input;

public:
    /*!
     * Constructor for a ZipNode.
     *
     * \param ctx Reference to the Context, which gives iterators for data
     * \param parent1 First parent of the ZipNode
     * \param parent2 Second parent of the ZipNode
     * \param parent_stack1 Function stack with all lambdas between the parent and this node for first DIA
     * \param parent_stack2 Function stack with all lambdas between the parent and this node for second DIA
     * \param zip_function Zip function used to zip elements.
     */
    TwoZipNode(Context& ctx,
               DIANode<ParentInput1>* parent1,
               DIANode<ParentInput2>* parent2,
               ParentStack1& parent_stack1,
               ParentStack2& parent_stack2,
               ZipFunction zip_function)
        : DOpNode<ValueType>(ctx, { parent1, parent2 }),
          zip_function_(zip_function)
    {
        // Hook PreOp(s)
        auto pre_op1_fn = [=](const ZipArg0& input) {
                              emit1_(input);
                          };
        auto pre_op2_fn = [=](const ZipArg1& input) {
                              emit2_(input);
                          };

        // close the function stacks with our pre ops and register it at parent
        // nodes for output
        auto lop_chain1 = parent_stack1.push(pre_op1_fn).emit();
        auto lop_chain2 = parent_stack2.push(pre_op2_fn).emit();

        parent1->RegisterChild(lop_chain1);
        parent2->RegisterChild(lop_chain2);

        // Setup Emitters
        for (size_t i = 0; i < num_dias_; ++i) {
            id_[i] = context_.get_data_manager().AllocateDIA();
        }
        emit1_ = context_.get_data_manager().
                 template GetLocalEmitter<ZipArg0>(id_[0]);
        emit2_ = context_.get_data_manager().
                 template GetLocalEmitter<ZipArg1>(id_[1]);
    }

    /*!
     * Actually executes the zip operation. Uses the member functions PreOp,
     * MainOp and PostOp.
     */
    void Execute() override {
        MainOp();
        // get data from data manager
        auto it1 = context_.get_data_manager().
                   template GetIterator<ZipArg0>(id_[0]);
        auto it2 = context_.get_data_manager().
                   template GetIterator<ZipArg1>(id_[1]);
        do {
            it1.WaitForMore();
            it2.WaitForMore();
            // Iterate over smaller DIA
            while (it1.HasNext() && it2.HasNext()) {
                auto item = std::make_pair(it1.Next(), it2.Next());
                for (auto func : DIANode<ValueType>::callbacks_) {
                    func(item);
                }
            }
        } while (!it1.IsClosed() && !it2.IsClosed());
    }

    /*!
     * Creates empty stack.
     */
    auto ProduceStack() {
        // Hook PostOp
        auto post_op_fn = [=](ValueType elem, auto emit_func) {
                              return PostOp(elem, emit_func);
                          };

        return MakeFunctionStack<ValueType>(post_op_fn);
    }

    /*!
     * Returns "[ZipNode]" as a string.
     * \return "[ZipNode]"
     */
    std::string ToString() override {
        return "[ZipNode]";
    }

private:
    //! Zip function
    ZipFunction zip_function_;

    //! Number of storage DIAs backing
    static const size_t num_dias_ = 2;

    //! Ids of storage DIAs
    std::array<data::DIAId, num_dias_> id_;

    //! Emitter
    data::Emitter<ZipArg0> emit1_;
    data::Emitter<ZipArg1> emit2_;

    //!Receive elements from other workers.
    void MainOp() {
        //net::Group flow_group = context_.get_flow_net_group();
        net::FlowControlChannel& channel = context_.get_flow_control_channel();

        data::Manager &data_manager = context_.get_data_manager();
        size_t workers = context_.number_worker();

        // Offsets to declare which target gets which block
        std::vector<size_t> blocks(num_dias_, 0);

        for (size_t i = 0; i < num_dias_; ++i) {
            size_t numElems = data_manager.GetNumElements(id_[i]);
            //net::PrefixSum(flow_group, prefix);
            size_t prefixNumElems = channel.PrefixSum(numElems, common::SumOp<ValueType>(), false);
            //flow_group.TotalSum(prefix);
            size_t totalNumElems = channel.AllReduce(numElems);
            //size_t total = data_manager.GetNumElements(id_[i]);
            size_t per_pe = totalNumElems / workers;
            // offsets for scattering
            std::vector<size_t> offsets;

            // first idx allocated to this pe
            size_t offset_idx = context_.rank() * per_pe;

            // move element to left neighbor workers
            size_t move_to_left = std::min(offset_idx-prefixNumElems, numElems);
            if (move_to_left > 0) {
                size_t target_rank = context_.rank()-1;
                while (target_rank >= 0) {
                    size_t current_move = std::min(move_to_left, per_pe);
                    move_to_left -= current_move;
                    if (current_move == 0)
                        LOG << "send nothing to target: "
                            << target_rank;
                    else
                        LOG << "["
                            << ((target_rank * per_pe + per_pe - 1) - current_move + 1)
                            << ", "
                            << (target_rank * per_pe + per_pe - 1)
                            << "] "
                            << "to target: "
                            << target_rank;
                    --target_rank;
                }
            }

            // move element to right neighbor workers
            size_t move_to_right = std::min((prefixNumElems+numElems) - (offset_idx+per_pe-1), numElems);
            if (move_to_right > 0) {
                size_t target_rank = context_.rank()+1;
                while (target_rank < workers) {
                    size_t current_move = std::min(move_to_right, per_pe);
                    move_to_right -= current_move;
                    if (current_move == 0)
                        LOG << "send nothing to target: "
                            << target_rank;
                    else
                        LOG << "["
                            << (target_rank * per_pe)
                            << ", "
                            << ((target_rank * per_pe) + current_move - 1)
                            << "] "
                            << "to target: "
                            << target_rank;
                    ++target_rank;
                }
            }

            // TODO(ms): offsets still unfilled
            // scatter data
            // TODO(ms): target chain id must be replaced, currently source chain id used
            data_manager.Scatter(id_[i], id_[i], offsets);
        }
    }

    //! Use the ZipFunction to Zip workers
    template <typename Emitter>
    void PostOp(ValueType input, Emitter emit_func) {
        emit_func(zip_function_(input.first, input.second));
    }
};

template <typename CurrentType, typename Stack>
template <typename ZipFunction, typename SecondDIA>
auto DIARef<CurrentType, Stack>::Zip(
    const ZipFunction &zip_function, SecondDIA second_dia) {

    using ZipResult
              = typename common::FunctionTraits<ZipFunction>::result_type;

    using ZipResultNode
              = TwoZipNode<ZipResult, Stack, typename SecondDIA::Stack,
                           ZipFunction>;

    auto zip_node
        = std::make_shared<ZipResultNode>(node_->get_context(),
                                          node_.get(),
                                          second_dia.get_node(),
                                          local_stack_,
                                          second_dia.get_stack(),
                                          zip_function);

    auto zip_stack = zip_node->ProduceStack();

    return DIARef<ZipResultNode, decltype(zip_stack)>(
        std::move(zip_node), zip_stack);
}

} // namespace api
} // namespace c7a

//! \}

#endif // !C7A_API_ZIP_NODE_HEADER

/******************************************************************************/
