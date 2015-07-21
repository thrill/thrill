/*******************************************************************************
 * c7a/api/zip.hpp
 *
 * DIANode for a reduce operation. Performs the actual reduce operation
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_ZIP_HEADER
#define C7A_API_ZIP_HEADER

#include <c7a/api/dop_node.hpp>
#include <c7a/common/logger.hpp>
#include <c7a/net/collective_communication.hpp>

#include <algorithm>
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
          typename ParentStack0, typename ParentStack1,
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

    using ParentInput0 = typename ParentStack0::Input;
    using ParentInput1 = typename ParentStack1::Input;

public:
    /*!
     * Constructor for a ZipNode.
     *
     * \param ctx Reference to the Context, which gives iterators for data
     * \param parent0 First parent of the ZipNode
     * \param parent1 Second parent of the ZipNode
     * \param parent_stack0 Function stack with all lambdas between the parent and this node for first DIA
     * \param parent_stack1 Function stack with all lambdas between the parent and this node for second DIA
     * \param zip_function Zip function used to zip elements.
     */
    TwoZipNode(Context& ctx,
               const std::shared_ptr<DIANode<ParentInput0> >& parent0,
               const std::shared_ptr<DIANode<ParentInput1> >& parent1,
               const ParentStack0& parent_stack0,
               const ParentStack1& parent_stack1,
               ZipFunction zip_function)
        : DOpNode<ValueType>(ctx, { parent0, parent1 }, "ZipNode"),
          zip_function_(zip_function)
    {
        // // Hook PreOp(s)
        auto pre_op0_fn = [=](const ZipArg0& input) {
                              writers_[0](input);
                          };
        auto pre_op1_fn = [=](const ZipArg1& input) {
                              writers_[1](input);
                          };

        // close the function stacks with our pre ops and register it at parent
        // nodes for output
        auto lop_chain0 = parent_stack0.push(pre_op0_fn).emit();
        auto lop_chain1 = parent_stack1.push(pre_op1_fn).emit();

        parent0->RegisterChild(lop_chain0);
        parent1->RegisterChild(lop_chain1);
    }

    /*!
     * Actually executes the zip operation. Uses the member functions PreOp,
     * MainOp and PostOp.
     */
    void Execute() override {
        this->StartExecutionTimer();
        MainOp();

        // get data from data manager
        auto it0 = files_[0].GetReader();
        auto it1 = files_[1].GetReader();

        // Iterate over smaller DIA
        while (it0.HasNext() && it1.HasNext()) {
            ZipArg0 i0 = it0.Next<ZipArg0>();
            ZipArg1 i1 = it1.Next<ZipArg1>();
            ValueType v = zip_function_(i0, i1);
            for (auto func : DIANode<ValueType>::callbacks_) {
                func(v);
            }
        }

        this->StopExecutionTimer();
    }

    /*!
     * Creates empty stack.
     */
    auto ProduceStack() {
        // Hook PostOp
        return FunctionStack<ValueType>();
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
    static const size_t num_inputs_ = 2;

    //! Files for intermediate storage
    std::array<data::File, num_inputs_> files_;

    //! Writers to intermediate files
    std::array<data::File::Writer, num_inputs_> writers_  {
        { files_[0].GetWriter(), files_[1].GetWriter() }
    };

    //! Receive elements from other workers.
    void MainOp() {
        for (size_t i = 0; i != writers_.size(); ++i) {
            writers_[i].Close();
        }

        net::FlowControlChannel& channel = context_.flow_control_channel();
        //data::Manager& data_manager = context_.data_manager();
        size_t workers = context_.number_worker();

        for (size_t i = 0; i < num_inputs_; ++i) {
            //! number of elements of this worker
            size_t numElems = files_[i].NumItems();
            //! exclusive prefixsum of number of elements
            size_t prefixNumElems = channel.PrefixSum(numElems, common::SumOp<ValueType>(), false);
            //! total number of elements, over all worker
            size_t totalNumElems = channel.AllReduce(numElems);
            //! number of elements per worker
            size_t per_pe = totalNumElems / workers;
            //! offsets for scattering
            std::vector<size_t> offsets(num_inputs_, 0);

            size_t offset = 0;
            size_t count = std::min(per_pe - prefixNumElems % per_pe, numElems);
            size_t target = prefixNumElems / per_pe;

            //! do as long as there are elements to be scattered, includes elements
            //! kept on this worker
            while (numElems > 0) {
                offsets[target] = offset + count - 1;
                prefixNumElems += count;
                numElems -= count;
                offset += count;
                count = std::min(per_pe - prefixNumElems % per_pe, numElems);
                target++;
            }

            //! fill offset vector, no more scattering here
            for (size_t x = target; x < workers; x++)
                offsets[x] = offsets[x - 1];

            //! target channel id
            //data::ChannelId channelId = data_manager.AllocateChannelId();

            //! scatter elements to other workers, if necessary
            //data_manager.Scatter<ValueType>(id_[i], channelId, offsets);
        }
    }
};

template <typename ValueType, typename Stack>
template <typename ZipFunction, typename SecondDIA>
auto DIARef<ValueType, Stack>::Zip(
    const ZipFunction &zip_function, SecondDIA second_dia) const {

    using ZipResult
              = typename common::FunctionTraits<ZipFunction>::result_type;

    using ZipResultNode
              = TwoZipNode<ZipResult, Stack, typename SecondDIA::Stack,
                           ZipFunction>;

    static_assert(
        std::is_convertible<
            ValueType,
            typename common::FunctionTraits<ZipFunction>::template arg<0>
            >::value,
        "ZipFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            typename SecondDIA::ItemType,
            typename common::FunctionTraits<ZipFunction>::template arg<0>
            >::value,
        "ZipFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            ZipResult,
            typename common::FunctionTraits<ZipFunction>::result_type
            >::value,
        "ZipFunction has the wrong output type.");

    auto zip_node
        = std::make_shared<ZipResultNode>(node_->context(),
                                          node_,
                                          second_dia.node(),
                                          stack_,
                                          second_dia.stack(),
                                          zip_function);

    auto zip_stack = zip_node->ProduceStack();

    return DIARef<ZipResultNode, decltype(zip_stack)>(
        zip_node, zip_stack);
}

//! \}

} // namespace api
} // namespace c7a

#endif // !C7A_API_ZIP_HEADER

/******************************************************************************/
