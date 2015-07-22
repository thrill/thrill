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
#include <c7a/data/channel_multiplexer.hpp>
#include <c7a/data/file.hpp>
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
    using ZipResult =
              typename common::FunctionTraits<ZipFunction>::result_type;

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

    ~TwoZipNode() { }

    /*!
     * Actually executes the zip operation. Uses the member functions PreOp,
     * MainOp and PostOp.
     */
    void Execute() override {
        this->StartExecutionTimer();
        MainOp();

        if (dia_min_size_ != 0) {
            // get inbound readers from all Channels
            std::vector<data::Channel::ConcatReader> readers {
                channels_[0]->OpenReader(), channels_[1]->OpenReader()
            };

            size_t result_count = 0;

            while (readers[0].HasNext() && readers[1].HasNext()) {
                ZipArg0 i0 = readers[0].Next<ZipArg0>();
                ZipArg1 i1 = readers[1].Next<ZipArg1>();
                ValueType v = zip_function_(i0, i1);
                for (auto func : DIANode<ValueType>::callbacks_) {
                    func(v);
                }
                ++result_count;
            }

            sLOG << "result_count" << result_count;
        }

        this->StopExecutionTimer();
    }

    void PushData() override { }

    void Dispose() override { }

    /*!
     * Creates empty stack.
     */
    auto ProduceStack() {
        // Hook PostOp
        return FunctionStack<ZipResult>();
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

    //! Array of inbound Channels
    std::array<data::ChannelPtr, num_inputs_> channels_;

    //! \name Variables for Calculating Exchange
    //! \{

    //! prefix sum over the number of items in workers
    std::array<size_t, num_inputs_> dia_size_prefixsum_;

    //! total number of items in dia over all workers
    std::array<size_t, num_inputs_> dia_total_size_;

    //! minimum total size of Zipped inputs
    size_t dia_min_size_;

    //! \}

    //! Scatter items from DIA "in" to other workers if necessary.
    template <typename ZipArgNum>
    void DoScatter(size_t in) {
        const size_t workers = context_.number_worker();

        size_t local_size = files_[in].NumItems();
        size_t size_prefixsum = dia_size_prefixsum_[in];
        const size_t& total_size = dia_total_size_[in];

        //! number of elements per worker
        size_t per_pe = total_size / workers;
        //! offsets for scattering
        std::vector<size_t> offsets(workers, 0);

        sLOG << "input" << in << "dia_size_prefixsum" << size_prefixsum
             << "dia_total_size" << total_size;

        size_t offset = 0;
        size_t count = std::min(per_pe - size_prefixsum % per_pe, local_size);
        size_t target = size_prefixsum / per_pe;

        //! do as long as there are elements to be scattered, includes elements
        //! kept on this worker
        while (local_size > 0 && target < workers) {
            offsets[target] = offset + count;
            size_prefixsum += count;
            local_size -= count;
            offset += count;
            count = std::min(per_pe - size_prefixsum % per_pe, local_size);
            ++target;
        }

        //! fill offset vector, no more scattering here
        while (target < workers) {
            offsets[target] = target == 0 ? 0 : offsets[target - 1];
            ++target;
        }

        for (size_t i = 0; i != offsets.size(); ++i) {
            LOG << "input " << in << " offsets[" << i << "] = " << offsets[i];
        }

        data::Manager& data_manager = context_.data_manager();

        //! target channel id
        channels_[in] = data_manager.GetNewChannel();

        //! scatter elements to other workers, if necessary
        channels_[in]->template Scatter<ZipArgNum>(files_[in], offsets);
    }

    //! Receive elements from other workers.
    void MainOp() {
        for (size_t i = 0; i != writers_.size(); ++i) {
            writers_[i].Close();
        }

        // first: calculate total size of the DIAs to Zip

        net::FlowControlChannel& channel = context_.flow_control_channel();

        for (size_t in = 0; in < num_inputs_; ++in) {
            //! number of elements of this worker
            size_t dia_local_size = files_[in].NumItems();
            sLOG << "input" << in << "dia_local_size" << dia_local_size;

            //! exclusive prefixsum of number of elements
            dia_size_prefixsum_[in] =
                channel.PrefixSum(dia_local_size, common::SumOp<size_t>(), false);

            //! total number of elements, over all worker. TODO(tb): use a
            //! Broadcast from the last node instead.
            dia_total_size_[in] = channel.AllReduce(dia_local_size);
        }

        // return only the minimum size of all DIAs
        dia_min_size_ =
            *std::min_element(dia_total_size_.begin(), dia_total_size_.end());

        // perform scatters to exchange data, with different types.

        if (dia_min_size_ != 0) {
            DoScatter<ZipArg0>(0);
            DoScatter<ZipArg1>(1);
        }
    }
};

template <typename ValueType, typename Stack>
template <typename ZipFunction, typename SecondDIA>
auto DIARef<ValueType, Stack>::Zip(
    SecondDIA second_dia, const ZipFunction &zip_function) const {

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
        "ZipFunction has the wrong input type in DIA 0");

    static_assert(
        std::is_convertible<
            typename SecondDIA::ValueType,
            typename common::FunctionTraits<ZipFunction>::template arg<1>
            >::value,
        "ZipFunction has the wrong input type in DIA 1");

    auto zip_node
        = std::make_shared<ZipResultNode>(node_->context(),
                                          node_,
                                          second_dia.node(),
                                          stack_,
                                          second_dia.stack(),
                                          zip_function);

    auto zip_stack = zip_node->ProduceStack();

    return DIARef<ZipResult, decltype(zip_stack)>(zip_node, zip_stack);
}

//! \}

} // namespace api
} // namespace c7a

//! \}
#endif // !C7A_API_ZIP_HEADER

/******************************************************************************/
