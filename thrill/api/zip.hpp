/*******************************************************************************
 * thrill/api/zip.hpp
 *
 * DIANode for a zip operation. Performs the actual zip operation
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_ZIP_HEADER
#define THRILL_API_ZIP_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <functional>
#include <string>
#include <vector>

namespace thrill {
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
          typename ParentDIARef0, typename ParentDIARef1,
          typename ZipFunction>
class TwoZipNode : public DOpNode<ValueType>
{
    static const bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

    template <typename Type>
    using FunctionTraits = common::FunctionTraits<Type>;

    using ZipArg0 =
              typename FunctionTraits<ZipFunction>::template arg_plain<0>;
    using ZipArg1 =
              typename FunctionTraits<ZipFunction>::template arg_plain<1>;
    using ZipResult =
              typename FunctionTraits<ZipFunction>::result_type;

public:
    /*!
     * Constructor for a ZipNode.
     *
     * \param parent0 First parent of the ZipNode
     * \param parent1 Second parent of the ZipNode
     * \param zip_function Zip function used to zip elements.
     */
    TwoZipNode(const ParentDIARef0& parent0,
               const ParentDIARef1& parent1,
               ZipFunction zip_function,
               StatsNode* stats_node)
        : DOpNode<ValueType>(parent0.ctx(), { parent0.node(), parent1.node() }, "ZipNode", stats_node),
          zip_function_(zip_function)
    {
        // Hook PreOp(s)
        auto pre_op0_fn = [=](const ZipArg0& input) {
                              writers_[0](input);
                          };
        auto pre_op1_fn = [=](const ZipArg1& input) {
                              writers_[1](input);
                          };

        // close the function stacks with our pre ops and register it at parent
        // nodes for output
        auto lop_chain0 = parent0.stack().push(pre_op0_fn).emit();
        auto lop_chain1 = parent1.stack().push(pre_op1_fn).emit();

        parent0.node()->RegisterChild(lop_chain0, this->type());
        parent1.node()->RegisterChild(lop_chain1, this->type());
    }

    ~TwoZipNode() { }

    /*!
     * Actually executes the zip operation. Uses the member functions PreOp,
     * MainOp and PostOp.
     */
    void Execute() final {
        MainOp();
    }

    void PushData() final {
        size_t result_count = 0;

        if (result_size_ != 0) {
            // get inbound readers from all Channels
            std::vector<data::Channel::CachingConcatReader> readers {
                channels_[0]->OpenCachingReader(), channels_[1]->OpenCachingReader()
            };

            while (readers[0].HasNext() && readers[1].HasNext()) {
                ZipArg0 i0 = readers[0].Next<ZipArg0>();
                ZipArg1 i1 = readers[1].Next<ZipArg1>();
                this->PushItem(zip_function_(i0, i1));
                ++result_count;
            }

            // Empty out readers. If they have additional items, this is
            // necessary for the CachingBlockQueueSource, as it has to cache the
            // additional blocks -tb. TODO(tb): this is weird behaviour.
            // yes ... weird - ts
            while (readers[0].HasNext())
                readers[0].Next<ZipArg0>();

            while (readers[1].HasNext())
                readers[1].Next<ZipArg1>();

            channels_[0]->Close();
            channels_[1]->Close();
            this->WriteChannelStats(channels_[0]);
            this->WriteChannelStats(channels_[1]);
        }

        sLOG << "Zip: result_count" << result_count;
    }

    void Dispose() final { }

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
    std::string ToString() final {
        return "[ZipNode]";
    }

private:
    //! Zip function
    ZipFunction zip_function_;

    //! Number of storage DIAs backing
    static const size_t num_inputs_ = 2;

    //! Files for intermediate storage
    std::array<data::File, num_inputs_> files_ {
        { context_.GetFile(), context_.GetFile() }
    };

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

    //! minimum total size of Zipped inputs
    size_t result_size_;

    //! \}

    //! Scatter items from DIA "in" to other workers if necessary.
    template <typename ZipArgNum>
    void DoScatter(size_t in) {
        const size_t workers = context_.num_workers();

        size_t local_begin =
            std::min(result_size_,
                     dia_size_prefixsum_[in] - files_[in].NumItems());
        size_t local_end = std::min(result_size_, dia_size_prefixsum_[in]);
        size_t local_size = local_end - local_begin;

        //! number of elements per worker (rounded up)
        size_t per_pe = (result_size_ + workers - 1) / workers;
        //! offsets for scattering
        std::vector<size_t> offsets(workers, 0);

        size_t offset = 0;
        size_t count = std::min(per_pe - local_begin % per_pe, local_size);
        size_t target = local_begin / per_pe;

        sLOG << "input" << in
             << "local_begin" << local_begin << "local_end" << local_end
             << "local_size" << local_size
             << "result_size_" << result_size_ << "pre_pe" << per_pe
             << "count" << count << "target" << target;

        //! do as long as there are elements to be scattered, includes elements
        //! kept on this worker
        while (local_size > 0 && target < workers) {
            offsets[target] = offset + count;
            local_begin += count;
            local_size -= count;
            offset += count;
            count = std::min(per_pe - local_begin % per_pe, local_size);
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

        //! target channel id
        channels_[in] = context_.GetNewChannel();

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

        //! total number of items in DIAs over all workers
        std::array<size_t, num_inputs_> dia_total_size;

        for (size_t in = 0; in < num_inputs_; ++in) {
            //! number of elements of this worker
            size_t dia_local_size = files_[in].NumItems();
            sLOG << "input" << in << "dia_local_size" << dia_local_size;

            //! inclusive prefixsum of number of elements: we have items from
            //! [dia_size_prefixsum - local_size, dia_size_prefixsum).
            dia_size_prefixsum_[in] = channel.PrefixSum(dia_local_size);

            //! total number of elements, over all worker. TODO(tb): use a
            //! Broadcast from the last node instead.
            dia_total_size[in] = channel.AllReduce(dia_local_size);
        }

        // return only the minimum size of all DIAs.
        result_size_ =
            *std::min_element(dia_total_size.begin(), dia_total_size.end());

        // perform scatters to exchange data, with different types.
        if (result_size_ != 0) {
            DoScatter<ZipArg0>(0);
            DoScatter<ZipArg1>(1);
        }
    }
};

template <typename ValueType, typename Stack>
template <typename ZipFunction, typename SecondDIA>
auto DIARef<ValueType, Stack>::Zip(
    SecondDIA second_dia, const ZipFunction &zip_function) const {
    assert(IsValid());
    assert(second_dia.IsValid());

    using ZipResult
              = typename FunctionTraits<ZipFunction>::result_type;

    using ZipResultNode
              = TwoZipNode<ZipResult, DIARef, SecondDIA, ZipFunction>;

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<ZipFunction>::template arg<0>
            >::value,
        "ZipFunction has the wrong input type in DIA 0");

    static_assert(
        std::is_convertible<
            typename SecondDIA::ValueType,
            typename FunctionTraits<ZipFunction>::template arg<1>
            >::value,
        "ZipFunction has the wrong input type in DIA 1");

    StatsNode* stats_node = AddChildStatsNode("Zip", DIANodeType::DOP);
    second_dia.AppendChildStatsNode(stats_node);
    auto zip_node
        = std::make_shared<ZipResultNode>(*this,
                                          second_dia,
                                          zip_function,
                                          stats_node);

    auto zip_stack = zip_node->ProduceStack();

    return DIARef<ZipResult, decltype(zip_stack)>(
        zip_node,
        zip_stack,
        { stats_node });
}

//! \}

} // namespace api
} // namespace thrill

//! \}
#endif // !THRILL_API_ZIP_HEADER

/******************************************************************************/
