/*******************************************************************************
 * thrill/api/zip_pad.hpp
 *
 * DIANode for a zip operation. Performs the actual zip operation
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_ZIP_PAD_HEADER
#define THRILL_API_ZIP_PAD_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <array>
#include <functional>
#include <string>
#include <tuple>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * A DIANode which performs a ZipPadded operation. ZipPadded combines two DIAs
 * element-by-element. The ZipPadNode stores the zip_function UDF. The chainable
 * LOps are stored in the Stack.
 *
 * <pre>
 *                ParentStack0 ParentStack1
 *                 +--------+   +--------+
 *                 |        |   |        |  A ParentStackX is called with
 *                 |        |   |        |  ParentInputX, and must deliver
 *                 |        |   |        |  a ZipPaddedArgX item.
 *               +-+--------+---+--------+-+
 *               | | PreOp0 |   | PreOp1 | |
 *               | +--------+   +--------+ |
 *    DIA<T> --> |        ZipPadded        |
 *               |        +-------+        |
 *               |        |PostOp |        |
 *               +--------+-------+--------+
 *                        |       | New DIA<T>::stack_ is started
 *                        |       | with PostOp to chain next nodes.
 *                        +-------+
 * </pre>
 *
 * \tparam ValueType Output type of the ZipPadded operation.
 *
 * \tparam ParentDIA0 Function stack, which contains the chained lambdas
 * between the last and this DIANode for first input DIA.
 *
 * \tparam ParentDIA1 Function stack, which contains the chained lambdas
 * between the last and this DIANode for second input DIA.
 *
 * \tparam ZipFunction Type of the ZipFunction.
 */
template <typename ValueType, typename ZipFunction,
          typename ParentDIA0, typename ... ParentDIAs>
class ZipPadNode final : public DOpNode<ValueType>
{
    static const bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

    template <size_t Index>
    using ZipArgN =
              typename common::FunctionTraits<ZipFunction>::template arg_plain<Index>;
    using ZipArgs =
              typename common::FunctionTraits<ZipFunction>::args;

public:
    /*!
     * Constructor for a ZipNode.
     */
    ZipPadNode(const ZipFunction& zip_function,
               StatsNode* stats_node,
               const ZipArgs& padding,
               const ParentDIA0& parent0,
               const ParentDIAs& ... parents)
        : DOpNode<ValueType>(
              parent0.ctx(), { parent0.node(), parents.node() ... },
              stats_node),
          zip_function_(zip_function),
          padding_(padding)
    {
        // allocate files.
        files_.reserve(num_inputs_);
        for (size_t i = 0; i < num_inputs_; ++i)
            files_.emplace_back(context_.GetFile());

        for (size_t i = 0; i < num_inputs_; ++i)
            writers_[i] = files_[i].GetWriter();

        // Hook PreOp(s)
        common::VarCallForeachIndex(
            [this](auto index, auto parent) {

                // get the ZipFunction's argument for this index
                using ZipArg = ZipArgN<decltype(index)::index>;

                // check that the parent's type is convertible to the
                // ZipFunction argument.
                static_assert(
                    std::is_convertible<
                        typename decltype(parent)::ValueType, ZipArg
                        >::value,
                    "ZipFunction argument does not match input DIA");

                auto pre_op_fn = [=](const ZipArg& input) {
                                     writers_[index].PutItem(input);
                                 };

                // close the function stacks with our pre ops and register it at
                // parent nodes for output
                auto lop_chain = parent.stack().push(pre_op_fn).emit();

                parent.node()->RegisterChild(lop_chain, this->type());
            },
            parent0, parents ...);
    }

    void Execute() final {
        MainOp();
    }

    void PushData(bool consume) final {
        size_t result_count = 0;

        if (result_size_ != 0) {
            // get inbound readers from all Streams
            std::array<data::CatStream::CatReader, num_inputs_> readers;
            for (size_t i = 0; i < num_inputs_; ++i)
                readers[i] = streams_[i]->OpenCatReader(consume);

            while (AnyHasNext(readers)) {
                auto v = common::VarMapEnumerate<num_inputs_>(
                    [this, &readers](auto index) {
                        using ZipArg = ZipArgN<decltype(index)::index>;
                        if (!readers[index].HasNext()) {
                                // take padding_ if next is not available.
                            return std::get<decltype(index)::index>(padding_);
                        }
                        return readers[index].template Next<ZipArg>();
                    });

                this->PushItem(common::ApplyTuple(zip_function_, v));
                ++result_count;
            }
        }

        sLOG << "Zip: result_count" << result_count;
    }

    void Dispose() final { }

private:
    //! Zip function
    ZipFunction zip_function_;

    //! Number of storage DIAs backing
    static const size_t num_inputs_ = 1 + sizeof ... (ParentDIAs);

    //! padding for shorter DIAs
    ZipArgs padding_;

    //! Files for intermediate storage
    // std::array<data::File, num_inputs_> files_ {
    //     { context_.GetFile(), context_.GetFile() }
    // };
    std::vector<data::File> files_;

    //! Writers to intermediate files
    std::array<data::File::Writer, num_inputs_> writers_;

    //! Array of inbound CatStreams
    std::array<data::CatStreamPtr, num_inputs_> streams_;

    //! \name Variables for Calculating Exchange
    //! \{

    //! prefix sum over the number of items in workers
    std::array<size_t, num_inputs_> dia_size_prefixsum_;

    //! longest size of Zipped inputs
    size_t result_size_;

    //! \}

    //! Scatter items from DIA "Index" to other workers if necessary.
    template <size_t Index>
    void DoScatter() {
        const size_t workers = context_.num_workers();

        size_t local_begin = dia_size_prefixsum_[Index] - files_[Index].num_items();
        size_t local_end = dia_size_prefixsum_[Index];
        size_t local_size = local_end - local_begin;

        //! number of elements per worker (rounded up)
        size_t per_pe = (result_size_ + workers - 1) / workers;
        //! offsets for scattering
        std::vector<size_t> offsets(workers, 0);

        size_t offset = 0;
        size_t count = std::min(per_pe - local_begin % per_pe, local_size);
        size_t target = local_begin / per_pe;

        sLOG << "input" << Index
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
            LOG << "input " << Index << " offsets[" << i << "] = " << offsets[i];
        }

        //! target stream id
        streams_[Index] = context_.GetNewCatStream();

        //! scatter elements to other workers, if necessary
        using ZipArg = ZipArgN<Index>;
        streams_[Index]->template Scatter<ZipArg>(files_[Index], offsets);
    }

    //! Receive elements from other workers.
    void MainOp() {
        for (size_t i = 0; i != writers_.size(); ++i)
            writers_[i].Close();

        // first: calculate total size of the DIAs to Zip

        net::FlowControlChannel& channel = context_.flow_control_channel();

        //! total number of items in DIAs over all workers
        std::array<size_t, num_inputs_> dia_total_size;

        for (size_t in = 0; in < num_inputs_; ++in) {
            //! number of elements of this worker
            size_t dia_local_size = files_[in].num_items();
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
            *std::max_element(dia_total_size.begin(), dia_total_size.end());

        // perform scatters to exchange data, with different types.
        if (result_size_ != 0) {
            common::VarCallEnumerate<num_inputs_>(
                [=](auto index) {
                    this->DoScatter<decltype(index)::index>();
                });
        }
    }

    //! helper for PushData() which checks all inputs
    static bool AnyHasNext(
        std::array<data::CatStream::CatReader, num_inputs_>& readers) {
        for (size_t i = 0; i < num_inputs_; ++i) {
            if (readers[i].HasNext()) return true;
        }
        return false;
    }
};

/*!
 * ZipPad is a DOp, which Zips any number of DIAs in style of functional
 * programming. The zip_function is used to zip the i-th elements of all input
 * DIAs together to form the i-th element of the output DIA. The type of the
 * output DIA can be inferred from the zip_function. The output DIA's length is
 * the *maximum* of all input DIAs, shorter DIAs are padded with
 * default-constructed items.
 *
 * \tparam ZipFunction Type of the zip_function. This is a function with two
 * input elements, both of the local type, and one output element, which is
 * the type of the Zip node.
 *
 * \param zip_function Zip function, which zips two elements together
 *
 * \param second_dia DIA, which is zipped together with the original DIA.
 */
template <typename ZipFunction, typename FirstDIA, typename ... DIAs>
auto ZipPad(const ZipFunction &zip_function,
            const FirstDIA &first_dia, const DIAs &... dias) {

    using VarForeachExpander = int[];

    first_dia.AssertValid();
    (void)VarForeachExpander {
        (dias.AssertValid(), 0) ...
    };

    static_assert(
        std::is_convertible<
            typename FirstDIA::ValueType,
            typename common::FunctionTraits<ZipFunction>::template arg<0>
            >::value,
        "ZipFunction has the wrong input type in DIA 0");

    using ZipResult =
              typename common::FunctionTraits<ZipFunction>::result_type;

    using ZipArgs =
              typename common::FunctionTraits<ZipFunction>::args;

    using ZipPadNode
              = api::ZipPadNode<ZipResult, ZipFunction, FirstDIA, DIAs ...>;

    StatsNode* stats_node = first_dia.AddChildStatsNode("ZipPadded", DIANodeType::DOP);
    (void)VarForeachExpander {
        (dias.AppendChildStatsNode(stats_node), 0) ...
    };

    auto zip_node
        = std::make_shared<ZipPadNode>(
        zip_function, stats_node, ZipArgs(), first_dia, dias ...);

    return DIA<ZipResult>(zip_node, { stats_node });
}

/*!
 * ZipPad is a DOp, which Zips any number of DIAs in style of functional
 * programming. The zip_function is used to zip the i-th elements of all input
 * DIAs together to form the i-th element of the output DIA. The type of the
 * output DIA can be inferred from the zip_function. The output DIA's length is
 * the *maximum* of all input DIAs, shorter DIAs are padded with items given by
 * the padding parameter.
 *
 * \tparam ZipFunction Type of the zip_function. This is a function with two
 * input elements, both of the local type, and one output element, which is
 * the type of the Zip node.
 *
 * \param zip_function Zip function, which zips two elements together
 *
 * \param padding std::tuple<args> of padding sentinels delivered to ZipFunction
 * if an input dia is too short.
 *
 * \param second_dia DIA, which is zipped together with the original DIA.
 */
template <typename ZipFunction, typename FirstDIA, typename ... DIAs>
auto ZipPadding(
    const ZipFunction &zip_function,
    const typename common::FunctionTraits<ZipFunction>::args & padding,
    const FirstDIA &first_dia, const DIAs &... dias) {

    using VarForeachExpander = int[];

    first_dia.AssertValid();
    (void)VarForeachExpander {
        (dias.AssertValid(), 0) ...
    };

    static_assert(
        std::is_convertible<
            typename FirstDIA::ValueType,
            typename common::FunctionTraits<ZipFunction>::template arg<0>
            >::value,
        "ZipFunction has the wrong input type in DIA 0");

    using ZipResult =
              typename common::FunctionTraits<ZipFunction>::result_type;

    using ZipPadNode
              = api::ZipPadNode<ZipResult, ZipFunction, FirstDIA, DIAs ...>;

    StatsNode* stats_node = first_dia.AddChildStatsNode("ZipPadded", DIANodeType::DOP);
    (void)VarForeachExpander {
        (dias.AppendChildStatsNode(stats_node), 0) ...
    };

    auto zip_node
        = std::make_shared<ZipPadNode>(
        zip_function, stats_node, padding, first_dia, dias ...);

    return DIA<ZipResult>(zip_node, { stats_node });
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_ZIP_PAD_HEADER

/******************************************************************************/
