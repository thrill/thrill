/*******************************************************************************
 * thrill/api/zip.hpp
 *
 * DIANode for a zip operation. Performs the actual zip operation
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_ZIP_HEADER
#define THRILL_API_ZIP_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/meta.hpp>
#include <thrill/common/string.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <array>
#include <functional>
#include <tuple>
#include <vector>

namespace thrill {
namespace api {

/*!
 * A DIANode which performs a Zip operation. Zip combines two DIAs
 * element-by-element. The ZipNode stores the zip_function UDF. The chainable
 * LOps are stored in the Stack.
 *
 * <pre>
 *                ParentStack0 ParentStack1
 *                 +--------+   +--------+
 *                 |        |   |        |  A ParentStackX is called with
 *                 |        |   |        |  ParentInputX, and must deliver
 *                 |        |   |        |  a ZipArgX item.
 *               +-+--------+---+--------+-+
 *               | | PreOp0 |   | PreOp1 | |
 *               | +--------+   +--------+ |
 *    DIA<T> --> |           Zip           |
 *               |        +-------+        |
 *               |        |PostOp |        |
 *               +--------+-------+--------+
 *                        |       | New DIA<T>::stack_ is started
 *                        |       | with PostOp to chain next nodes.
 *                        +-------+
 * </pre>
 *
 * \tparam ValueType Output type of the Zip operation.
 *
 * \tparam ParentDIA0 Function stack, which contains the chained lambdas
 * between the last and this DIANode for first input DIA.
 *
 * \tparam ParentDIA1 Function stack, which contains the chained lambdas
 * between the last and this DIANode for second input DIA.
 *
 * \tparam ZipFunction Type of the ZipFunction.
 *
 * \ingroup api_layer
 */
template <typename ValueType, typename ZipFunction,
          bool Pad, bool UnequalCheck, bool NoRebalance, size_t kNumInputs>
class ZipNode final : public DOpNode<ValueType>
{
    static constexpr bool debug = false;

    //! Set this variable to true to enable generation and output of stats
    static constexpr bool stats_enabled = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

    template <size_t Index>
    using ZipArgN =
              typename common::FunctionTraits<ZipFunction>::template arg_plain<Index>;
    using ZipArgsTuple =
              typename common::FunctionTraits<ZipFunction>::args_tuple_plain;

public:
    /*!
     * Constructor for a ZipNode.
     */
    template <typename ParentDIA0, typename ... ParentDIAs>
    ZipNode(const ZipFunction& zip_function, const ZipArgsTuple& padding,
            const ParentDIA0& parent0, const ParentDIAs& ... parents)
        : Super(parent0.ctx(), "Zip",
                { parent0.id(), parents.id() ... },
                { parent0.node(), parents.node() ... }),
          zip_function_(zip_function),
          padding_(padding),
          // this weirdness is due to a MSVC2015 parser bug
          parent_stack_empty_(
              std::array<bool, kNumInputs>{
                  { ParentDIA0::stack_empty, (ParentDIAs::stack_empty)... }
              })
    {
        // allocate files.
        files_.reserve(kNumInputs);
        for (size_t i = 0; i < kNumInputs; ++i)
            files_.emplace_back(context_.GetFile(this));

        // Hook PreOp(s)
        common::VariadicCallForeachIndex(
            RegisterParent(this), parent0, parents ...);
    }

    void StartPreOp(size_t parent_index) final {
        writers_[parent_index] = files_[parent_index].GetWriter();
    }

    //! Receive a whole data::File of ValueType, but only if our stack is empty.
    bool OnPreOpFile(const data::File& file, size_t parent_index) final {
        assert(parent_index < kNumInputs);
        if (!parent_stack_empty_[parent_index]) return false;

        // accept file
        assert(files_[parent_index].num_items() == 0);
        files_[parent_index] = file.Copy();
        return true;
    }

    void StopPreOp(size_t parent_index) final {
        LOG << *this << " StopPreOp() parent_index=" << parent_index;
        writers_[parent_index].Close();
    }

    void Execute() final {
        MainOp();
    }

    void PushData(bool consume) final {
        size_t result_count = 0;

        if (result_size_ != 0) {
            if (NoRebalance) {
                // get inbound readers from all Streams
                std::array<data::File::Reader, kNumInputs> readers;
                for (size_t i = 0; i < kNumInputs; ++i)
                    readers[i] = files_[i].GetReader(consume);

                ReaderNext<data::File::Reader> reader_next(*this, readers);

                while (reader_next.HasNext()) {
                    auto v = common::VariadicMapEnumerate<kNumInputs>(reader_next);
                    this->PushItem(common::ApplyTuple(zip_function_, v));
                    ++result_count;
                }
            }
            else {
                // get inbound readers from all Streams
                std::array<data::CatStream::CatReader, kNumInputs> readers;
                for (size_t i = 0; i < kNumInputs; ++i)
                    readers[i] = streams_[i]->GetCatReader(consume);

                ReaderNext<data::CatStream::CatReader> reader_next(*this, readers);

                while (reader_next.HasNext()) {
                    auto v = common::VariadicMapEnumerate<kNumInputs>(reader_next);
                    this->PushItem(common::ApplyTuple(zip_function_, v));
                    ++result_count;
                }
            }
        }

        if (stats_enabled) {
            context_.PrintCollectiveMeanStdev(
                "Zip() result_count", result_count);
        }
    }

    void Dispose() final {
        files_.clear();
    }

private:
    //! Zip function
    ZipFunction zip_function_;

    //! padding for shorter DIAs
    const ZipArgsTuple padding_;

    //! Whether the parent stack is empty
    const std::array<bool, kNumInputs> parent_stack_empty_;

    //! Files for intermediate storage
    std::vector<data::File> files_;

    //! Writers to intermediate files
    data::File::Writer writers_[kNumInputs];

    //! Array of inbound CatStreams
    data::CatStreamPtr streams_[kNumInputs];

    //! \name Variables for Calculating Exchange
    //! \{

    //! exclusive prefix sum over the number of items in workers
    std::array<size_t, kNumInputs> dia_size_prefixsum_;

    //! shortest size of Zipped inputs
    size_t result_size_;

    //! \}

    //! Register Parent PreOp Hooks, instantiated and called for each Zip parent
    class RegisterParent
    {
    public:
        explicit RegisterParent(ZipNode* node) : node_(node) { }

        template <typename Index, typename Parent>
        void operator () (const Index&, Parent& parent) {

            // get the ZipFunction's argument for this index
            using ZipArg = ZipArgN<Index::index>;

            // check that the parent's type is convertible to the ZipFunction
            // argument.
            static_assert(
                std::is_convertible<typename Parent::ValueType, ZipArg>::value,
                "ZipFunction argument does not match input DIA");

            // construct lambda with only the writer in the closure
            data::File::Writer* writer = &node_->writers_[Index::index];
            auto pre_op_fn = [writer](const ZipArg& input) -> void {
                                 writer->Put(input);
                             };

            // close the function stacks with our pre ops and register it at
            // parent nodes for output
            auto lop_chain = parent.stack().push(pre_op_fn).fold();

            parent.node()->AddChild(node_, lop_chain, Index::index);
        }

    private:
        ZipNode* node_;
    };

    //! Scatter items from DIA "Index" to other workers if necessary.
    template <size_t Index>
    void DoScatter() {
        const size_t workers = context_.num_workers();

        size_t local_begin = std::min(result_size_, dia_size_prefixsum_[Index]);
        size_t local_end = std::min(
            result_size_,
            dia_size_prefixsum_[Index] + files_[Index].num_items());
        size_t local_size = local_end - local_begin;

        //! number of elements per worker (rounded up)
        size_t per_pe = (result_size_ + workers - 1) / workers;
        //! offsets for scattering
        std::vector<size_t> offsets(workers + 1, 0);

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
            ++target;
            offsets[target] = offset + count;
            local_begin += count;
            local_size -= count;
            offset += count;
            count = std::min(per_pe - local_begin % per_pe, local_size);
        }

        //! fill offset vector, no more scattering here
        while (target < workers) {
            ++target;
            offsets[target] = target == 0 ? 0 : offsets[target - 1];
        }

        LOG << "offsets[" << Index << "] = " << common::VecToStr(offsets);

        //! target stream id
        streams_[Index] = context_.GetNewCatStream(this);

        //! scatter elements to other workers, if necessary
        using ZipArg = ZipArgN<Index>;
        streams_[Index]->template Scatter<ZipArg>(
            files_[Index], offsets, /* consume */ true);
    }

    //! Receive elements from other workers.
    void MainOp() {
        if (NoRebalance) {
            // no communication: everyone just checks that all input DIAs have
            // the same local size.
            result_size_ = files_[0].num_items();
            for (size_t i = 1; i < kNumInputs; ++i) {
                if (result_size_ != files_[i].num_items()) {
                    die("Zip() input DIA " << i << " partition does not match.");
                }
            }
            return;
        }

        // first: calculate total size of the DIAs to Zip

        using ArraySizeT = std::array<size_t, kNumInputs>;

        //! number of elements of this worker
        ArraySizeT dia_local_size;
        for (size_t i = 0; i < kNumInputs; ++i) {
            dia_local_size[i] = files_[i].num_items();
            sLOG << "input" << i << "dia_local_size" << dia_local_size[i];

            if (stats_enabled) {
                context_.PrintCollectiveMeanStdev(
                    "Zip() local_size", dia_local_size[i]);
            }
        }

        //! inclusive prefixsum of number of elements: we have items from
        //! [dia_size_prefixsum - local_size, dia_size_prefixsum). And get the
        //! total number of items in DIAs, over all worker.
        dia_size_prefixsum_ = dia_local_size;
        ArraySizeT dia_total_size = context_.net.ExPrefixSumTotal(
            dia_size_prefixsum_,
            ArraySizeT(), common::ComponentSum<ArraySizeT>());

        size_t max_dia_total_size =
            *std::max_element(dia_total_size.begin(), dia_total_size.end());

        // return only the minimum size of all DIAs.
        result_size_ =
            Pad ? max_dia_total_size
            : *std::min_element(dia_total_size.begin(), dia_total_size.end());

        // warn if DIAs have unequal size
        if (!Pad && UnequalCheck && result_size_ != max_dia_total_size) {
            die("Zip(): input DIAs have unequal size: "
                << common::VecToStr(dia_total_size));
        }

        if (result_size_ == 0) return;

        // perform scatters to exchange data, with different types.
        common::VariadicCallEnumerate<kNumInputs>(
            [=](auto index) {
                (void)index;
                this->DoScatter<decltype(index)::index>();
            });
    }

    //! Access CatReaders for different different parents.
    template <typename Reader>
    class ReaderNext
    {
    public:
        ReaderNext(ZipNode& zip_node,
                   std::array<Reader, kNumInputs>& readers)
            : zip_node_(zip_node), readers_(readers) { }

        //! helper for PushData() which checks all inputs
        bool HasNext() {
            if (Pad) {
                for (size_t i = 0; i < kNumInputs; ++i) {
                    if (readers_[i].HasNext()) return true;
                }
                return false;
            }
            else {
                for (size_t i = 0; i < kNumInputs; ++i) {
                    if (!readers_[i].HasNext()) return false;
                }
                return true;
            }
        }

        template <typename Index>
        auto operator () (const Index&) {

            // get the ZipFunction's argument for this index
            using ZipArg = ZipArgN<Index::index>;

            if (Pad && !readers_[Index::index].HasNext()) {
                // take padding_ if next is not available.
                return std::get<Index::index>(zip_node_.padding_);
            }
            return readers_[Index::index].template Next<ZipArg>();
        }

    private:
        ZipNode& zip_node_;

        //! reference to the reader array in PushData().
        std::array<Reader, kNumInputs>& readers_;
    };
};

/*!
 * Zips two DIAs of equal size in style of functional programming by applying
 * zip_function to the i-th elements of both input DIAs to form the i-th element
 * of the output DIA. The type of the output DIA can be inferred from the
 * zip_function.
 *
 * The two input DIAs are required to be of equal size, otherwise use the
 * CutTag variant.
 *
 * \tparam ZipFunction Type of the zip_function. This is a function with two
 * input elements, both of the local type, and one output element, which is
 * the type of the Zip node.
 *
 * \param zip_function Zip function, which zips two elements together
 *
 * \param first_dia the initial DIA.
 *
 * \param dias DIAs, which is zipped together with the original DIA.
 *
 * \ingroup dia_dops
 */
template <typename ZipFunction, typename FirstDIAType, typename FirstDIAStack,
          typename ... DIAs>
auto Zip(const ZipFunction &zip_function,
         const DIA<FirstDIAType, FirstDIAStack>&first_dia,
         const DIAs &... dias) {

    using VarForeachExpander = int[];

    first_dia.AssertValid();
    (void)VarForeachExpander {
        (dias.AssertValid(), 0) ...
    };

    static_assert(
        std::is_convertible<
            FirstDIAType,
            typename common::FunctionTraits<ZipFunction>::template arg<0>
            >::value,
        "ZipFunction has the wrong input type in DIA 0");

    using ZipResult
              = typename common::FunctionTraits<ZipFunction>::result_type;

    using ZipArgsTuple =
              typename common::FunctionTraits<ZipFunction>::args_tuple_plain;

    using ZipNode = api::ZipNode<
              ZipResult, ZipFunction,
              /* Pad */ false, /* UnequalCheck */ true, /* NoRebalance */ false,
              1 + sizeof ... (DIAs)>;

    auto node = common::MakeCounting<ZipNode>(
        zip_function, ZipArgsTuple(), first_dia, dias ...);

    return DIA<ZipResult>(node);
}

/*!
 * Zips any number of DIAs of equal size in style of functional programming by
 * applying zip_function to the i-th elements of both input DIAs to form the
 * i-th element of the output DIA. The type of the output DIA can be inferred
 * from the zip_function.
 *
 * If the two input DIAs are of unequal size, the result is the shorter of
 * both. Otherwise use the PadTag variant.
 *
 * \tparam ZipFunction Type of the zip_function. This is a function with two
 * input elements, both of the local type, and one output element, which is the
 * type of the Zip node.
 *
 * \param zip_function Zip function, which zips two elements together
 *
 * \param first_dia the initial DIA.
 *
 * \param dias DIAs, which is zipped together with the original DIA.
 *
 * \ingroup dia_dops
 */
template <typename ZipFunction, typename FirstDIAType, typename FirstDIAStack,
          typename ... DIAs>
auto Zip(struct CutTag,
         const ZipFunction &zip_function,
         const DIA<FirstDIAType, FirstDIAStack>&first_dia,
         const DIAs &... dias) {

    using VarForeachExpander = int[];

    first_dia.AssertValid();
    (void)VarForeachExpander {
        (dias.AssertValid(), 0) ...
    };

    static_assert(
        std::is_convertible<
            FirstDIAType,
            typename common::FunctionTraits<ZipFunction>::template arg<0>
            >::value,
        "ZipFunction has the wrong input type in DIA 0");

    using ZipResult
              = typename common::FunctionTraits<ZipFunction>::result_type;

    using ZipArgsTuple =
              typename common::FunctionTraits<ZipFunction>::args_tuple_plain;

    using ZipNode = api::ZipNode<
              ZipResult, ZipFunction,
              /* Pad */ false, /* UnequalCheck */ false, /* NoRebalance */ false,
              1 + sizeof ... (DIAs)>;

    auto node = common::MakeCounting<ZipNode>(
        zip_function, ZipArgsTuple(), first_dia, dias ...);

    return DIA<ZipResult>(node);
}

/*!
 * Zips any number of DIAs in style of functional programming by applying
 * zip_function to the i-th elements of both input DIAs to form the i-th element
 * of the output DIA. The type of the output DIA can be inferred from the
 * zip_function.
 *
 * The output DIA's length is the *maximum* of all input DIAs, shorter DIAs are
 * padded with default-constructed items.
 *
 * \tparam ZipFunction Type of the zip_function. This is a function with two
 * input elements, both of the local type, and one output element, which is
 * the type of the Zip node.
 *
 * \param zip_function Zip function, which zips two elements together
 *
 * \param first_dia the initial DIA.
 *
 * \param dias DIAs, which is zipped together with the first DIA.
 *
 * \ingroup dia_dops
 */
template <typename ZipFunction, typename FirstDIAType, typename FirstDIAStack,
          typename ... DIAs>
auto Zip(struct PadTag,
         const ZipFunction &zip_function,
         const DIA<FirstDIAType, FirstDIAStack>&first_dia,
         const DIAs &... dias) {

    using VarForeachExpander = int[];

    first_dia.AssertValid();
    (void)VarForeachExpander {
        (dias.AssertValid(), 0) ...
    };

    static_assert(
        std::is_convertible<
            FirstDIAType,
            typename common::FunctionTraits<ZipFunction>::template arg<0>
            >::value,
        "ZipFunction has the wrong input type in DIA 0");

    using ZipResult =
              typename common::FunctionTraits<ZipFunction>::result_type;

    using ZipArgsTuple =
              typename common::FunctionTraits<ZipFunction>::args_tuple_plain;

    using ZipNode = api::ZipNode<
              ZipResult, ZipFunction,
              /* Pad */ true, /* UnequalCheck */ false, /* NoRebalance */ false,
              1 + sizeof ... (DIAs)>;

    auto node = common::MakeCounting<ZipNode>(
        zip_function, ZipArgsTuple(), first_dia, dias ...);

    return DIA<ZipResult>(node);
}

/*!
 * Zips any number of DIAs in style of functional programming by applying
 * zip_function to the i-th elements of both input DIAs to form the i-th element
 * of the output DIA. The type of the output DIA can be inferred from the
 * zip_function.
 *
 * The output DIA's length is the *maximum* of all input DIAs, shorter DIAs are
 * padded with items given by the padding parameter.
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
 * \param first_dia the initial DIA.
 *
 * \param dias DIAs, which is zipped together with the original DIA.
 *
 * \ingroup dia_dops
 */
template <typename ZipFunction, typename FirstDIAType, typename FirstDIAStack,
          typename ... DIAs>
auto Zip(
    struct PadTag,
    const ZipFunction &zip_function,
    const typename common::FunctionTraits<ZipFunction>::args_tuple_plain & padding,
    const DIA<FirstDIAType, FirstDIAStack>&first_dia,
    const DIAs &... dias) {

    using VarForeachExpander = int[];

    first_dia.AssertValid();
    (void)VarForeachExpander {
        (dias.AssertValid(), 0) ...
    };

    static_assert(
        std::is_convertible<
            FirstDIAType,
            typename common::FunctionTraits<ZipFunction>::template arg<0>
            >::value,
        "ZipFunction has the wrong input type in DIA 0");

    using ZipResult =
              typename common::FunctionTraits<ZipFunction>::result_type;

    using ZipNode = api::ZipNode<
              ZipResult, ZipFunction,
              /* Pad */ true, /* UnequalCheck */ false, /* NoRebalance */ false,
              1 + sizeof ... (DIAs)>;

    auto node = common::MakeCounting<ZipNode>(
        zip_function, padding, first_dia, dias ...);

    return DIA<ZipResult>(node);
}

/*!
 * Zips any number of DIAs in style of functional programming by applying
 * zip_function to the i-th elements of both input DIAs to form the i-th element
 * of the output DIA. The type of the output DIA can be inferred from the
 * zip_function.
 *
 * In this variant, the DIA partitions on all PEs must have matching length. No
 * rebalancing is performed, and the program will die if any partition
 * mismatches. This enables Zip to proceed without any communication.
 *
 * \tparam ZipFunction Type of the zip_function. This is a function with two
 * input elements, both of the local type, and one output element, which is
 * the type of the Zip node.
 *
 * \param zip_function Zip function, which zips two elements together
 *
 * \param first_dia the initial DIA.
 *
 * \param dias DIAs, which is zipped together with the original DIA.
 *
 * \ingroup dia_dops
 */
template <typename ZipFunction, typename FirstDIAType, typename FirstDIAStack,
          typename ... DIAs>
auto Zip(
    struct NoRebalanceTag,
    const ZipFunction &zip_function,
    const DIA<FirstDIAType, FirstDIAStack>&first_dia,
    const DIAs &... dias) {

    using VarForeachExpander = int[];

    first_dia.AssertValid();
    (void)VarForeachExpander {
        (dias.AssertValid(), 0) ...
    };

    static_assert(
        std::is_convertible<
            FirstDIAType,
            typename common::FunctionTraits<ZipFunction>::template arg<0>
            >::value,
        "ZipFunction has the wrong input type in DIA 0");

    using ZipResult =
              typename common::FunctionTraits<ZipFunction>::result_type;

    using ZipArgsTuple =
              typename common::FunctionTraits<ZipFunction>::args_tuple_plain;

    using ZipNode = api::ZipNode<
              ZipResult, ZipFunction,
              /* Pad */ false, /* UnequalCheck */ false, /* NoRebalance */ true,
              1 + sizeof ... (DIAs)>;

    auto node = common::MakeCounting<ZipNode>(
        zip_function, ZipArgsTuple(), first_dia, dias ...);

    return DIA<ZipResult>(node);
}

template <typename ValueType, typename Stack>
template <typename ZipFunction, typename SecondDIA>
auto DIA<ValueType, Stack>::Zip(
    const SecondDIA &second_dia, const ZipFunction &zip_function) const {
    return api::Zip(zip_function, *this, second_dia);
}

template <typename ValueType, typename Stack>
template <typename ZipFunction, typename SecondDIA>
auto DIA<ValueType, Stack>::Zip(
    struct CutTag const &, const SecondDIA &second_dia,
    const ZipFunction &zip_function) const {
    return api::Zip(CutTag, zip_function, *this, second_dia);
}

template <typename ValueType, typename Stack>
template <typename ZipFunction, typename SecondDIA>
auto DIA<ValueType, Stack>::Zip(
    struct PadTag const &, const SecondDIA &second_dia,
    const ZipFunction &zip_function) const {
    return api::Zip(PadTag, zip_function, *this, second_dia);
}

template <typename ValueType, typename Stack>
template <typename ZipFunction, typename SecondDIA>
auto DIA<ValueType, Stack>::Zip(
    struct NoRebalanceTag const &, const SecondDIA &second_dia,
    const ZipFunction &zip_function) const {
    return api::Zip(NoRebalanceTag, zip_function, *this, second_dia);
}

//! \}

} // namespace api

//! imported from api namespace
using api::Zip;

} // namespace thrill

#endif // !THRILL_API_ZIP_HEADER

/******************************************************************************/
