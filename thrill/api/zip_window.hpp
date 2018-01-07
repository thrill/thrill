/*******************************************************************************
 * thrill/api/zip_window.hpp
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
#ifndef THRILL_API_ZIP_WINDOW_HEADER
#define THRILL_API_ZIP_WINDOW_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/string.hpp>
#include <thrill/data/file.hpp>
#include <tlx/meta/apply_tuple.hpp>
#include <tlx/meta/call_for_range.hpp>
#include <tlx/meta/call_foreach_with_index.hpp>
#include <tlx/meta/vexpand.hpp>
#include <tlx/meta/vmap_for_range.hpp>

#include <algorithm>
#include <array>
#include <functional>
#include <tuple>
#include <vector>

namespace thrill {
namespace api {

/******************************************************************************/

//! tag structure for ZipWindow()
struct ArrayTag {
    ArrayTag() { }
};

//! global const ArrayTag instance
const struct ArrayTag ArrayTag;

/******************************************************************************/
// ZipWindowTraits - Helper to create std::tuple<std::vector<Args> ...>

#ifndef THRILL_DOXYGEN_IGNORE

// taken from: http://stackoverflow.com/questions/7943525/
// is-it-possible-to-figure-out-the-parameter-type-and-return-type-of-a-lambda
template <typename T>
struct ZipWindowTraits : public ZipWindowTraits<decltype(& T::operator ())>{ };
// For generic types, directly use the result of the signature of its 'operator()'

#endif

//! specialize for pointers to const member function
template <typename ClassType, typename ReturnType, typename... Args>
struct ZipWindowTraits<ReturnType (ClassType::*)(Args...) const>{

    //! arity is the number of arguments.
    static constexpr size_t arity = sizeof ... (Args);

    using result_type = ReturnType;
    using is_const = std::true_type;

    //! the tuple of value_type inside the vectors
    using value_type_tuple = std::tuple<
              typename std::remove_cv<
                  typename std::remove_reference<Args>::type
                  >::type::value_type...>;

    //! the tuple of value_types: with remove_cv and remove_reference applied.
    using value_type_tuple_plain = std::tuple<
              typename std::remove_cv<
                  typename std::remove_reference<
                      typename std::remove_cv<
                          typename std::remove_reference<Args>::type
                          >::type::value_type>::type>::type...>;

    //! the i-th argument is equivalent to the i-th tuple element of a tuple
    //! composed of those arguments.
    template <size_t i>
    using value_type = typename std::tuple_element<i, value_type_tuple>::type;

    //! return i-th argument reduced to plain type: remove_cv and
    //! remove_reference.
    template <size_t i>
    using value_type_plain =
              typename std::remove_cv<
                  typename std::remove_reference<value_type<i> >::type>::type;

    //! the tuple of std::vector<>s: with remove_cv and remove_reference applied
    using vector_tuple_plain = std::tuple<
              typename std::remove_cv<
                  typename std::remove_reference<Args>::type>::type...>;

    //! the i-th argument is equivalent to the i-th tuple element of a tuple
    //! composed of those arguments.
    template <size_t i>
    using vector_plain = typename std::tuple_element<
              i, vector_tuple_plain>::type;
};

//! specialize for pointers to mutable member function
template <typename ClassType, typename ReturnType, typename... Args>
struct ZipWindowTraits<ReturnType (ClassType::*)(Args...)>
    : public ZipWindowTraits<ReturnType (ClassType::*)(Args...) const>{
    using is_const = std::false_type;
};

//! specialize for function pointers
template <typename ReturnType, typename... Args>
struct ZipWindowTraits<ReturnType (*)(Args...)>{

    //! arity is the number of arguments.
    static constexpr size_t arity = sizeof ... (Args);

    using result_type = ReturnType;
    using is_const = std::true_type;

    //! the tuple of value_type inside the vectors
    using value_type_tuple = std::tuple<
              typename std::remove_cv<
                  typename std::remove_reference<Args>::type
                  >::type::value_type...>;

    //! the tuple of value_types: with remove_cv and remove_reference applied.
    using value_type_tuple_plain = std::tuple<
              typename std::remove_cv<
                  typename std::remove_reference<
                      typename std::remove_cv<
                          typename std::remove_reference<Args>::type
                          >::type::value_type>::type>::type...>;

    //! the i-th argument is equivalent to the i-th tuple element of a tuple
    //! composed of those arguments.
    template <size_t i>
    using value_type = typename std::tuple_element<i, value_type_tuple>::type;

    //! return i-th argument reduced to plain type: remove_cv and
    //! remove_reference.
    template <size_t i>
    using value_type_plain =
              typename std::remove_cv<
                  typename std::remove_reference<value_type<i> >::type>::type;

    //! the tuple of std::vector<>s: with remove_cv and remove_reference applied
    using vector_tuple_plain = std::tuple<
              typename std::remove_cv<
                  typename std::remove_reference<Args>::type>::type...>;

    //! the i-th argument is equivalent to the i-th tuple element of a tuple
    //! composed of those arguments.
    template <size_t i>
    using vector_plain = typename std::tuple_element<
              i, vector_tuple_plain>::type;
};

/******************************************************************************/

template <typename ZipWindowNode, bool UseArray>
class ZipWindowReader;

/******************************************************************************/

template <typename ValueType, typename ZipFunction_,
          bool Pad_, bool UnequalCheck, bool UseStdArray, size_t kNumInputs_>
class ZipWindowNode final : public DOpNode<ValueType>
{
    static constexpr bool debug = false;

    //! Set this variable to true to enable generation and output of stats
    static constexpr bool stats_enabled = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

public:
    using ZipFunction = ZipFunction_;
    static constexpr bool Pad = Pad_;
    static constexpr size_t kNumInputs = kNumInputs_;

    template <size_t Index>
    using ZipArgN =
              typename ZipWindowTraits<ZipFunction>::template value_type_plain<Index>;
    using ZipArgsTuple =
              typename ZipWindowTraits<ZipFunction>::value_type_tuple_plain;

public:
    /*!
     * Constructor for a ZipWindowNode.
     */
    template <typename ParentDIA0, typename... ParentDIAs>
    ZipWindowNode(const std::array<size_t, kNumInputs>& window_size,
                  const ZipFunction& zip_function, const ZipArgsTuple& padding,
                  const ParentDIA0& parent0, const ParentDIAs& ... parents)
        : Super(parent0.ctx(), "ZipWindow",
                { parent0.id(), parents.id() ... },
                { parent0.node(), parents.node() ... }),
          window_size_(window_size),
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
        tlx::call_foreach_with_index(
            RegisterParent(this), parent0, parents...);
    }

    void StartPreOp(size_t parent_index) final {
        writers_[parent_index] = files_[parent_index].GetWriter();
    }

    //! Receive a whole data::File of ValueType, but only if our stack is empty.
    bool OnPreOpFile(const data::File& file, size_t parent_index) final {
        assert(parent_index < kNumInputs);
        if (!parent_stack_empty_[parent_index]) {
            LOGC(context_.my_rank() == 0)
                << "ZipWindow rejected File from parent "
                << "due to non-empty function stack.";
            return false;
        }

        // accept file
        LOGC(context_.my_rank() == 0)
            << "ZipWindow accepted File from parent " << parent_index;
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

        if (result_window_count_ != 0) {
            // get inbound readers from all Streams
            std::array<data::CatStream::CatReader, kNumInputs> readers;
            for (size_t i = 0; i < kNumInputs; ++i)
                readers[i] = streams_[i]->GetCatReader(consume);

            ZipWindowReader<ZipWindowNode, UseStdArray> reader_next(
                *this, readers);

            while (reader_next.HasNext()) {
                auto v = tlx::vmap_for_range<kNumInputs>(reader_next);
                this->PushItem(tlx::apply_tuple(zip_function_, v));
                ++result_count;
            }
        }

        if (stats_enabled) {
            context_.PrintCollectiveMeanStdev(
                "ZipWindow() result_count", result_count);
        }
    }

    void Dispose() final {
        files_.clear();
        for (size_t i = 0; i < kNumInputs; ++i)
            streams_[i].reset();
    }

private:
    //! Size k of the windows
    const std::array<size_t, kNumInputs> window_size_;

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
    std::array<size_t, kNumInputs> size_prefixsum_;

    //! shortest size of Zipped inputs
    size_t result_window_count_;

    //! \}

    //! Register Parent PreOp Hooks, instantiated and called for each Zip parent
    class RegisterParent
    {
    public:
        explicit RegisterParent(ZipWindowNode* node) : node_(node) { }

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
        ZipWindowNode* node_;
    };

    //! Scatter items from DIA "Index" to other workers if necessary.
    template <size_t Index>
    void DoScatter() {
        const size_t workers = context_.num_workers();

        size_t result_size = result_window_count_ * window_size_[Index];

        // range of items on local node
        size_t local_begin = std::min(result_size, size_prefixsum_[Index]);
        size_t local_end = std::min(
            result_size, size_prefixsum_[Index] + files_[Index].num_items());

        // number of elements per worker (double)
        double per_pe =
            static_cast<double>(result_window_count_) / static_cast<double>(workers);
        // offsets for scattering
        std::vector<size_t> offsets(workers + 1, 0);

        for (size_t i = 0; i <= workers; ++i) {
            // calculate range we have to send to each PE
            size_t cut =
                static_cast<size_t>(std::ceil(i * per_pe)) * window_size_[Index];
            offsets[i] =
                cut < local_begin ? 0 : std::min(cut, local_end) - local_begin;
        }

        LOG << "per_pe=" << per_pe
            << " offsets[" << Index << "] = " << offsets;

        // target stream id
        streams_[Index] = context_.GetNewCatStream(this);

        // scatter elements to other workers, if necessary
        using ZipArg = ZipArgN<Index>;
        streams_[Index]->template Scatter<ZipArg>(
            files_[Index], offsets, /* consume */ true);
    }

    //! Receive elements from other workers.
    void MainOp() {
        // first: calculate total size of the DIAs to Zip

        using ArraySizeT = std::array<size_t, kNumInputs>;

        // number of elements of this worker
        ArraySizeT local_size;
        for (size_t i = 0; i < kNumInputs; ++i) {
            local_size[i] = files_[i].num_items();
            sLOG << "input" << i << "local_size" << local_size[i];

            if (stats_enabled) {
                context_.PrintCollectiveMeanStdev(
                    "ZipWindow() local_size", local_size[i]);
            }
        }

        // exclusive prefixsum of number of elements: we have items from
        // [size_prefixsum, size_prefixsum + local_size). And get the total
        // number of items in each DIAs, over all worker.
        size_prefixsum_ = local_size;
        ArraySizeT total_size = context_.net.ExPrefixSumTotal(
            size_prefixsum_,
            ArraySizeT(), common::ComponentSum<ArraySizeT>());

        // calculate number of full windows in each DIA
        ArraySizeT total_window_count;
        for (size_t i = 0; i < kNumInputs; ++i) {
            total_window_count[i] =
                (total_size[i] + window_size_[i] - 1) / window_size_[i];
        }

        size_t max_total_window_count = *std::max_element(
            total_window_count.begin(), total_window_count.end());

        // return only the minimum window count of all DIAs.
        result_window_count_ =
            Pad ? max_total_window_count : *std::min_element(
                total_window_count.begin(), total_window_count.end());

        sLOG << "ZipWindow() total_size" << total_size
             << "total_window_count" << total_window_count
             << "max_total_window_count" << max_total_window_count
             << "result_window_count_" << result_window_count_;

        // warn if DIAs have unequal window size
        if (!Pad && UnequalCheck && result_window_count_ != max_total_window_count) {
            die("ZipWindow(): input DIAs have unequal size: "
                << common::VecToStr(total_size));
        }

        if (result_window_count_ == 0) return;

        // perform scatters to exchange data, with different types.
        tlx::call_for_range<kNumInputs>(
            [=](auto index) {
                tlx::unused(index);
                this->DoScatter<decltype(index)::index>();
            });
    }

    //! for access to internal members
    template <typename ZipWindowNode, bool UseArray>
    friend class ZipWindowReader;
};

//! template specialization Reader which delivers std::vector<>s to ZipFunction
template <typename ZipWindowNode>
class ZipWindowReader<ZipWindowNode, /* UseArray */ false>
{
public:
    using Reader = data::CatStream::CatReader;
    using ZipFunction = typename ZipWindowNode::ZipFunction;
    static constexpr size_t Pad = ZipWindowNode::Pad;
    static constexpr size_t kNumInputs = ZipWindowNode::kNumInputs;

    template <size_t Index>
    using ZipArgN =
              typename ZipWindowTraits<ZipFunction>
              ::template value_type_plain<Index>;

    template <size_t Index>
    using ZipVectorN =
              typename ZipWindowTraits<ZipFunction>
              ::template vector_plain<Index>;

    ZipWindowReader(ZipWindowNode& zip_node,
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
    const ZipVectorN<Index::index>& operator () (const Index&) {

        // get the ZipFunction's argument for this index
        using ZipArg = ZipArgN<Index::index>;

        std::vector<ZipArg>& vec = std::get<Index::index>(vectors_);
        vec.clear();

        for (size_t i = 0; i < zip_node_.window_size_[Index::index]; ++i) {
            if (Pad && !readers_[Index::index].HasNext()) {
                // take padding_ if next is not available.
                vec.emplace_back(
                    std::get<Index::index>(zip_node_.padding_));
            }
            else {
                vec.emplace_back(
                    readers_[Index::index].template Next<ZipArg>());
            }
        }

        return vec;
    }

private:
    ZipWindowNode& zip_node_;

    //! reference to the reader array in PushData().
    std::array<Reader, kNumInputs>& readers_;

    //! tuple of std::vector<>s
    typename ZipWindowTraits<ZipFunction>::vector_tuple_plain vectors_;
};

//! template specialization Reader which delivers std::array<>s to ZipFunction
template <typename ZipWindowNode>
class ZipWindowReader<ZipWindowNode, /* UseArray */ true>
{
public:
    using Reader = data::CatStream::CatReader;
    using ZipFunction = typename ZipWindowNode::ZipFunction;
    static constexpr size_t Pad = ZipWindowNode::Pad;
    static constexpr size_t kNumInputs = ZipWindowNode::kNumInputs;

    template <size_t Index>
    using ZipArgN =
              typename ZipWindowTraits<ZipFunction>
              ::template value_type_plain<Index>;

    template <size_t Index>
    using ZipVectorN =
              typename ZipWindowTraits<ZipFunction>
              ::template vector_plain<Index>;

    ZipWindowReader(ZipWindowNode& zip_node,
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
    const ZipVectorN<Index::index>& operator () (const Index&) {

        // get the ZipFunction's argument for this index
        using ZipArg = ZipArgN<Index::index>;
        using ZipVector = ZipVectorN<Index::index>;

        ZipVector& vec = std::get<Index::index>(vectors_);

        for (size_t i = 0; i < zip_node_.window_size_[Index::index]; ++i) {
            if (Pad && !readers_[Index::index].HasNext()) {
                // take padding_ if next is not available.
                vec[i] = std::get<Index::index>(zip_node_.padding_);
            }
            else {
                vec[i] = readers_[Index::index].template Next<ZipArg>();
            }
        }

        return vec;
    }

private:
    ZipWindowNode& zip_node_;

    //! reference to the reader array in PushData().
    std::array<Reader, kNumInputs>& readers_;

    //! tuple of std::vector<>s
    typename ZipWindowTraits<ZipFunction>::vector_tuple_plain vectors_;
};

/******************************************************************************/

/*!
 * Zips two DIAs of equal size in style of functional programming by applying
 * zip_function to the i-th fixed-sized windows of both input DIAs to form the
 * i-th element of the output DIA. The input DIAs length must be multiples of
 * the corresponding window size. The type of the output DIA can be inferred
 * from the zip_function.
 *
 * The two input DIAs are required to be of equal window multiples, otherwise
 * use the CutTag variant.
 *
 * \ingroup dia_dops
 */
template <typename ZipFunction, typename FirstDIAType, typename FirstDIAStack,
          typename... DIAs>
auto ZipWindow(const std::array<size_t, 1 + sizeof ... (DIAs)>& window_size,
               const ZipFunction& zip_function,
               const DIA<FirstDIAType, FirstDIAStack>& first_dia,
               const DIAs& ... dias) {

    tlx::vexpand((first_dia.AssertValid(), 0), (dias.AssertValid(), 0) ...);

    static_assert(
        std::is_convertible<
            std::vector<FirstDIAType>,
            typename ZipWindowTraits<ZipFunction>::template vector_plain<0>
            >::value,
        "ZipFunction has the wrong input type in DIA 0");

    using ZipResult
              = typename ZipWindowTraits<ZipFunction>::result_type;

    using ZipArgsTuple =
              typename ZipWindowTraits<ZipFunction>::value_type_tuple_plain;

    using ZipWindowNode = api::ZipWindowNode<
              ZipResult, ZipFunction,
              /* Pad */ false, /* UnequalCheck */ true,
              /* UseStdArray */ false,
              1 + sizeof ... (DIAs)>;

    auto node = tlx::make_counting<ZipWindowNode>(
        window_size, zip_function, ZipArgsTuple(), first_dia, dias...);

    return DIA<ZipResult>(node);
}

/*!
 * Zips two DIAs of equal size in style of functional programming by applying
 * zip_function to the i-th fixed-sized windows of both input DIAs to form the
 * i-th element of the output DIA. The input DIAs length must be multiples of
 * the corresponding window size. The type of the output DIA can be inferred
 * from the zip_function.
 *
 * If the two input DIAs are of unequal size, the result is the shorter of
 * both. Otherwise use the PadTag variant.
 *
 * \ingroup dia_dops
 */
template <typename ZipFunction, typename FirstDIAType, typename FirstDIAStack,
          typename... DIAs>
auto ZipWindow(struct CutTag,
               const std::array<size_t, 1 + sizeof ... (DIAs)>& window_size,
               const ZipFunction& zip_function,
               const DIA<FirstDIAType, FirstDIAStack>& first_dia,
               const DIAs& ... dias) {

    tlx::vexpand((first_dia.AssertValid(), 0), (dias.AssertValid(), 0) ...);

    static_assert(
        std::is_convertible<
            std::vector<FirstDIAType>,
            typename ZipWindowTraits<ZipFunction>::template vector_plain<0>
            >::value,
        "ZipFunction has the wrong input type in DIA 0");

    using ZipResult
              = typename ZipWindowTraits<ZipFunction>::result_type;

    using ZipArgsTuple =
              typename ZipWindowTraits<ZipFunction>::value_type_tuple_plain;

    using ZipWindowNode = api::ZipWindowNode<
              ZipResult, ZipFunction,
              /* Pad */ false, /* UnequalCheck */ false,
              /* UseStdArray */ false,
              1 + sizeof ... (DIAs)>;

    auto node = tlx::make_counting<ZipWindowNode>(
        window_size, zip_function, ZipArgsTuple(), first_dia, dias...);

    return DIA<ZipResult>(node);
}

/*!
 * Zips two DIAs of equal size in style of functional programming by applying
 * zip_function to the i-th fixed-sized windows of both input DIAs to form the
 * i-th element of the output DIA. The input DIAs length must be multiples of
 * the corresponding window size. The type of the output DIA can be inferred
 * from the zip_function.
 *
 * The output DIA's length is the *maximum* of all input DIAs, shorter DIAs are
 * padded with items given by the padding parameter.
 *
 * \ingroup dia_dops
 */
template <typename ZipFunction, typename FirstDIAType, typename FirstDIAStack,
          typename... DIAs>
auto ZipWindow(
    struct PadTag,
    const std::array<size_t, 1 + sizeof ... (DIAs)>& window_size,
    const ZipFunction& zip_function,
    const typename ZipWindowTraits<ZipFunction>::value_type_tuple_plain& padding,
    const DIA<FirstDIAType, FirstDIAStack>& first_dia,
    const DIAs& ... dias) {

    tlx::vexpand((first_dia.AssertValid(), 0), (dias.AssertValid(), 0) ...);

    static_assert(
        std::is_convertible<
            std::vector<FirstDIAType>,
            typename ZipWindowTraits<ZipFunction>::template vector_plain<0>
            >::value,
        "ZipFunction has the wrong input type in DIA 0");

    using ZipResult =
              typename common::FunctionTraits<ZipFunction>::result_type;

    using ZipWindowNode = api::ZipWindowNode<
              ZipResult, ZipFunction,
              /* Pad */ true, /* UnequalCheck */ false,
              /* UseStdArray */ false,
              1 + sizeof ... (DIAs)>;

    auto node = tlx::make_counting<ZipWindowNode>(
        window_size, zip_function, padding, first_dia, dias...);

    return DIA<ZipResult>(node);
}

/*!
 * Zips two DIAs of equal size in style of functional programming by applying
 * zip_function to the i-th fixed-sized windows of both input DIAs to form the
 * i-th element of the output DIA. The input DIAs length must be multiples of
 * the corresponding window size. The type of the output DIA can be inferred
 * from the zip_function.
 *
 * The output DIA's length is the *maximum* of all input DIAs, shorter DIAs are
 * padded with default-constructed items.
 *
 * \ingroup dia_dops
 */
template <typename ZipFunction, typename FirstDIAType, typename FirstDIAStack,
          typename... DIAs>
auto ZipWindow(struct PadTag,
               const std::array<size_t, 1 + sizeof ... (DIAs)>& window_size,
               const ZipFunction& zip_function,
               const DIA<FirstDIAType, FirstDIAStack>& first_dia,
               const DIAs& ... dias) {

    using ZipArgsTuple =
              typename ZipWindowTraits<ZipFunction>::value_type_tuple_plain;

    return ZipWindow(PadTag, window_size, zip_function,
                     ZipArgsTuple(), first_dia, dias...);
}

/******************************************************************************/

/*!
 * Zips two DIAs of equal size in style of functional programming by applying
 * zip_function to the i-th fixed-sized windows of both input DIAs to form the
 * i-th element of the output DIA. The input DIAs length must be multiples of
 * the corresponding window size. The type of the output DIA can be inferred
 * from the zip_function.
 *
 * The output DIA's length is the *maximum* of all input DIAs, shorter DIAs are
 * padded with items given by the padding parameter.
 *
 * \ingroup dia_dops
 */
template <typename ZipFunction, typename FirstDIAType, typename FirstDIAStack,
          typename... DIAs>
auto ZipWindow(
    struct ArrayTag,
    struct PadTag,
    const std::array<size_t, 1 + sizeof ... (DIAs)>& window_size,
    const ZipFunction& zip_function,
    const typename ZipWindowTraits<ZipFunction>::value_type_tuple_plain& padding,
    const DIA<FirstDIAType, FirstDIAStack>& first_dia,
    const DIAs& ... dias) {

    tlx::vexpand((first_dia.AssertValid(), 0), (dias.AssertValid(), 0) ...);

    // static_assert(
    //     std::is_convertible<
    //     std::array<FirstDIAType, >,
    //         typename ZipWindowTraits<ZipFunction>::template vector_plain<0>
    //         >::value,
    //     "ZipFunction has the wrong input type in DIA 0");

    using ZipResult =
              typename common::FunctionTraits<ZipFunction>::result_type;

    using ZipWindowNode = api::ZipWindowNode<
              ZipResult, ZipFunction,
              /* Pad */ true, /* UnequalCheck */ false,
              /* UseStdArray */ true,
              1 + sizeof ... (DIAs)>;

    auto node = tlx::make_counting<ZipWindowNode>(
        window_size, zip_function, padding, first_dia, dias...);

    return DIA<ZipResult>(node);
}

/*!
 * Zips two DIAs of equal size in style of functional programming by applying
 * zip_function to the i-th fixed-sized windows of both input DIAs to form the
 * i-th element of the output DIA. The input DIAs length must be multiples of
 * the corresponding window size. The type of the output DIA can be inferred
 * from the zip_function.
 *
 * The output DIA's length is the *maximum* of all input DIAs, shorter DIAs are
 * padded with default constructed items.
 *
 * \ingroup dia_dops
 */
template <typename ZipFunction, typename FirstDIAType, typename FirstDIAStack,
          typename... DIAs>
auto ZipWindow(
    struct ArrayTag,
    struct PadTag,
    const std::array<size_t, 1 + sizeof ... (DIAs)>& window_size,
    const ZipFunction& zip_function,
    const DIA<FirstDIAType, FirstDIAStack>& first_dia,
    const DIAs& ... dias) {

    using ZipArgsTuple =
              typename ZipWindowTraits<ZipFunction>::value_type_tuple_plain;

    return ZipWindow(ArrayTag, PadTag, window_size, zip_function,
                     ZipArgsTuple(), first_dia, dias...);
}

/******************************************************************************/

//! \}

} // namespace api

//! imported from api namespace
using api::ZipWindow;

//! imported from api namespace
using api::ArrayTag;

} // namespace thrill

#endif // !THRILL_API_ZIP_WINDOW_HEADER

/******************************************************************************/
