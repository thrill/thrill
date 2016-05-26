/*******************************************************************************
 * thrill/api/dia.hpp
 *
 * Interface for Operations, holds pointer to node and lambda from node to state
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Huyen Chau Nguyen <hello@chau-nguyen.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_DIA_HEADER
#define THRILL_API_DIA_HEADER

#include <thrill/api/context.hpp>
#include <thrill/api/dia_node.hpp>
#include <thrill/api/function_stack.hpp>
#include <thrill/common/function_traits.hpp>
#include <thrill/common/functional.hpp>

#include <cassert>
#include <functional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace api {

//! \ingroup api_layer
//! \{

//! tag structure for ReduceByKey(), and ReduceToIndex()
struct VolatileKeyTag {
    VolatileKeyTag() { }
};

//! global const VolatileKeyTag instance
const struct VolatileKeyTag VolatileKeyTag;

//! tag structure for Window() and FlatWindow()
struct DisjointTag {
    DisjointTag() { }
};

//! global const DisjointTag instance
const struct DisjointTag DisjointTag;

/*!
 * DIA is the interface between the user and the Thrill framework. A DIA can be
 * imagined as an immutable array, even though the data does not need to be
 * which represents the state after the previous DOp or Action. Additionally, a
 * DIA stores the local lambda function chain of type Stack, which can transform
 * elements of the DIANode to elements of this DIA. DOps/Actions create a DIA
 * and a new DIANode, to which the DIA links to. LOps only create a new DIA,
 * which link to the previous DIANode.
 *
 * \tparam ValueType Type of elements currently in this DIA.
 * \tparam Stack Type of the function chain.
 */
template <typename ValueType_, typename Stack_ = FunctionStack<ValueType_> >
class DIA
{
    friend class Context;

    //! alias for convenience.
    template <typename Function>
    using FunctionTraits = common::FunctionTraits<Function>;

public:
    //! type of the items virtually in the DIA, which is the type emitted by the
    //! current LOp stack.
    using ValueType = ValueType_;

    //! Type of this function stack
    using Stack = Stack_;

    //! type of the items delivered by the DOp, and pushed down the function
    //! stack towards the next nodes. If the function stack contains LOps nodes,
    //! these may transform the type.
    using StackInput = typename Stack::Input;

    //! boolean indication whether this FunctionStack is empty
    static constexpr bool stack_empty = Stack::empty;

    //! type of pointer to the real node object implementation. This object has
    //! base item type StackInput which is transformed by the function stack
    //! lambdas further. But even pushing more lambdas does not change the stack
    //! input type.
    using DIANodePtr = common::CountingPtr<DIANode<StackInput> >;

    //! default-constructor: invalid DIA
    DIA() = default;

    //! Return whether the DIA is valid.
    bool IsValid() const { return node_.get() != nullptr; }

    //! Assert that the DIA is valid.
    void AssertValid() const { assert(IsValid()); }

    /*!
     * Constructor of a new DIA with a pointer to a DIANode and a
     * function chain from the DIANode to this DIA.
     *
     * \param node Pointer to the last DIANode, DOps and Actions create a new
     * DIANode, LOps link to the DIANode of the previous DIA.
     *
     * \param stack Function stack consisting of functions between last DIANode
     * and this DIA.
     *
     * \param id Serial id of DIA, which includes LOps
     *
     * \param label static string label of DIA.
     */
    DIA(const DIANodePtr& node, const Stack& stack, size_t id, const char* label)
        : node_(node), stack_(stack), id_(id), label_(label) { }

    /*!
     * Constructor of a new DIA supporting move semantics of nodes.
     *
     * \param node Pointer to the last DIANode, DOps and Actions create a new
     * DIANode, LOps link to the DIANode of the previous DIA.
     *
     * \param stack Function stack consisting of functions between last DIANode
     * and this DIA.
     *
     * \param id Serial id of DIA, which includes LOps
     *
     * \param label static string label of DIA.
     */
    DIA(DIANodePtr&& node, const Stack& stack, size_t id, const char* label)
        : node_(std::move(node)), stack_(stack), id_(id), label_(label) { }

    /*!
     * Constructor of a new DIA with a real backing DIABase.
     *
     * \param node Pointer to the last DIANode, DOps and Actions create a new
     * DIANode, LOps link to the DIANode of the previous DIA.
      */
    explicit DIA(DIANodePtr&& node)
        : DIA(std::move(node), FunctionStack<ValueType>(),
              node->id(), node->label()) { }

    /*!
     * Copy-Constructor of a DIA with empty function chain from a DIA with
     * a non-empty chain.  The functionality of the chain is stored in a newly
     * created LOpNode.  The current DIA than points to this LOpNode.  This
     * is needed to support assignment operations between DIA's.
     */
#ifdef THRILL_DOXYGEN_IGNORE
    template <typename AnyStack>
    DIA(const DIA<ValueType, AnyStack>& rhs);
#else
    template <typename AnyStack>
    DIA(const DIA<ValueType, AnyStack>& rhs)
#if __GNUC__ && !__clang__
    // the attribute warning does not work with gcc?
    __attribute__ ((warning(      // NOLINT
                        "Casting to DIA creates LOpNode instead of inline chaining.\n"
                        "Consider whether you can use auto instead of DIA.")));
#elif __GNUC__ && __clang__
    __attribute__ ((deprecated)); // NOLINT
#else
    ;                             // NOLINT
#endif
#endif  // THRILL_DOXYGEN_IGNORE

    //! \name Const Accessors
    //! \{

    //! Returns a pointer to the according DIANode.
    const DIANodePtr& node() const {
        assert(IsValid());
        return node_;
    }

    //! Returns the number of references to the according DIANode.
    size_t node_refcount() const {
        assert(IsValid());
        return node_->reference_count();
    }

    //! Returns the stored function chain.
    const Stack& stack() const {
        assert(IsValid());
        return stack_;
    }

    //! Return context_ of DIANode, e.g. for creating new LOps and DOps
    Context& context() const {
        assert(IsValid());
        return node_->context();
    }

    //! Return context_ of DIANode, e.g. for creating new LOps and DOps
    Context& ctx() const {
        assert(IsValid());
        return node_->context();
    }

    //! Returns id_
    size_t id() const { return id_; }

    //! Returns label_
    const char * label() const { return label_; }

    //! \}

    /*!
     * Mark the referenced DIANode for keeping, which makes children not consume
     * the data when executing. This does not create a new DIA, but returns the
     * existing one.
     */
    const DIA& Keep(size_t increase = 1) const {
        assert(IsValid());
        node_->IncConsumeCounter(increase);
        return *this;
    }

    /*!
     * Mark the referenced DIANode for keeping forever, which makes children not
     * consume the data when executing. This does not create a new DIA, but
     * returns the existing one.
     */
    const DIA& KeepForever() const {
        assert(IsValid());
        node_->SetConsumeCounter(DIABase::kNeverConsume);
        return *this;
    }

    /*!
     * Execute DIA's scope and parents such that this (Action)Node is
     * Executed. This does not create a new DIA, but returns the existing one.
     */
    DIA& Execute() {
        assert(IsValid());
        node_->RunScope();
        return *this;
    }

    //! \name Local Operations (LOps)
    //! \{

    /*!
     * Map applies `map_function` : \f$ A \to B \f$ to each item of a DIA and
     * delivers a new DIA contains the returned values, which may be of a
     * different type.
     *
     * The function chain of the returned DIA is this DIA's stack_ chained with
     * map_fn.
     *
     * \param map_function Map function of type MapFunction, which maps each
     * element to an element of a possibly different type.
     *
     * \ingroup dia_lops
     */
    template <typename MapFunction>
    auto Map(const MapFunction &map_function) const {
        assert(IsValid());

        using MapArgument
                  = typename FunctionTraits<MapFunction>::template arg_plain<0>;
        using MapResult
                  = typename FunctionTraits<MapFunction>::result_type;
        auto conv_map_function =
            [map_function](const MapArgument& input, auto emit_func) {
                emit_func(map_function(input));
            };

        static_assert(
            std::is_convertible<ValueType, MapArgument>::value,
            "MapFunction has the wrong input type");

        size_t new_id = context().next_dia_id();

        node_->context().logger_
            << "id" << new_id
            << "label" << "Map"
            << "class" << "DIA"
            << "event" << "create"
            << "type" << "LOp"
            << "parents" << (common::Array<size_t>{ id_ });

        auto new_stack = stack_.push(conv_map_function);
        return DIA<MapResult, decltype(new_stack)>(
            node_, new_stack, new_id, "Map");
    }

    /*!
     * Each item of a DIA is tested using `filter_function` : \f$ A \to
     * \textrm{bool} \f$ to determine whether it is copied into the output DIA
     * or excluded.
     *
     * The function chain of the returned DIA is this DIA's stack_ chained with
     * filter_function.
     *
     * \param filter_function Filter function of type FilterFunction, which maps
     * each element to a boolean.
     *
     * \ingroup dia_lops
     */
    template <typename FilterFunction>
    auto Filter(const FilterFunction &filter_function) const {
        assert(IsValid());

        using FilterArgument
                  = typename FunctionTraits<FilterFunction>::template arg_plain<0>;
        auto conv_filter_function =
            [filter_function](const FilterArgument& input, auto emit_func) {
                if (filter_function(input)) emit_func(input);
            };

        static_assert(
            std::is_convertible<ValueType, FilterArgument>::value,
            "FilterFunction has the wrong input type");

        size_t new_id = context().next_dia_id();

        node_->context().logger_
            << "id" << new_id
            << "label" << "Filter"
            << "class" << "DIA"
            << "event" << "create"
            << "type" << "LOp"
            << "parents" << (common::Array<size_t>{ id_ });

        auto new_stack = stack_.push(conv_filter_function);
        return DIA<ValueType, decltype(new_stack)>(
            node_, new_stack, new_id, "Filter");
    }

    /*!
     * \brief Each item of a DIA is expanded by the `flatmap_function` : \f$ A
     * \to \textrm{array}(B) \f$ to zero or more items of different type, which
     * are concatenated in the resulting DIA. The return type of
     * `flatmap_function` must be specified as template parameter.
     *
     * FlatMap is a LOp, which maps this DIA according to the flatmap_function
     * given by the user. The flatmap_function maps each element to elements of
     * a possibly different type. The flatmap_function has an emitter function
     * as it's second parameter. This emitter is called once for each element to
     * be emitted. The function chain of the returned DIA is this DIA's stack_
     * chained with flatmap_function.

     * \tparam ResultType ResultType of the FlatmapFunction, if different from
     * item type of DIA.
     *
     * \param flatmap_function Map function of type FlatmapFunction, which maps
     * each element to elements of a possibly different type.
     *
     * \ingroup dia_lops
     */
    template <typename ResultType = ValueType, typename FlatmapFunction>
    auto FlatMap(const FlatmapFunction &flatmap_function) const {
        assert(IsValid());

        size_t new_id = context().next_dia_id();

        node_->context().logger_
            << "id" << new_id
            << "label" << "FlatMap"
            << "class" << "DIA"
            << "event" << "create"
            << "type" << "LOp"
            << "parents" << (common::Array<size_t>{ id_ });

        auto new_stack = stack_.push(flatmap_function);
        return DIA<ResultType, decltype(new_stack)>(
            node_, new_stack, new_id, "FlatMap");
    }

    /*!
     * Each item of a DIA is copied into the output DIA with success probability
     * p (an independent Bernoulli trial).
     *
     * \ingroup dia_lops
     */
    auto BernoulliSample(double p) const;

    /*!
     * Union is a LOp, which creates the union of all items from any number of
     * DIAs as a single DIA, where the items are in an arbitrary order.  All
     * input DIAs must contain the same type, which is also the output DIA's
     * type.
     *
     * The Union operation concatenates all _local_ pieces of a DIA, no
     * rebalancing is performed, and no communication is needed.
     *
     * \ingroup dia_lops
     */
    template <typename SecondDIA>
    auto Union(const SecondDIA &second_dia) const;

    //! \}

    //! \name Actions
    //! \{

    /*!
     * Computes the total size of all elements across all workers.
     *
     * \ingroup dia_actions
     */
    size_t Size() const;

    /*!
     * Returns the whole DIA in an std::vector on each worker. This is only for
     * testing purposes and should not be used on large datasets.
     *
     * \ingroup dia_actions
     */
    std::vector<ValueType> AllGather() const;

    /**
     * \brief AllGather is an Action, which returns the whole DIA in an
     * std::vector on each worker. This is only for testing purposes and should
     * not be used on large datasets.
     *
     * \ingroup dia_actions
     */
    void AllGather(std::vector<ValueType>* out_vector) const;

    /*!
     * Print is an Action, which collects all data of the DIA at the worker 0
     * and prints using ostream serialization. It is implemented using Gather().
     *
     * \ingroup dia_actions
     */
    void Print(const std::string& name) const;

    /*!
     * Print is an Action, which collects all data of the DIA at the worker 0
     * and prints using ostream serialization. It is implemented using Gather().
     *
     * \ingroup dia_actions
     */
    void Print(const std::string& name, std::ostream& out) const;

    /*!
     * Gather is an Action, which collects all data of the DIA into a vector at
     * the given worker. This should only be done if the received data can fit
     * into RAM of the one worker.
     *
     * \ingroup dia_actions
     */
    std::vector<ValueType> Gather(size_t target_id = 0) const;

    /*!
     * Gather is an Action, which collects all data of the DIA into a vector at
     * the given worker. This should only be done if the received data can fit
     * into RAM of the one worker.
     *
     * \ingroup dia_actions
     */
    void Gather(size_t target_id, std::vector<ValueType>* out_vector)  const;

    /*!
     * Select up to sample_size items uniformly at random and return a new
     * DIA<T>.
     */
    auto Sample(size_t sample_size) const;

    /*!
     * AllReduce is an Action, which computes the reduction sum of all elements
     * globally and delivers the same value on all workers.
     *
     * \param reduce_function Reduce function.
     *
     * \param initial_value Initial value of the reduction.
     *
     * \ingroup dia_actions
     */
    template <typename ReduceFunction>
    auto AllReduce(const ReduceFunction &reduce_function,
                   const ValueType& initial_value = ValueType()) const;

    /*!
     * Sum is an Action, which computes the sum of all elements globally.
     *
     * \param sum_function Sum function.
     *
     * \param initial_value Initial value of the sum.
     *
     * \ingroup dia_actions
     */
    template <typename SumFunction = std::plus<ValueType> >
    auto Sum(const SumFunction& sum_function = SumFunction(),
             const ValueType& initial_value = ValueType()) const;

    /*!
     * Min is an Action, which computes the minimum of all elements globally.
     *
     * \param min_function Min function.
     *
     * \param initial_value Initial value of the min.
     *
     * \ingroup dia_actions
     */
    template <typename MinFunction = common::minimum<ValueType> >
    auto Min(const MinFunction& min_function = MinFunction(),
             const ValueType& initial_value = ValueType()) const;

    /*!
     * Max is an Action, which computes the maximum of all elements globally.
     *
     * \param max_function Max function.
     *
     * \param initial_value Initial value of the max.
     *
     * \ingroup dia_actions
     */
    template <typename MaxFunction = common::maximum<ValueType> >
    auto Max(const MaxFunction& max_function = MaxFunction(),
             const ValueType& initial_value = ValueType()) const;

    /*!
     * WriteLines is an Action, which writes std::strings to an output file.
     * Strings are written using fstream with a newline after each entry.
     *
     * \param filepath Destination of the output file.
     *
     * \ingroup dia_actions
     */
    void WriteLines(const std::string& filepath) const;

    /*!
     * WriteLinesMany is an Action, which writes std::strings to multiple output
     * files. Strings are written using fstream with a newline after each
     * entry. Each worker creates its individual file.
     *
     * \param filepath Destination of the output file. This filepath must
     * contain two special substrings: "$$$$$" is replaced by the worker id and
     * "#####" will be replaced by the file chunk id. The last occurrences of
     * "$" and "#" are replaced, otherwise "$$$$" and/or "##########" are
     * automatically appended.
     *
     * \param target_file_size target size of each individual file.
     *
     * \ingroup dia_actions
     */
    void WriteLinesMany(const std::string& filepath,
                        size_t target_file_size = 128* 1024* 1024) const;

    /*!
     * WriteBinary is a function, which writes a DIA to many files per
     * worker. The input DIA can be recreated with ReadBinary and equal
     * filepath.
     *
     * \param filepath Destination of the output file. This filepath must
     * contain two special substrings: "$$$$$" is replaced by the worker id and
     * "#####" will be replaced by the file chunk id. The last occurrences of
     * "$" and "#" are replaced, otherwise "$$$$" and/or "##########" are
     * automatically appended.
     *
     * \param max_file_size size limit of individual file.
     *
     * \ingroup dia_actions
     */
    void WriteBinary(const std::string& filepath,
                     size_t max_file_size = 128* 1024* 1024) const;

    //! \}

    //! \name Distributed Operations (DOps)
    //! \{

    /*!
     * ReduceBy is a DOp, which groups elements of the DIA with the
     * key_extractor and reduces each key-bucket to a single element using the
     * associative reduce_function. The reduce_function defines how two elements
     * can be reduced to a single element of equal type.
     *
     * The key of the reduced element has to be equal to the keys of the input
     * elements. Since ReduceBy is a DOp, it creates a new DIANode. The DIA
     * returned by Reduce links to this newly created DIANode. The stack_ of the
     * returned DIA consists of the PostOp of Reduce, as a reduced element can
     * directly be chained to the following LOps.
     *
     * \param key_extractor Key extractor function, which maps each element to a
     * key of possibly different type.
     *
     * \tparam ReduceFunction Type of the reduce_function. This is a function
     * reducing two elements of L's result type to a single element of equal
     * type.
     *
     * \param reduce_function Reduce function, which defines how the key buckets
     * are reduced to a single element. This function is applied associative but
     * not necessarily commutative.
     *
     * \param reduce_config Reduce configuration.
     *
     * \ingroup dia_dops
     */
    template <typename KeyExtractor, typename ReduceFunction,
              typename ReduceConfig = class DefaultReduceConfig>
    auto ReduceByKey(const KeyExtractor &key_extractor,
                     const ReduceFunction &reduce_function,
                     const ReduceConfig& reduce_config = ReduceConfig()) const;

    /*!
     * ReduceByKey is a DOp, which groups elements of the DIA with the
     * key_extractor and reduces each key-bucket to a single element using the
     * associative reduce_function. The reduce_function defines how two elements
     * can be reduced to a single element of equal type.
     *
     * In contrast to ReduceBy, the reduce_function is allowed to change the key
     * (Example: Integers with modulo function as key_extractor). Creates
     * overhead as both key and value have to be sent in shuffle step. Since
     * ReduceByKey is a DOp, it creates a new DIANode. The DIA returned by
     * Reduce links to this newly created DIANode. The stack_ of the returned
     * DIA consists of the PostOp of Reduce, as a reduced element can directly
     * be chained to the following LOps.
     *
     * \param key_extractor Key extractor function, which maps each element to a
     * key of possibly different type.
     *
     * \tparam ReduceFunction Type of the reduce_function. This is a function
     * reducing two elements of L's result type to a single element of equal
     * type.
     *
     * \param reduce_function Reduce function, which defines how the key buckets
     * are reduced to a single element. This function is applied associative but
     * not necessarily commutative.
     *
     * \param reduce_config Reduce configuration.
     *
     * \ingroup dia_dops
     */
    template <typename KeyExtractor, typename ReduceFunction,
              typename ReduceConfig = class DefaultReduceConfig>
    auto ReduceByKey(struct VolatileKeyTag,
                     const KeyExtractor &key_extractor,
                     const ReduceFunction &reduce_function,
                     const ReduceConfig& reduce_config = ReduceConfig()) const;

    /*!
     * ReducePair is a DOp, which groups key-value-pairs in the input DIA by
     * their key and reduces each key-bucket to a single element using the
     * associative reduce_function. The reduce_function defines how two elements
     * can be reduced to a single element of equal type. The reduce_function is
     * allowed to change the key. Since ReducePair is a DOp, it creates a new
     * DIANode. The DIA returned by Reduce links to this newly created
     * DIANode. The stack_ of the returned DIA consists of the PostOp of Reduce,
     * as a reduced element can directly be chained to the following LOps.
     *
     * \tparam ReduceFunction Type of the reduce_function. This is a function
     * reducing two elements of L's result type to a single element of equal
     * type.
     *
     * \param reduce_function Reduce function, which defines how the key buckets
     * are reduced to a single element. This function is applied associative but
     * not necessarily commutative.
     *
     * \param reduce_config Reduce configuration.
     *
     * \ingroup dia_dops
     */
    template <typename ReduceFunction,
              typename ReduceConfig = class DefaultReduceConfig>
    auto ReducePair(const ReduceFunction &reduce_function,
                    const ReduceConfig& reduce_config = ReduceConfig()) const;

    /*!
     * ReduceToIndex is a DOp, which groups elements of the DIA with the
     * key_extractor returning an unsigned integers and reduces each key-bucket
     * to a single element using the associative reduce_function.
     * In contrast to ReduceBy, ReduceToIndex returns a DIA in a defined order,
     * which has the reduced element with key i in position i.
     *
     * The reduce_function defines how two elements can be reduced to a single
     * element of equal type. The key of the reduced element has to be equal
     * to the keys of the input elements. Since ReduceToIndex is a DOp,
     * it creates a new DIANode. The DIA returned by ReduceToIndex links to
     * this newly created DIANode. The stack_ of the returned DIA consists
     * of the PostOp of ReduceToIndex, as a reduced element can
     * directly be chained to the following LOps.
     *
     * \param key_extractor Key extractor function, which maps each element to a
     * key of possibly different type.
     *
     * \tparam ReduceFunction Type of the reduce_function. This is a function
     * reducing two elements of L's result type to a single element of equal
     * type.
     *
     * \param reduce_function Reduce function, which defines how the key buckets
     * are reduced to a single element. This function is applied associative but
     * not necessarily commutative.
     *
     * \param size Resulting DIA size. Consequently, the key_extractor function
     * but always return < size for any element in the input DIA.
     *
     * \param neutral_element Item value with which to start the reduction in
     * each array cell.
     *
     * \param reduce_config Reduce configuration.
     *
     * \ingroup dia_dops
     */
    template <typename KeyExtractor, typename ReduceFunction,
              typename ReduceConfig = class DefaultReduceToIndexConfig>
    auto ReduceToIndex(
        const KeyExtractor &key_extractor,
        const ReduceFunction &reduce_function,
        size_t size,
        const ValueType& neutral_element = ValueType(),
        const ReduceConfig& reduce_config = ReduceConfig()) const;

    /*!
     * ReduceToIndexByKey is a DOp, which groups elements of the DIA with the
     * key_extractor returning an unsigned integers and reduces each key-bucket
     * to a single element using the associative reduce_function.
     * In contrast to ReduceByKey, ReduceToIndexByKey returns a DIA in a defined
     * order, which has the reduced element with key i in position i.
     * The reduce_function defines how two elements can be reduced to a single
     * element of equal type.
     *
     * ReduceToIndexByKey is the equivalent to ReduceByKey, as the
     * reduce_function is allowed to change the key.  Since ReduceToIndexByKey
     * is a DOp, it creates a new DIANode. The DIA returned by ReduceToIndex
     * links to this newly created DIANode. The stack_ of the returned DIA
     * consists of the PostOp of ReduceToIndex, as a reduced element can
     * directly be chained to the following LOps.
     *
     * \param key_extractor Key extractor function, which maps each element to a
     * key of possibly different type.
     *
     * \tparam ReduceFunction Type of the reduce_function. This is a function
     * reducing two elements of L's result type to a single element of equal
     * type.
     *
     * \param reduce_function Reduce function, which defines how the key buckets
     * are reduced to a single element. This function is applied associative but
     * not necessarily commutative.
     *
     * \param size Resulting DIA size. Consequently, the key_extractor function
     * but always return < size for any element in the input DIA.
     *
     * \param neutral_element Item value with which to start the reduction in
     * each array cell.
     *
     * \param reduce_config Reduce configuration.
     *
     * \ingroup dia_dops
     */
    template <typename KeyExtractor, typename ReduceFunction,
              typename ReduceConfig = class DefaultReduceToIndexConfig>
    auto ReduceToIndex(
        struct VolatileKeyTag,
        const KeyExtractor &key_extractor,
        const ReduceFunction &reduce_function,
        size_t size,
        const ValueType& neutral_element = ValueType(),
        const ReduceConfig& reduce_config = ReduceConfig()) const;

    /*!
     * GroupByKey is a DOp, which groups elements of the DIA by its key.
     * After having grouped all elements of one key, all elements of one key
     * will be processed according to the GroupByFunction and returns an output
     * Contrary to Reduce, GroupBy allows usage of functions that require all
     * elements of one key at once as GroupByFunction will be applied _after_
     * all elements with the same key have been grouped. However because of this
     * reason, the communication overhead is also higher. If possible, usage of
     * Reduce is therefore recommended.
     *
     * As GroupBy is a DOp, it creates a new DIANode. The DIA returned by
     * Reduce links to this newly created DIANode. The stack_ of the returned
     * DIA consists of the PostOp of Reduce, as a reduced element can
     * directly be chained to the following LOps.
     *
     * \tparam KeyExtractor Type of the key_extractor function.
     * The key_extractor function is equal to a map function.
     *
     * \param key_extractor Key extractor function, which maps each element to a
     * key of possibly different type.
     *
     * \tparam GroupByFunction Type of the groupby_function. This is a function
     * taking an iterator for all elements of the same key as input.
     *
     * \param groupby_function Reduce function, which defines how the key
     * buckets are grouped and processed.
     *      input param: api::GroupByReader with functions HasNext() and Next()
     *
     * \ingroup dia_dops
     */
    template <typename ValueOut, typename KeyExtractor,
              typename GroupByFunction, typename HashFunction =
                  std::hash<typename FunctionTraits<KeyExtractor>::result_type> >
    auto GroupByKey(const KeyExtractor &key_extractor,
                    const GroupByFunction &groupby_function) const;

    /*!
     * GroupBy is a DOp, which groups elements of the DIA by its key.
     * After having grouped all elements of one key, all elements of one key
     * will be processed according to the GroupByFunction and returns an output
     * Contrary to Reduce, GroupBy allows usage of functions that require all
     * elements of one key at once as GroupByFunction will be applied _after_
     * all elements with the same key have been grouped. However because of this
     * reason, the communication overhead is also higher. If possible, usage of
     * Reduce is therefore recommended.
     *
     * In contrast to GroupBy, GroupToIndex returns a DIA in a defined order,
     * which has the reduced element with key i in position i.
     * As GroupBy is a DOp, it creates a new DIANode. The DIA returned by
     * Reduce links to this newly created DIANode. The stack_ of the returned
     * DIA consists of the PostOp of Reduce, as a reduced element can
     * directly be chained to the following LOps.
     *
     * \tparam KeyExtractor Type of the key_extractor function.
     * The key_extractor function is equal to a map function.
     *
     * \param key_extractor Key extractor function, which maps each element to a
     * key of possibly different type.
     *
     * \tparam GroupByFunction Type of the groupby_function. This is a function
     * taking an iterator for all elements of the same key as input.
     *
     * \param groupby_function Reduce function, which defines how the key
     * buckets are grouped and processed.
     *      input param: api::GroupByReader with functions HasNext() and Next()
     *
     * \param size Resulting DIA size. Consequently, the key_extractor function
     * but always return < size for any element in the input DIA.
     *
     * \param neutral_element Item value with which to start the reduction in
     * each array cell.
     *
     * \ingroup dia_dops
     */
    template <typename ValueOut, typename KeyExtractor,
              typename GroupByFunction>
    auto GroupToIndex(const KeyExtractor &key_extractor,
                      const GroupByFunction &groupby_function,
                      const size_t size,
                      const ValueOut& neutral_element = ValueOut()) const;

    /*!
     * Zip is a DOp, which Zips two DIAs in style of functional programming. The
     * zip_function is used to zip the i-th elements of both input DIAs together
     * to form the i-th element of the output DIA. The type of the output
     * DIA can be inferred from the zip_function.
     *
     * \tparam ZipFunction Type of the zip_function. This is a function with two
     * input elements, both of the local type, and one output element, which is
     * the type of the Zip node.
     *
     * \param zip_function Zip function, which zips two elements together
     *
     * \param second_dia DIA, which is zipped together with the original
     * DIA.
     *
     * \ingroup dia_dops
     */
    template <typename ZipFunction, typename SecondDIA>
    auto Zip(const SecondDIA &second_dia, const ZipFunction &zip_function) const;

    /*!
     * Sort is a DOp, which sorts a given DIA according to the given compare_function.
     *
     * \tparam CompareFunction Type of the compare_function.
     *  Should be (ValueType,ValueType)->bool
     *
     * \param compare_function Function, which compares two elements. Returns true, if
     * first element is smaller than second. False otherwise.
     *
     * \ingroup dia_dops
     */
    template <typename CompareFunction = std::less<ValueType> >
    auto Sort(const CompareFunction& compare_function = CompareFunction()) const;

    /*!
     * Merge is a DOp, which merges two sorted DIAs to a single sorted DIA.
     * Both input DIAs must be used sorted conforming to the given comparator.
     * The type of the output DIA will be the type of this DIA.
     *
     * The merge operation balances all input data, so that each worker will
     * have an equal number of elements when the merge completes.
     *
     * \param comparator Comparator to specify the order of input and output.
     *
     * \param second_dia DIA, which is merged with this DIA.
     *
     * \ingroup dia_dops
     */
    template <typename Comparator = std::less<ValueType>, typename SecondDIA>
    auto Merge(const SecondDIA &second_dia,
               const Comparator& comparator = Comparator()) const;

    /*!
     * PrefixSum is a DOp, which computes the prefix sum of all elements. The sum
     * function defines how two elements are combined to a single element.
     *
     * \param sum_function Sum function (any associative function).
     *
     * \param initial_element Initial element of the sum function.
     *
     * \ingroup dia_dops
     */
    template <typename SumFunction = std::plus<ValueType> >
    auto PrefixSum(const SumFunction& sum_function = SumFunction(),
                   const ValueType& initial_element = ValueType()) const;

    /*!
     * Window is a DOp, which applies a window function to every k
     * consecutive items in a DIA. The window function is also given the index
     * of the first item, and can output zero or more items via an Emitter.
     *
     * \param window_size the size of the delivered window. Signature: TODO(tb).
     *
     * \param window_function Window function applied to each k item.
     *
     * \ingroup dia_dops
     */
    template <typename WindowFunction>
    auto Window(size_t window_size,
                const WindowFunction& window_function = WindowFunction()) const;

    /*!
     * Window is a DOp, which applies a window function to every k
     * consecutive items in a DIA. The window function is also given the index
     * of the first item, and can output zero or more items via an Emitter.
     *
     * \param window_size the size of the delivered window.
     *
     * \param window_function Window function applied to each k item.
     *
     * \ingroup dia_dops
     */
    template <typename WindowFunction>
    auto Window(struct DisjointTag, size_t window_size,
                const WindowFunction &window_function) const;

    /*!
     * FlatWindow is a DOp, which applies a window function to every k
     * consecutive items in a DIA. The window function is also given the index
     * of the first item, and can output zero or more items via an Emitter.
     *
     * \param window_size the size of the delivered window. Signature: TODO(tb).
     *
     * \param window_function Window function applied to each k item.
     *
     * \ingroup dia_dops
     */
    template <typename ValueOut, typename WindowFunction>
    auto FlatWindow(size_t window_size,
                    const WindowFunction& window_function = WindowFunction()) const;

    /*!
     * FlatWindow is a DOp, which applies a window function to every k
     * consecutive items in a DIA. The window function is also given the index
     * of the first item, and can output zero or more items via an Emitter.
     *
     * \param window_size the size of the delivered window. Signature: TODO(tb).
     *
     * \param window_function Window function applied to each k item.
     *
     * \ingroup dia_dops
     */
    template <typename ValueOut, typename WindowFunction>
    auto FlatWindow(struct DisjointTag, size_t window_size,
                    const WindowFunction &window_function) const;

    /*!
     * Concat is a DOp, which concatenates any number of DIAs to a single DIA.
     * All input DIAs must contain the same type, which is also the output DIA's
     * type.
     *
     * The concat operation balances all input data, so that each worker will
     * have an equal number of elements when the concat completes.
     *
     * \ingroup dia_dops
     */
    template <typename SecondDIA>
    auto Concat(const SecondDIA &second_dia) const;

    template <typename SimilarityFunction, typename SecondDIA, typename Threshhold>
    auto TrivialSimJoin(const SecondDIA &second_dia,
                        const SimilarityFunction &comparator,
                        const Threshhold &threshhold) const;

    /*!
     * Create a CollapseNode which is mainly used to collapse the LOp chain into
     * a DIA<T> with an empty stack. This is most often necessary for iterative
     * algorithms, where a DIA<T> reference variable is updated in each
     * iteration.
     *
     * \ingroup dia_dops
     */
    DIA<ValueType> Collapse() const;

    /*!
     * Create a CacheNode which contains all items of a DIA in calculated plain
     * format. This is needed if a DIA is reused many times, in order to avoid
     * recalculating a PostOp multiple times.
     *
     * \ingroup dia_dops
     */
    DIA<ValueType> Cache() const;

    //! \}

private:
    //! The DIANode which DIA points to. The node represents the latest DOp
    //! or Action performed previously.
    DIANodePtr node_;

    //! The local function chain, which stores the chained lambda function from
    //! the last DIANode to this DIA.
    Stack stack_;

    //! DIA serial id for logging, matches DIANode::id_ for DOps.
    size_t id_ = 0;

    //! static DIA (LOp or DOp) node label string, may match DIANode::label_.
    const char* label_ = nullptr;

    //! deliver next DIA serial id
    size_t next_dia_id() { return context().next_dia_id(); }
};

//! \}

} // namespace api

//! imported from api namespace
using api::DIA;

//! imported from api namespace
using api::DisjointTag;

//! imported from api namespace
using api::VolatileKeyTag;

} // namespace thrill

#endif // !THRILL_API_DIA_HEADER

/******************************************************************************/
