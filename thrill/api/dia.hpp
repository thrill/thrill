/*******************************************************************************
 * thrill/api/dia.hpp
 *
 * Interface for Operations, holds pointer to node and lambda from node to state
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Huyen Chau Nguyen <hello@chau-nguyen.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_DIA_HEADER
#define THRILL_API_DIA_HEADER

#include <thrill/api/context.hpp>
#include <thrill/api/dia_node.hpp>
#include <thrill/api/function_stack.hpp>
#include <thrill/api/stats_graph.hpp>
#include <thrill/common/function_traits.hpp>
#include <thrill/common/functional.hpp>

#include <cassert>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * DIARef is the interface between the user and the Thrill framework. A DIARef
 * can be imagined as an immutable array, even though the data does not need to
 * be which represents the state after the previous DOp or Action. Additionally,
 * a DIARef stores the local lambda function chain of type Stack, which can
 * transform elements of the DIANode to elements of this DIARef. DOps/Actions
 * create a DIARef and a new DIANode, to which the DIARef links to. LOps only
 * create a new DIARef, which link to the previous DIANode.
 *
 * \tparam ValueType Type of elements currently in this DIA.
 * \tparam Stack Type of the function chain.
 */
template <typename _ValueType, typename _Stack = FunctionStack<_ValueType> >
class DIARef
{
    friend class Context;

    template <typename Function>
    using FunctionTraits = common::FunctionTraits<Function>;

public:
    //! Type of this function stack
    using Stack = _Stack;

    //! type of the items delivered by the DOp, and pushed down the function
    //! stack towards the next nodes. If the function stack contains LOps nodes,
    //! these may transform the type.
    using StackInput = typename Stack::Input;

    //! type of the items virtually in the DIA, which is the type emitted by the
    //! current LOp stack.
    using ValueType = _ValueType;

    //! type of pointer to the real node object implementation. This object has
    //! base item type StackInput which is transformed by the function stack
    //! lambdas further. But even pushing more lambdas does not change the stack
    //! input type.
    using DIANodePtr = std::shared_ptr<DIANode<StackInput> >;

    //! default-constructor: invalid DIARef
    DIARef()
        : node_(nullptr)
    { }

    //! Return whether the DIARef is valid.
    bool IsValid() const { return node_.get() != nullptr; }

    /*!
     * Constructor of a new DIARef with a pointer to a DIANode and a
     * function chain from the DIANode to this DIARef.
     *
     * \param node Pointer to the last DIANode, DOps and Actions create a new
     * DIANode, LOps link to the DIANode of the previous DIARef.
     *
     * \param stack Function stack consisting of functions between last DIANode
     * and this DIARef.
     */
    DIARef(const DIANodePtr& node, const Stack& stack,
           const std::vector<StatsNode*>& stats_parents)
        : node_(node),
          stack_(stack),
          stats_parents_(stats_parents)
    { }

    /*!
     * Constructor of a new DIARef supporting move semantics of nodes.
     *
     * \param node Pointer to the last DIANode, DOps and Actions create a new
     * DIANode, LOps link to the DIANode of the previous DIARef.
     *
     * \param stack Function stack consisting of functions between last DIANode
     * and this DIARef.
     */
    DIARef(DIANodePtr&& node, const Stack& stack,
           const std::vector<StatsNode*>& stats_parents)
        : node_(std::move(node)),
          stack_(stack),
          stats_parents_(stats_parents)
    { }

    /*!
     * Copy-Constructor of a DIARef with empty function chain from a DIARef with
     * a non-empty chain.  The functionality of the chain is stored in a newly
     * created LOpNode.  The current DIARef than points to this LOpNode.  This
     * is needed to support assignment operations between DIARef's.
     *
     * \param rhs DIA containing a non-empty function chain.
     */
    template <typename AnyStack>
    DIARef(const DIARef<ValueType, AnyStack>& rhs)
#if __GNUC__ && !__clang__
    // the attribute warning does not work with gcc?
    __attribute__ ((warning(     // NOLINT
                        "Casting to DIARef creates LOpNode instead of inline chaining.\n"
                        "Consider whether you can use auto instead of DIARef.")))
#elif __GNUC__ && __clang__
    __attribute__ ((deprecated)) // NOLINT
#endif
    ;                            // NOLINT

    //! Returns a pointer to the according DIANode.
    const DIANodePtr & node() const {
        assert(IsValid());
        return node_;
    }

    //! Returns the number of references to the according DIANode.
    size_t node_refcount() const {
        assert(IsValid());
        return node_.use_count();
    }

    //! Returns the stored function chain.
    const Stack & stack() const {
        assert(IsValid());
        return stack_;
    }

    StatsNode * AddChildStatsNode(const char* label, const DIANodeType& type) const {
        StatsNode* node = node_->context().stats_graph().AddNode(label, type);
        for (const auto& parent : stats_parents_)
            node_->context().stats_graph().AddEdge(parent, node);
        return node;
    }

    void AppendChildStatsNode(StatsNode* stats_node) const {
        for (const auto& parent : stats_parents_)
            node_->context().stats_graph().AddEdge(parent, stats_node);
    }

    Context & ctx() const {
        assert(IsValid());
        return node_->context();
    }

    /*!
     * Mark the referenced DIANode for keeping, which makes children not consume
     * the data when executing. This does not create a new DIA, but returns the
     * existing one.
     */
    DIARef & Keep() {
        assert(IsValid());
        node_->SetConsume(false);
        return *this;
    }

    /*!
     * Mark the referenced DIANode as consuming, which makes it only executable
     * once. This does not create a new DIA, but returns the existing one.
     */
    DIARef & Consume() {
        assert(IsValid());
        node_->SetConsume(true);
        return *this;
    }

    /*!
     * Map is a LOp, which maps this DIARef according to the map_fn given by the
     * user.  The map_fn maps each element to another
     * element of a possibly different type. The function chain of the returned
     * DIARef is this DIARef's stack_ chained with map_fn.
     *
     * \tparam MapFunction Type of the map function.
     *
     * \param map_function Map function of type MapFunction, which maps each
     * element to an element of a possibly different type.
     */
    template <typename MapFunction>
    auto Map(const MapFunction &map_function) const {
        assert(IsValid());

        using MapArgument
                  = typename FunctionTraits<MapFunction>::template arg<0>;
        using MapResult
                  = typename FunctionTraits<MapFunction>::result_type;
        auto conv_map_function = [=](MapArgument input, auto emit_func) {
                                     emit_func(map_function(input));
                                 };

        static_assert(
            std::is_convertible<ValueType, MapArgument>::value,
            "MapFunction has the wrong input type");

        auto new_stack = stack_.push(conv_map_function);
        return DIARef<MapResult, decltype(new_stack)>(
            node_, new_stack, { AddChildStatsNode("Map", DIANodeType::LAMBDA) });
    }

    /*!
     * Filter is a LOp, which filters elements from this DIARef according to the
     * filter_function given by the user. The filter_function maps each element
     * to a boolean.  The function chain of the returned DIARef is this DIARef's
     * stack_ chained with filter_function.
     *
     * \tparam FilterFunction Type of the map function.
     *
     * \param filter_function Filter function of type FilterFunction, which maps
     * each element to a boolean.
     *
     */
    template <typename FilterFunction>
    auto Filter(const FilterFunction &filter_function) const {
        assert(IsValid());

        using FilterArgument
                  = typename FunctionTraits<FilterFunction>::template arg<0>;
        auto conv_filter_function = [=](FilterArgument input, auto emit_func) {
                                        if (filter_function(input)) emit_func(input);
                                    };

        static_assert(
            std::is_convertible<ValueType, FilterArgument>::value,
            "FilterFunction has the wrong input type");

        auto new_stack = stack_.push(conv_filter_function);
        return DIARef<ValueType, decltype(new_stack)>(
            node_, new_stack, { AddChildStatsNode("Filter", DIANodeType::LAMBDA) });
    }

    /*!
     * FlatMap is a LOp, which maps this DIARef according to the
     * flatmap_function given by the user. The flatmap_function maps each
     * element to elements of a possibly different type. The flatmap_function
     * has an emitter function as it's second parameter. This emitter is called
     * once for each element to be emitted. The function chain of the returned
     * DIARef is this DIARef's stack_ chained with flatmap_function.
     *
     * \tparam ResultType ResultType of the FlatmapFunction, if different from
     * item type of DIA.
     *
     * \tparam FlatmapFunction Type of the map function.
     *
     * \param flatmap_function Map function of type FlatmapFunction, which maps
     * each element to elements of a possibly different type.
     */
    template <typename ResultType = ValueType, typename FlatmapFunction>
    auto FlatMap(const FlatmapFunction &flatmap_function) const {
        assert(IsValid());

        auto new_stack = stack_.push(flatmap_function);
        return DIARef<ResultType, decltype(new_stack)>(node_, new_stack, { AddChildStatsNode("FlatMap", DIANodeType::LAMBDA) });
    }

    /*!
     * ReduceBy is a DOp, which groups elements of the DIARef with the
     * key_extractor and reduces each key-bucket to a single element using the
     * associative reduce_function. The reduce_function defines how two elements
     * can be reduced to a single element of equal type. The key of the reduced
     * element has to be equal to the keys of the input elements. Since ReduceBy
     * is a DOp, it creates a new DIANode. The DIARef returned by Reduce links
     * to this newly created DIANode. The stack_ of the returned DIARef consists
     * of the PostOp of Reduce, as a reduced element can
     * directly be chained to the following LOps.
     *
     * \tparam KeyExtractor Type of the key_extractor function.  The
     * key_extractor function is equal to a map function.
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
     */
    template <typename KeyExtractor, typename ReduceFunction>
    auto ReduceBy(const KeyExtractor &key_extractor,
                  const ReduceFunction &reduce_function) const;

    /*!
    * ReduceByKey is a DOp, which groups elements of the DIARef with the
    * key_extractor and reduces each key-bucket to a single element using the
    * associative reduce_function. The reduce_function defines how two elements
    * can be reduced to a single element of equal type.In contrast to ReduceBy,
    * the reduce_function is allowed to change the key (Example: Integers
    * with modulo function as key_extractor). Creates overhead as both key and
    * value have to be sent in shuffle step. Since ReduceByKey
    * is a DOp, it creates a new DIANode. The DIARef returned by Reduce links
    * to this newly created DIANode. The stack_ of the returned DIARef consists
    * of the PostOp of Reduce, as a reduced element can
    * directly be chained to the following LOps.
    *
    * \tparam KeyExtractor Type of the key_extractor function.
    * The key_extractor function is equal to a map function.
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
    */
    template <typename KeyExtractor, typename ReduceFunction>
    auto ReduceByKey(const KeyExtractor &key_extractor,
                     const ReduceFunction &reduce_function) const;

    /*!
     * ReducePair is a DOp, which groups key-value-pairs in the input DIARef by
     * their key and reduces each key-bucket to a single element using the
     * associative reduce_function. The reduce_function defines how two elements
     * can be reduced to a single element of equal type. The reduce_function is
     * allowed to change the key. Since ReducePair
     * is a DOp, it creates a new DIANode. The DIARef returned by Reduce links
     * to this newly created DIANode. The stack_ of the returned DIARef consists
     * of the PostOp of Reduce, as a reduced element can
     * directly be chained to the following LOps.
     *
     * \tparam ReduceFunction Type of the reduce_function. This is a function
     * reducing two elements of L's result type to a single element of equal
     * type.
     *
     * \param reduce_function Reduce function, which defines how the key buckets
     * are reduced to a single element. This function is applied associative but
     * not necessarily commutative.
     */
    template <typename ReduceFunction>
    auto ReducePair(const ReduceFunction &reduce_function) const;

    /*!
     * ReduceToIndex is a DOp, which groups elements of the DIARef with the
     * key_extractor returning an unsigned integers and reduces each key-bucket
     * to a single element using the associative reduce_function.
     * In contrast to ReduceBy, ReduceToIndex returns a DIA in a defined order,
     * which has the reduced element with key i in position i.
     * The reduce_function defines how two elements can be reduced to a single
     * element of equal type. The key of the reduced element has to be equal
     * to the keys of the input elements. Since ReduceToIndex is a DOp,
     * it creates a new DIANode. The DIARef returned by ReduceToIndex links to
     * this newly created DIANode. The stack_ of the returned DIARef consists
     * of the PostOp of ReduceToIndex, as a reduced element can
     * directly be chained to the following LOps.
     *
     * \tparam KeyExtractor Type of the key_extractor function.
     * The key_extractor function is equal to a map function and has an unsigned
     * integer as it's output type.
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
     */
    template <typename KeyExtractor, typename ReduceFunction>
    auto ReduceToIndex(const KeyExtractor &key_extractor,
                       const ReduceFunction &reduce_function,
                       size_t size,
                       const ValueType& neutral_element = ValueType()) const;

    /*!
     * ReduceToIndexByKey is a DOp, which groups elements of the DIARef with the
     * key_extractor returning an unsigned integers and reduces each key-bucket
     * to a single element using the associative reduce_function.
     * In contrast to ReduceByKey, ReduceToIndexByKey returns a DIA in a defined
     * order, which has the reduced element with key i in position i.
     * The reduce_function defines how two elements can be reduced to a single
     * element of equal type. ReduceToIndexByKey is the equivalent to
     * ReduceByKey, as the reduce_function is allowed to change the key.
     * Since ReduceToIndexByKey is a DOp,
     * it creates a new DIANode. The DIARef returned by ReduceToIndex links to
     * this newly created DIANode. The stack_ of the returned DIARef consists
     * of the PostOp of ReduceToIndex, as a reduced element can
     * directly be chained to the following LOps.
     *
     * \tparam KeyExtractor Type of the key_extractor function.
     * The key_extractor function is equal to a map function and has an unsigned
     * integer as it's output type.
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
     */
    template <typename KeyExtractor, typename ReduceFunction>
    auto ReduceToIndexByKey(const KeyExtractor &key_extractor,
                            const ReduceFunction &reduce_function,
                            size_t size,
                            const ValueType& neutral_element = ValueType()) const;

    /*!
     * ReducePairToIndex is a DOp, which groups key-value-pairs of the input
     * DIARef by their key, which has to be an unsigned integer. Each key-bucket
     * is reduced to a single element using the associative reduce_function.
     * In contrast to Reduce, ReduceToIndex returns a DIA in a defined order,
     * which has the reduced element with key i in position i.
     * The reduce_function defines how two elements can be reduced to a single
     * element of equal type. The reduce_function is allowed to change the key.
     * Since ReduceToIndex is a DOp,
     * it creates a new DIANode. The DIARef returned by ReduceToIndex links to
     * this newly created DIANode. The stack_ of the returned DIARef consists
     * of the PostOp of ReduceToIndex, as a reduced element can
     * directly be chained to the following LOps.
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
     */
    template <typename ReduceFunction>
    auto ReducePairToIndex(
        const ReduceFunction &reduce_function, size_t size,
        const typename FunctionTraits<ReduceFunction>::result_type&
        neutral_element = typename FunctionTraits<ReduceFunction>::result_type()) const;

    /*!
     * GroupBy is a DOp, which groups elements of the DIARef by its key.
     * After having grouped all elements of one key, all elements of one key
     * will be processed according to the GroupByFunction and returns an output
     * Contrary to Reduce, GroupBy allows usage of functions that require all
     * elements of one key at once as GroupByFunction will be applied _after_
     * all elements with the same key have been grouped. However because of this
     * reason, the communication overhead is also higher. If possible, usage of
     * Reduce is therefore recommended.
     * As GroupBy is a DOp, it creates a new DIANode. The DIARef returned by
     * Reduce links to this newly created DIANode. The stack_ of the returned
     * DIARef consists of the PostOp of Reduce, as a reduced element can
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
     */
    template <typename ValueOut,
              typename KeyExtractor,
              typename GroupByFunction,
              typename HashFunction =
                  std::hash<typename common::FunctionTraits<KeyExtractor>::result_type> >
    auto GroupBy(const KeyExtractor &key_extractor,
                 const GroupByFunction &reduce_function) const;

    /*!
     * GroupBy is a DOp, which groups elements of the DIARef by its key.
     * After having grouped all elements of one key, all elements of one key
     * will be processed according to the GroupByFunction and returns an output
     * Contrary to Reduce, GroupBy allows usage of functions that require all
     * elements of one key at once as GroupByFunction will be applied _after_
     * all elements with the same key have been grouped. However because of this
     * reason, the communication overhead is also higher. If possible, usage of
     * Reduce is therefore recommended.
     * In contrast to GroupBy, GroupByIndex returns a DIA in a defined order,
     * which has the reduced element with key i in position i.
     * As GroupBy is a DOp, it creates a new DIANode. The DIARef returned by
     * Reduce links to this newly created DIANode. The stack_ of the returned
     * DIARef consists of the PostOp of Reduce, as a reduced element can
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
     */
    template <typename ValueOut,
              typename KeyExtractor,
              typename GroupByFunction,
              typename HashFunction =
                  std::hash<typename common::FunctionTraits<KeyExtractor>::result_type> >
    auto GroupByIndex(const KeyExtractor &key_extractor,
                      const GroupByFunction &reduce_function,
                      const std::size_t number_keys,
                      const ValueOut& neutral_element = ValueOut()) const;

    /*!
     * Zip is a DOp, which Zips two DIAs in style of functional programming. The
     * zip_function is used to zip the i-th elements of both input DIAs together
     * to form the i-th element of the output DIARef. The type of the output
     * DIARef can be inferred from the zip_function.
     *
     * \tparam ZipFunction Type of the zip_function. This is a function with two
     * input elements, both of the local type, and one output element, which is
     * the type of the Zip node.
     *
     * \param zip_function Zip function, which zips two elements together
     *
     * \param second_dia DIARef, which is zipped together with the original
     * DIARef.
     */
    template <typename ZipFunction, typename SecondDIA>
    auto Zip(SecondDIA second_dia, const ZipFunction &zip_function) const;

    /*!
     * TODO
     */
    template <typename MergeFunction, typename SecondDIA>
    auto Merge(
        SecondDIA second_dia, const MergeFunction &zip_function) const;

    /*!
     * PrefixSum is a DOp, which computes the prefix sum of all elements. The sum
     * function defines how two elements are combined to a single element.
     *
     * \tparam SumFunction Type of the sum_function.
     *
     * \param sum_function Sum function (any associative function).
     *
     * \param initial_element Initial element of the sum function.
     */
    template <typename SumFunction = std::plus<ValueType> >
    auto PrefixSum(const SumFunction& sum_function = SumFunction(),
                   const ValueType& initial_element = ValueType()) const;

    /*!
     * Sort is a DOp, which sorts a given DIA according to the given compare_function.
     *
     * \tparam CompareFunction Type of the compare_function.
     *  Should be (ValueType,ValueType)->bool
     *
     * \param compare_function Function, which compares two elements. Returns true, if
     * first element is smaller than second. False otherwise.
     */
    template <typename CompareFunction = std::less<ValueType> >
    auto Sort(const CompareFunction& compare_function = std::less<ValueType>()) const;

    /*!
     * Sum is an Action, which computes the sum of all elements globally.
     *
     * \tparam SumFunction Type of the sum_function.
     *
     * \param sum_function Sum function.
     *
     * \param initial_value Initial value of the sum.
     */
    template <typename SumFunction = std::plus<ValueType> >
    auto Sum(const SumFunction& sum_function = std::plus<ValueType>(),
             const ValueType& initial_value = ValueType()) const;

    /*!
     * Size is an Action, which computes the size of all elements in all workers.
     */
    size_t Size() const;

    /*!
     * WriteLines is an Action, which writes std::strings to an output file.
     * Strings are written using fstream with a newline after each entry.
     *
     * \param filepath Destination of the output file.
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
     */
    void WriteBinary(const std::string& filepath,
                     size_t max_file_size = 128* 1024* 1024) const;

    /*!
     * AllGather is an Action, which returns the whole DIA in an std::vector on
     * each worker. This is only for testing purposes and should not be used on
     * large datasets.
     */
    void AllGather(std::vector<ValueType>* out_vector) const;

    /*!
     * AllGather is an Action, which returns the whole DIA in an std::vector on
     * each worker. This is only for testing purposes and should not be used on
     * large datasets. Variant that returns the vector.
     */
    std::vector<ValueType> AllGather() const;

    /*!
     * Gather is an Action, which collects all data of the DIA into a vector at
     * the given worker. This should only be done if the received data can fit
     * into RAM of the one worker.
     */
    std::vector<ValueType> Gather(size_t target_id) const;

    /*!
     * Gather is an Action, which collects all data of the DIA into a vector at
     * the given worker. This should only be done if the received data can fit
     * into RAM of the one worker.
     */
    void Gather(size_t target_id, std::vector<ValueType>* out_vector)  const;

    auto Collapse() const;

    auto Cache() const;

    /*!
     * Returns the string which defines the DIANode node_.
     *
     * \return The string of node_
     */
    std::string NodeString() const {
        return node_->ToString();
    }

private:
    //! The DIANode which DIARef points to. The node represents the latest DOp
    //! or Action performed previously.
    DIANodePtr node_;

    //! The local function chain, which stores the chained lambda function from
    //! the last DIANode to this DIARef.
    Stack stack_;

    std::vector<StatsNode*> stats_parents_;
};

/*!
 * ReadLines is a DOp, which reads a file from the file system and
 * creates an ordered DIA according to a given read function.
 *
 * \param ctx Reference to the context object
 * \param filepath Path of the file in the file system
 */
DIARef<std::string> ReadLines(Context& ctx, std::string filepath);

/*!
 * GenerateFromFile is a DOp, which reads a file from the file system and
 * applies the generate function on each line. The DIA is generated by
 * pulling random (possibly duplicate) elements out of those generated
 * elements.
 *
 * \tparam GeneratorFunction Type of the generator function.
 *
 * \param ctx Reference to the context object
 * \param filepath Path of the file in the file system
 * \param generator_function Generator function, which is performed on each
 * element
 * \param size Size of the output DIA
 */
template <typename GeneratorFunction>
auto GenerateFromFile(Context & ctx, std::string filepath,
                      const GeneratorFunction &generator_function,
                      size_t size);

//! \}

} // namespace api

//! imported from api namespace
using api::DIARef;

} // namespace thrill

#endif // !THRILL_API_DIA_HEADER

/******************************************************************************/
