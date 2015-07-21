/*******************************************************************************
 * c7a/api/dia.hpp
 *
 * Interface for Operations, holds pointer to node and lambda from node to state
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_DIA_HEADER
#define C7A_API_DIA_HEADER

#include <c7a/api/context.hpp>
#include <c7a/api/dia_node.hpp>
#include <c7a/api/function_stack.hpp>
#include <c7a/common/function_traits.hpp>
#include <c7a/common/functional.hpp>

#include <cassert>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <stack>
#include <string>
#include <utility>
#include <vector>

namespace c7a {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename T>
class DIANode;

/*!
 * DIARef is the interface between the user and the c7a framework. A DIARef can
 * be imagined as an immutable array, even though the data does not need to be
 * which represents the state after the previous DOp or Action. Additionally, a
 * DIARef stores the local lambda function chain of type Stack, which can transform
 * elements of the DIANode to elements of this DIARef. DOps/Actions create a
 * DIARef and a new DIANode, to which the DIARef links to. LOps only create a
 * new DIARef, which link to the previous DIANode.
 *
 * \tparam ValueType Type of elements currently in this DIA.
 * \tparam Stack Type of the function chain.
 */
template <typename ValueType, typename _Stack = FunctionStack<ValueType> >
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
    using ItemType = ValueType;

    //! type of pointer to the real node object implementation. This object has
    //! base item type StackInput which is transformed by the function stack
    //! lambdas further. But even pushing more lambdas does not change the stack
    //! input type.
    using DIANodePtr = std::shared_ptr<DIANode<StackInput> >;

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
    DIARef(const DIANodePtr& node, const Stack& stack)
        : node_(node),
          stack_(stack)
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
    DIARef(DIANodePtr&& node, const Stack& stack)
        : node_(std::move(node)),
          stack_(stack)
    { }

    /*!
     * Copy-Constructor of a DIARef with empty function chain
     * from a DIARef with a non-empty chain.
     * The functionality of the chain is stored in a newly created LOpNode.
     * The current DIARef than points to this LOpNode.
     * This is needed to support assignment operations between DIARef's.
     *
     * \param rhs DIA containing a non-empty function chain.
     */
    template <typename AnyStack>
    DIARef(const DIARef<ValueType, AnyStack>& rhs)
#if __GNUC__ && !__clang__
    // the attribute warning does not work with gcc?
    __attribute__ ((warning("Casting to DIARef creates LOpNode instead of inline chaining.\n"
                            "Consider whether you can use auto instead of DIARef.")))
#else
    __attribute__ ((deprecated))
#endif
    ;

    /*!
     * Returns a pointer to the according DIANode.
     */
    const DIANodePtr & node() const {
        return node_;
    }

    /*!
     * Returns the number of references to the according DIANode.
     */
    size_t node_refcount() const {
        return node_.use_count();
    }

    /*!
     * Returns the stored function chain.
     */
    const Stack & stack() const {
        return stack_;
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
        using MapArgument
                  = typename FunctionTraits<MapFunction>::template arg<0>;
        using MapResult
                  = typename common::FunctionTraits<MapFunction>::result_type;
        auto conv_map_function = [=](MapArgument input, auto emit_func) {
                                     emit_func(map_function(input));
                                 };

        static_assert(
            std::is_same<MapArgument, ValueType>::value,
            "MapFunction has the wrong input type");

        auto new_stack = stack_.push(conv_map_function);
        return DIARef<MapResult, decltype(new_stack)>(node_, new_stack);
    }

    /*!
     * Filter is a LOp, which filters elements from  this DIARef
     * according to the filter_function given by the
     * user. The filter_function maps each element to a boolean.
     * The function chain of the returned DIARef is this DIARef's
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
        using FilterArgument
                  = typename common::FunctionTraits<FilterFunction>::template arg<0>;
        auto conv_filter_function = [=](FilterArgument input, auto emit_func) {
                                        if (filter_function(input)) emit_func(input);
                                    };

        static_assert(
            std::is_same<FilterArgument, ValueType>::value,
            "FilterFunction has the wrong input type");

        auto new_stack = stack_.push(conv_filter_function);
        return DIARef<ValueType, decltype(new_stack)>(node_, new_stack);
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
        auto new_stack = stack_.push(flatmap_function);
        return DIARef<ResultType, decltype(new_stack)>(node_, new_stack);
    }

    /*!
     * Reduce is a DOp, which groups elements of the DIARef with the
     * key_extractor and reduces each key-bucket to a single element using the
     * associative reduce_function. The reduce_function defines how two elements
     * can be reduced to a single element of equal type. Since Reduce is a DOp,
     * it creates a new DIANode. The DIARef returned by Reduce links to this
     * newly created DIANode. The stack_ of the returned DIARef consists
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
    auto ReduceBy(const KeyExtractor &key_extractor,
                  const ReduceFunction &reduce_function) const;

    /*!
     * ReduceToIndex is a DOp, which groups elements of the DIARef with the
     * key_extractor returning an unsigned integers and reduces each key-bucket
     * to a single element using the associative reduce_function.
     * In contrast to Reduce, ReduceToIndex returns a DIA in a defined order,
     * which has the reduced element with key i in position i.
     * The reduce_function defines how two elements can be reduced to a single
     * element of equal type. Since ReduceToIndex is a DOp, it creates a new
     * DIANode. The DIARef returned by ReduceToIndex links to this
     * newly created DIANode. The stack_ of the returned DIARef consists
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
     * \param max_index Largest index given by the key_extractor function for
     * any element in the input DIA.
     *
     * \param neutral_element Item value with which to start the reduction in
     * each array cell.
     */
    template <typename KeyExtractor, typename ReduceFunction>
    auto ReduceToIndex(const KeyExtractor &key_extractor,
                       const ReduceFunction &reduce_function,
                       size_t max_index,
                       ValueType neutral_element = ValueType()) const;

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
    auto Zip(const ZipFunction &zip_function, SecondDIA second_dia) const;

    /*!
     * PrefixSum is a DOp, which computes the prefix sum of all elements. The sum
     * function defines how two elements are combined to a single element.
     *
     * \tparam SumFunction Type of the sum_function.
     *
     * \param sum_function Sum function (any associative function).
     *
     * \param neutral_element Neutral element of the sum function.
     */
    template <typename SumFunction = common::SumOp<ValueType> >
    auto PrefixSum(const SumFunction& sum_function = SumFunction(),
                   ValueType neutral_element = ValueType()) const;

    /*!
     * Sum is an Action, which computes the sum of all elements globally.
     *
     * \tparam SumFunction Type of the sum_function.
     *
     * \param sum_function Sum function.
     *
     * \param initial_value Initial value of the sum.
     */
    template <typename SumFunction>
    auto Sum(const SumFunction& sum_function = common::SumOp<ValueType>(),
             ValueType initial_value = ValueType()) const;

    /*!
     * Size is an Action, which computes the size of all elements in all workers.
     */
    size_t Size() const;

    /*!
     * WriteToFileSystem is an Action, which writes elements to an output file.
     * A provided function is used prepare the elements before written.
     *
     * \tparam WriteFunction Type of the write_function. This is a function with
     * one input element of the local type.
     *
     * \param write_function Write function, which prepares an element to be
     * written to disk.
     *
     * \param filepath Destination of the output file.
     */
    template <typename WriteFunction>
    void WriteToFileSystem(const std::string& filepath,
                           const WriteFunction& write_function) const;

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
};

/*!
 * ReadLines is a DOp, which reads a file from the file system and
 * creates an ordered DIA according to a given read function.
 *
 * \tparam ReadFunction Type of the read function.
 *
 * \param ctx Reference to the context object
 * \param filepath Path of the file in the file system
 * \param read_function Read function, which is performed on each
 * element
 */
template <typename ReadFunction>
auto ReadLines(Context & ctx, std::string filepath,
               const ReadFunction &read_function);

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

/*!
 * Generate is a DOp, which creates an DIA according to a generator
 * function. This function is used to generate a DIA of a certain size by
 * applying it to integers from 0 to size - 1.
 *
 * \tparam GeneratorFunction Type of the generator function. Input type has to
 * be unsigned integer
 *
 * \param ctx Reference to the context object
 * \param generator_function Generator function, which maps integers from 0 to size - 1
 * to elements.
 * \param size Size of the output DIA
 */
template <typename GeneratorFunction>
auto Generate(Context & ctx,
              const GeneratorFunction &generator_function,
              size_t size);

//! \}

} // namespace api
} // namespace c7a

#endif // !C7A_API_DIA_HEADER

/******************************************************************************/
