/*******************************************************************************
 * c7a/api/dia.hpp
 *
 * Interface for Operations, holds pointer to node and lambda from node to state
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_DIA_HEADER
#define C7A_API_DIA_HEADER

#include <functional>
#include <vector>
#include <stack>
#include <iostream>
#include <fstream>
#include <cassert>
#include <memory>
#include <string>
#include <utility>

#include "dia_node.hpp"
#include "function_traits.hpp"
#include "lop_node.hpp"
#include "read_node.hpp"
#include "context.hpp"
#include "write_node.hpp"
#include "generate_node.hpp"
#include "generate_file_node.hpp"
#include "allgather_node.hpp"

#include <c7a/net/collective_communication.hpp>
#include <c7a/common/future.hpp>

namespace c7a {

//! \addtogroup api Interface
//! \{

/*!
 * DIARef is the interface between the user and the c7a framework. A DIARef can
 * be imagined as an immutable array, even though the data does not need to be
 * materialized at all. A DIARef contains a pointer to a DIANode of type T,
 * which represents the state after the previous DOp or Action. Additionally, a
 * DIARef stores the local lambda function chain of type Stack, which can transform
 * elements of the DIANode to elements of this DIARef. DOps/Actions create a
 * DIARef and a new DIANode, to which the DIARef links to. LOps only create a
 * new DIARef, which link to the previous DIANode.
 *
 * \tparam T Type of elements going into this DIA and LOp Chain.
 *
 * \tparam Stack Type of the function chain.
 */
template <typename T, typename Stack = FunctionStack<> >
class DIARef
{
    friend class Context;
    using DIANodePtr = std::shared_ptr<DIANode<T> >;

public:
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
    DIARef(DIANodePtr& node, Stack& stack)
        : node_(node),
          local_stack_(stack)
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
    DIARef(DIANodePtr&& node, Stack& stack)
        : node_(std::move(node)),
          local_stack_(stack)
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
    DIARef(const DIARef<T, AnyStack>& rhs)
        __attribute__((deprecated))
#if __GNUC__
        // the attribute warning does not work with gcc?
        __attribute__((warning("Casting to DIARef creates LOpNode instead of inline chaining.\n"
                               "Consider whether you can use auto instead of DIARef.")));
#endif

    /*!
     * Returns a pointer to the according DIANode.
     */
    DIANode<T> * get_node() const {
        return node_.get();
    }

    /*!
     * Returns the number of references to the according DIANode.
     */
    int get_node_count() const {
        return node_.use_count();
    }

    /*!
     * Returns the stored function chain.
     */
    const Stack & get_stack() const {
        return local_stack_;
    }

    /*!
     * Map is a LOp, which maps this DIARef according to the map_fn given by the
     * user.  The map_fn maps each element to another
     * element of a possibly different type. The function chain of the returned
     * DIARef is this DIARef's local_stack_ chained with map_fn.
     *
     * \tparam MapFunction Type of the map function.
     *
     * \param map_function Map function of type MapFunction, which maps each element
     * to an element of a possibly different type.
     *
     */
    template <typename MapFunction>
    auto Map(const MapFunction &map_function) {
        using MapArgument
                  = typename FunctionTraits<MapFunction>::template arg<0>;
        auto conv_map_function = [=](MapArgument input, auto emit_func) {
                             emit_func(map_function(input));
                         };

        auto new_stack = local_stack_.push(conv_map_function);
        return DIARef<T, decltype(new_stack)>(node_, new_stack);
    }

    /*!
     * Filter is a LOp, which filters elements from  this DIARef
     * according to the filter_function given by the
     * user. The filter_function maps each element to a boolean.
     * The function chain of the returned DIARef is this DIARef's
     * local_stack_ chained with filter_function.
     *
     * \tparam FilterFunction Type of the map function.
     *
     * \param filter_function Filter function of type FilterFunction, which maps
     * each element to a boolean.
     *
     */
    template <typename FilterFunction>
    auto Filter(const FilterFunction &filter_function) {
        using FilterArgument
                  = typename FunctionTraits<FilterFunction>::template arg<0>;
        auto conv_filter_function = [=](FilterArgument input, auto emit_func) {
                                  if (filter_function(input)) emit_func(input);
                              };

        auto new_stack = local_stack_.push(conv_filter_function);
        return DIARef<T, decltype(new_stack)>(node_, new_stack);
    }

    /*!
     * FlatMap is a LOp, which maps this DIARef according to the
     * flatmap_function given by the user. The flatmap_function maps each
     * element to elements of a possibly different type. The flatmap_function
     * has an emitter function as it's second parameter. This emitter is called
     * once for each element to be emitted. The function chain of the returned
     * DIARef is this DIARef's local_stack_ chained with flatmap_function.
     *
     * \tparam FlatmapFunction Type of the map function.
     *
     * \param flatmap_function Map function of type FlatmapFunction, which maps
     * each element to elements of a possibly different type.
     */
    template <typename FlatmapFunction>
    auto FlatMap(const FlatmapFunction &flatmap_function) {
        auto new_stack = local_stack_.push(flatmap_function);
        return DIARef<T, decltype(new_stack)>(node_, new_stack);
    }

    /*!
     * Reduce is a DOp, which groups elements of the DIARef with the
     * key_extractor and reduces each key-bucket to a single element using the
     * associative reduce_function. The reduce_function defines how two elements
     * can be reduced to a single element of equal type. Since Reduce is a DOp,
     * it creates a new DIANode. The DIARef returned by Reduce links to this
     * newly created DIANode. The local_stack_ of the returned DIARef consists
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
                  const ReduceFunction &reduce_function);

	template <typename KeyExtractor, typename ReduceFunction>
    auto ReduceToIndex(const KeyExtractor &key_extractor,
					   const ReduceFunction &reduce_function, size_t max_index);

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
    auto Zip(const ZipFunction &zip_function, SecondDIA second_dia);

    /*!
     * Sum is an Action, which computes the sum of elements of all workers.
     *
     * \tparam SumFunction Type of the sum_function.
     *
     * \param sum_function Sum function.
     */
    template <typename SumFunction>
    auto Sum(const SumFunction &sum_function);

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
                           const WriteFunction& write_function) {
        using WriteResult = typename FunctionTraits<WriteFunction>::result_type;

        using WriteResultNode = WriteNode<T, WriteResult, WriteFunction,
                                          decltype(local_stack_)>;

        auto shared_node =
            std::make_shared<WriteResultNode>(node_->get_context(),
                                              node_.get(),
                                              local_stack_,
                                              write_function,
                                              filepath);

        auto write_stack = shared_node->ProduceStack();
        core::StageBuilder().RunScope(shared_node.get());
    }

    /*!
     * AllGather is an Action, which returns the whole DIA in an std::vector on
     * each worker. This is only for testing purposes and should not be used on
     * large datasets.
     */
    template<typename Out>
     void AllGather(std::vector<Out>* out_vector) {

        using AllGatherResultNode = AllGatherNode<T, Out, decltype(local_stack_)>;



        auto shared_node =
            std::make_shared<AllGatherResultNode>(node_->get_context(),
                                                  node_.get(),
                                                  local_stack_,
                                                  out_vector);


        core::StageBuilder().RunScope(shared_node.get());
    }

    /*!
     * Returns Chuck Norris!
     *
     * \return Chuck Norris
     */
    const std::vector<T> & evil_get_data() const {
        return (std::vector<T>{ T() });
    }

    /*!
     * Returns the string which defines the DIANode node_.
     *
     * \return The string of node_
     */
    std::string NodeString() {
        return node_->ToString();
    }

    /*!
     * Prints the DIANode and all it's children recursively. The printing is
     * performed tree-style.
     */
    void PrintNodes() {
        using BasePair = std::pair<DIABase*, int>;
        std::stack<BasePair> dia_stack;
        dia_stack.push(std::make_pair(node_, 0));
        while (!dia_stack.empty()) {
            auto curr = dia_stack.top();
            auto node = curr.first;
            int depth = curr.second;
            dia_stack.pop();
            auto is_end = true;
            if (!dia_stack.empty()) is_end = dia_stack.top().second < depth;
            for (int i = 0; i < depth - 1; ++i) {
                std::cout << "│   ";
            }
            if (is_end && depth > 0) std::cout << "└── ";
            else if (depth > 0) std::cout << "├── ";
            std::cout << node->ToString() << std::endl;
            auto children = node->get_childs();
            for (auto c : children) {
                dia_stack.push(std::make_pair(c, depth + 1));
            }
        }
    }

private:
    //! The DIANode which DIARef points to. The node represents the latest DOp
    //! or Action performed previously.
    DIANodePtr node_;

    //! The local function chain, which stores the chained lambda function from
    //! the last DIANode to this DIARef.
    Stack local_stack_;
};

//! \}

template <typename T, typename Stack>
template <typename AnyStack>
DIARef<T,Stack>::DIARef(const DIARef<T, AnyStack>& rhs) {
    // Create new LOpNode.  Transfer stack from rhs to LOpNode.  Build new
    // DIARef with empty stack and LOpNode
    auto rhs_node = std::move(rhs.get_node());
    auto rhs_stack = rhs.get_stack();
    using LOpChainNode
        = LOpNode<T, decltype(rhs_stack)>;

    LOG0 << "WARNING: cast to DIARef creates LOpNode instead of inline chaining.";
    LOG0 << "Consider whether you can use auto instead of DIARef.";

    auto shared_node
        = std::make_shared<LOpChainNode>(rhs_node->get_context(),
                                         rhs_node,
                                         rhs_stack);
    node_ = std::move(shared_node);
    local_stack_ = FunctionStack<>();
}

template <typename ReadFunction>
auto ReadLines(Context & ctx, std::string filepath,
                        const ReadFunction &read_function) {
    using ReadResult = typename FunctionTraits<ReadFunction>::result_type;
    using ReadResultNode = ReadNode<ReadResult, ReadFunction>;

    auto shared_node =
        std::make_shared<ReadResultNode>(ctx,
                                         read_function,
                                         filepath);

    auto read_stack = shared_node->ProduceStack();

    return DIARef<ReadResult, decltype(read_stack)>
               (std::move(shared_node), read_stack);
}

template <typename GeneratorFunction>
auto GenerateFromFile(Context & ctx, std::string filepath,
                      const GeneratorFunction &generator_function,
                      size_t size) {
    using GeneratorResult =
        typename FunctionTraits<GeneratorFunction>::result_type;
    using GenerateResultNode =
        GenerateFileNode<GeneratorResult, GeneratorFunction>;

    auto shared_node =
        std::make_shared<GenerateResultNode>(ctx,
											 generator_function,
											 filepath,
											 size);

    auto generator_stack = shared_node->ProduceStack();

    return DIARef<GeneratorResult, decltype(generator_stack)>
               (std::move(shared_node), generator_stack);
}

template <typename GeneratorFunction>
auto Generate(Context & ctx,
			  const GeneratorFunction &generator_function,
			  size_t size) {
    using GeneratorResult =
        typename FunctionTraits<GeneratorFunction>::result_type;
    using GenerateResultNode =
        GenerateNode<GeneratorResult, GeneratorFunction>;

    auto shared_node =
        std::make_shared<GenerateResultNode>(ctx,
											 generator_function,
											 size);

    auto generator_stack = shared_node->ProduceStack();

    return DIARef<GeneratorResult, decltype(generator_stack)>
               (std::move(shared_node), generator_stack);
}

} // namespace c7a

#endif // !C7A_API_DIA_HEADER

/******************************************************************************/
