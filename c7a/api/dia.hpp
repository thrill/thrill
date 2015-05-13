/*******************************************************************************
 * c7a/api/dia.hpp
 *
 * Interface for Operations, holds pointer to node and lambda from node to state
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
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
#include <unordered_map>
#include <string>

#include "dia_node.hpp"
#include "function_traits.hpp"
#include "lop_node.hpp"
#include "zip_node.hpp"
#include "read_node.hpp"
#include "reduce_node.hpp"
#include "context.hpp"

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
 * \tparam T Type of elements in this DIARef.
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
    {
        // Create new LOpNode
        // Transfer stack from rhs to LOpNode
        // Build new DIARef with empty stack and LOpNode
        auto rhs_node = std::move(rhs.get_node());
        auto rhs_stack = rhs.get_stack();
        using LOpChainNode
                  = LOpNode<T, decltype(rhs_stack)>;

        std::shared_ptr<LOpChainNode> shared_node(
            new LOpChainNode(rhs_node->get_data_manager(),
                             { rhs_node },
                             rhs_stack));
        node_ = std::move(shared_node);
        local_stack_ = FunctionStack<>();
    }

    /*!
     * Returns a pointer to the according DIANode.
     */
    DIANode<T> * get_node() const
    {
        return node_.get();
    }

    /*!
     * Returns the number of references to the according DIANode.
     */
    int get_node_count() const
    {
        return node_.use_count();
    }

    /*!
     * Returns the stored function chain.
     */
    Stack & get_stack()
    {
        return local_stack_;
    }

    /*!
     * Map is a LOp, which maps this DIARef according to the map_fn given by the
     * user.  The map_fn maps each element to another
     * element of a possibly different type. The DIARef returned by Map has the
     * same type T. The function chain of the returned DIARef is this DIARef's
     * local_stack_ chained with map_fn. 
     *
     * \tparam map_fn_t Type of the map function.
     *
     * \param map_fn Map function of type map_fn_t, which maps each element to
     * an element of a possibly different type.
     *
     */
    template <typename map_fn_t>
    auto Map(const map_fn_t &map_fn) {
        using map_arg_t
                  = typename FunctionTraits<map_fn_t>::template arg<0>;
        using map_result_t
                  = typename FunctionTraits<map_fn_t>::result_type;
        auto conv_map_fn = [ = ](map_arg_t input, std::function<void(map_result_t)> emit_func) {
                               emit_func(map_fn(input));
                           };

        auto new_stack = local_stack_.push(conv_map_fn);
        return DIARef<T, decltype(new_stack)>(node_, new_stack);
    }

    /*!
     * FlatMap is a LOp, which maps this DIARef according to the flatmap_fn
     * given by the user.  The flatmap_fn maps each element 
     * to elements of a possibly different type. The flatmap_fn has an emitter
     * function as it's second parameter. This emitter is called once for each
     * element to be emitted. The DIARef returned by FlatMap has the same type
     * T. The function chain of the returned DIARef is this DIARef's
     * local_stack_ chained with flatmap_fn.  
     *
     * \tparam flatmap_fn_t Type of the map function. 
     *
     * \param flatmap_fn Map function of type map_fn_t, which maps each element
     * to elements of a possibly different type.
     */
    template <typename flatmap_fn_t>
    auto FlatMap(const flatmap_fn_t &flatmap_fn) {
        auto new_stack = local_stack_.push(flatmap_fn);
        return DIARef<T, decltype(new_stack)>(node_, new_stack);
    }

    /*!
     * Reduce is a DOp, which groups elements of the DIARef with the
     * key_extractor and reduces each key-bucket to a single element using the
     * associative reduce_function. The reduce_function defines how two elements
     * can be reduced to a single element of equal type. Since Reduce is a DOp, it
     * creates a new DIANode. The DIARef returned by Reduce links to this newly 
     * created DIANode. The local_stack_ of the returned DIARef consists of 
     * the PostOp of Reduce, as a reduced element can
     * directly be chained to the following LOps.
     *
     * \tparam key_extr_fn_t Type of the key_extractor function. 
     * The key_extractor function is equal to a map function.
     *
     * \param key_extractor Key extractor function, which maps each element to a
     * key of possibly different type.
     *
     */
    template <typename key_extr_fn_t>
    auto ReduceBy(const key_extr_fn_t &key_extractor) {
        return ReduceSugar<key_extr_fn_t>(key_extractor, node_.get(), local_stack_);
    }

    /*!
     * Zip is a DOp, which Zips two DIAs in style of functional programming. The
     * zip_function is used to zip the i-th elements of both input DIAs together
     * to form the i-th element of the output DIARef. The type of the output
     * DIARef can be inferred from the zip_function.
     *
     * \tparam zip_fn_t Type of the zip_function. This is a function with two
     * input elements, both of the local type, and one output element, which is
     * the type of the Zip node.
     *
     * \param zip_fn Zip function, which zips two elements together
     *
     * \param second_dia DIARef, which is zipped together with the original
     * DIARef.
     */
    template <typename zip_fn_t, typename second_dia_t>
    auto Zip(const zip_fn_t &zip_fn, second_dia_t second_dia) {
        using zip_result_t
                  = typename FunctionTraits<zip_fn_t>::result_type;
        using zip_arg_0_t
                  = typename FunctionTraits<zip_fn_t>::template arg<0>;
        using zip_arg_1_t
                  = typename FunctionTraits<zip_fn_t>::template arg<1>;
        using ZipResultNode
                  = TwoZipNode<T, zip_arg_1_t, zip_result_t,
                               decltype(local_stack_), decltype(second_dia.get_stack()), zip_fn_t>;

        std::shared_ptr<ZipResultNode> shared_node(
            new ZipResultNode(node_->get_data_manager(),
                              node_.get(),
                              second_dia.get_node(),
                              local_stack_,
                              second_dia.get_stack(),
                              zip_fn));

        auto zip_stack = shared_node->ProduceStack();
        return DIARef<zip_result_t, decltype(zip_stack)>
                   (std::move(shared_node), zip_stack);
    }

    /*!
     * Returns Chuck Norris!
     *
     * \return Chuck Norris
     */
    const std::vector<T> & evil_get_data() const
    {
        return (std::vector<T>{ T() });
    }

    /*!
     * Returns the string which defines the DIANode node_.
     *
     * \return The string of node_
     */
    std::string NodeString()
    {
        return node_->ToString();
    }

    /*!
     * Prints the DIANode and all it's children recursively. The printing is
     * performed tree-style.
     */
    void PrintNodes()
    {
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

    /*!
     * Syntactic sugaaah for reduce
     */
    template <typename key_extr_fn_t>
    class ReduceSugar
    {
    public:
        ReduceSugar(const key_extr_fn_t& key_extractor, DIANode<T>* node, Stack& local_stack)
            : key_extractor_(key_extractor), node_(node), local_stack_(local_stack) { }

        /*!
         * Syntactic sugaaah
         *
         * \tparam reduce_fn_t Type of the reduce_function. This is a function
         * reducing two elements of L's result type to a single element of equal
         * type.
         *
         * \param reduce_function Reduce function, which defines how the key
         * buckets are reduced to a single element. This function is applied
         * associative but not necessarily commutative.
         *
         */
        template <typename reduce_fn_t>
        auto With(const reduce_fn_t &reduce_function) {
            using dop_result_t
                      = typename FunctionTraits<reduce_fn_t>::result_type;
            using ReduceResultNode
                      = ReduceNode<T, dop_result_t, decltype(local_stack_), key_extr_fn_t, reduce_fn_t>;

            std::shared_ptr<ReduceResultNode> shared_node(
                new ReduceResultNode(node_->get_data_manager(),
                                     node_,
                                     local_stack_,
                                     key_extractor_,
                                     reduce_function));

            auto reduce_stack = shared_node->ProduceStack();

            return DIARef<dop_result_t, decltype(reduce_stack)>
                       (std::move(shared_node), reduce_stack);
        }

    private:
        const key_extr_fn_t& key_extractor_;
        DIANode<T>* node_;
        Stack& local_stack_;
    };
};

//! \}

template <typename read_fn_t>
auto ReadFromFileSystem(Context & ctx, std::string filepath,
                        const read_fn_t &read_fn) {
    using read_result_t = typename FunctionTraits<read_fn_t>::result_type;
    using ReadResultNode = ReadNode<read_result_t, read_fn_t>;

    std::shared_ptr<ReadResultNode>
    shared_node(new ReadResultNode(ctx,
                                   read_fn,
                                   filepath));

    auto read_stack = shared_node->ProduceStack();

    return DIARef<read_result_t, decltype(read_stack)>
               (std::move(shared_node), read_stack);
}

} // namespace c7a

#endif // !C7A_API_DIA_HEADER

/******************************************************************************/
