/*******************************************************************************
 * c7a/api/dia.hpp
 *
 * Interface for Operations, holds pointer to node and lambda from node to state
 ******************************************************************************/

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
#include "reduce_node.hpp"

/*!
 * DIA is the interface between the user and the c7a framework. A DIA can be
 * imagined as an immutable array, even though the data does not need to be 
 * materialized at all. A DIA contains a pointer to a DIANode of type @tparam T,
 * which represents the state after the previous DOp or Action. Additionally, a DIA 
 * stores the local lambda function of type @tparam L, which can transform 
 * elements of the DIANode to elements of this DIA. DOps/Actions create a DIA
 * and a new DIANode, to which the DIA links to. LOps only create a new DIA, which
 * link to the previous DIANode. The types T and L are inferred from the 
 * user-defined function given through the operation.
 *
 * \tparam T Type of elements in this DIA.
 * \tparam L Type of the lambda function to transform elements from the previous
 *  DIANode to elements of this DIA.
 */

namespace c7a {

template <typename T, typename L>
class DIA {
friend class Context;
public:
    /*!
     * Constructor of a new DIA with a pointer to a DIANode and a lambda function
     * from the DIANode to this DIA.
     *
     * \param node Pointer to the last DIANode, DOps and Actions create a new DIANode,
     * LOps link to the DIANode of the previous DIA.
     *     
     * \param lambda Function which can transform elements from the DIANode to elements
     * of this DIA.
     */
    DIA(DIANode<T>* node, L lambda) : node_(node), local_lambda_(lambda) {}

    friend void swap(DIA& first, DIA& second) {
        using std::swap;
        swap(first.data_, second.data_);
    }

    DIA& operator = (DIA rhs) {
        swap(*this, rhs);
        return *this;
    }

    /*!
     * Returns a pointer to the according DIANode.
     */
    DIANode<T>* get_node() {
        return node_;
    }

    /*!
     * Map is a LOp, which maps this DIA according to the @param map_fn given by the user. 
     * The map_fn maps each element of type L::result to one other element of a possibly different
     * type. The DIA returned by Map has the same type T. The lambda function of the returned DIA
     * is this DIA's local_lambda_ chained with map_fn. Therefore the type L of the returned DIA 
     * is a lambda function from T to map_fn::result. 
     *
     * \tparam map_fn_t Type of the map function. The type of the returned DIA is deducted from this type.
     *
     * \param map_fn Map function of type map_fn_t, which maps each element to an element of a possibly
     * different type.
     *
     */
    template <typename map_fn_t>
    auto Map(const map_fn_t &map_fn) {
        // Extract type of this DIA
        using local_input_t
                  = typename FunctionTraits<L>::template arg<0>;

        // Chains the map_fn to local_lambda_ and creates a new chained lambda function.
        // This chained function applies map_fn after the functions in local_lambda_.
        auto chained_lambda = [=](local_input_t i) {
                return map_fn(local_lambda_(i));
            };

        // Return new DIA with same node and chained lambda
        return DIA<T, decltype(chained_lambda)>(node_, chained_lambda);
    }

    /*!
     * FlatMap is a LOp, which maps this DIA according to the function @param flatmap_fn given by the user. 
     * The flatmap_fn maps each element of type L::result to elements of a possibly different
     * type. The flatmap_fn has an emitter function as it's second parameter. This emitter is called
     * once for each element to be emitted. The DIA returned by FlatMap has the same type T. The
     * lambda function of the returned DIA is this DIA's local_lambda_ chained with flatmap_fn.
     * Therefore the type L of the returned DIA is a lambda function from T to map_fn::result. 
     *
     * \tparam flatmap_fn_t Type of the map function. The type of the returned DIA is deducted from this type
     *
     * \param flatmap_fn Map function of type map_fn_t, which maps each element to an element of a possibly
     * different type.  
     */
    template <typename flatmap_fn_t>
    auto FlatMap(const flatmap_fn_t &flatmap_fn) {
        // Extract types of the flatmap_fn
        using emit_fn_t
                  = typename FunctionTraits<flatmap_fn_t>::template arg<1>;
        using emit_arg_t
                  = typename FunctionTraits<emit_fn_t>::template arg<0>;
        // Extract type of this DIA.
        using local_input_t
                  = typename FunctionTraits<L>::template arg<0>;

        // Chains the flatmap_fn to local_lambda_ and creates a new chained lambda function.
        // This chained function applies flatmap_fn after the functions in local_lambda_ and emits
        // output elements.
        auto chained_lambda = [=](local_input_t i) {
                return flatmap_fn(local_lambda_(i), [](emit_arg_t t) {});
            };

        // Return new DIA with same node and chained lambda
        return DIA<T, decltype(chained_lambda)>(node_, chained_lambda);
    }

    /*!
     * Reduce is a DOp, which groups elements of the DIA with the @param key_extractor and reduces each
     * key-bucket to a single element using the associative @param reduce_function. The reduce_function
     * defines, how two elements can be reduced to a single element of equal type. As Reduce is a DOp,
     * it creates a new DIANode with type L::result. The DIA returned by Reduce links to this newly
     * created DIANode. The local_lambda_ of the returned DIA consists of the reduce_function, as a reduced
     * element can directly be chained to the following LOps.
     *
     * \tparam key_extr_fn_t Type of the key_extractor function. This is a function from L::result to a
     * possibly different key type. The key_extractor function is equal to a map function.
     *
     * \tparam reduce_fn_t Type of the reduce_function. This is a function reducing two elements of type
     * L::result to a single element of equal type.
     *
     * \param key_extractor Key extractor function, which maps each element to a key of possibly different type.
     *
     * \param reduce_function Reduce function, which defines how the key buckets are reduced to a
     * single element. This function is applied associative but not necessarily commutative.
     *
     */
    template<typename key_extr_fn_t, typename reduce_fn_t>
    auto Reduce(const key_extr_fn_t& key_extractor, const reduce_fn_t& reduce_function) {
        // Extract types
        using key_t = typename FunctionTraits<key_extr_fn_t>::result_type;
        using dop_result_t 
            = typename FunctionTraits<reduce_fn_t>::result_type;
        using local_input_t
            = typename FunctionTraits<L>::template arg<0>;
        using ReduceResultNode
            = ReduceNode<T, L, key_extr_fn_t, reduce_fn_t>;
        
        // Create new node with local lambas and parent node
        ReduceResultNode* reduce_node 
            = new ReduceResultNode(node_->get_data_manager(), 
                                   { node_ }, 
                                   local_lambda_, 
                                   key_extractor, 
                                   reduce_function);

        // Return new DIA with reduce node and post-op
        return DIA<dop_result_t, decltype(reduce_node->get_post_op())>
            (reduce_node, reduce_node->get_post_op());
    }

    /*!
     * Returns Chuck Norris!
     *
     * \return Chuck Norris
     */
    const std::vector<T> & evil_get_data() const {
        return std::vector<T>{T()};
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
    void PrintNodes () {
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
    //! The DIANode which DIA points to. The node represents the latest DOp or Action performed previously.
    DIANode<T>* node_;
    //! The chained lambda function to transform an element from the previous DIANode to this DIA.
    L local_lambda_;
};

} // namespace c7a

#endif // !C7A_API_DIA_HEADER

/******************************************************************************/
