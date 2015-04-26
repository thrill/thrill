/*******************************************************************************
 * c7a/api/dia.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
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

namespace c7a {

template <typename T, typename L>
class DIA {
friend class Context;
public:
    DIA(DIANode<T>* node, L lambda) : node_(node), local_lambda_(lambda) {}

    friend void swap(DIA& first, DIA& second) {
        using std::swap;
        swap(first.data_, second.data_);
    }

    DIA& operator = (DIA rhs) {
        swap(*this, rhs);
        return *this;
    }

    DIANode<T>* get_node() {
        return node_;
    }

    template <typename map_fn_t>
    auto Map(const map_fn_t &map_fn) {
        // Extract Types
        using local_input_t
                  = typename FunctionTraits<L>::template arg<0>;

        // This has to be different for each LOp
        auto chained_lambda = [=](local_input_t i) {
                return map_fn(local_lambda_(i));
            };

        // Return new DIA with same node and chained lambda
        return DIA<T, decltype(chained_lambda)>(node_, chained_lambda);
    };

    template <typename flatmap_fn_t>
    auto FlatMap(const flatmap_fn_t &flatmap_fn) {
        // Extract Types
        using emit_fn_t
                  = typename FunctionTraits<flatmap_fn_t>::template arg<1>;
        using emit_arg_t
                  = typename FunctionTraits<emit_fn_t>::template arg<0>;
        using local_input_t
                  = typename FunctionTraits<L>::template arg<0>;

        // This has to be different for each LOp
        auto chained_lambda = [=](local_input_t i) {
                return flatmap_fn(local_lambda_(i), [](emit_arg_t t) {});
            };

        // Return new DIA with same node and chained lambda
        return DIA<T, decltype(chained_lambda)>(node_, chained_lambda);
    };

    template<typename key_extr_fn_t, typename reduce_fn_t>
    auto Reduce(const key_extr_fn_t& key_extr, const reduce_fn_t& reduce_fn) {
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
                                   key_extr, 
                                   reduce_fn);

        // Return new DIA with reduce node and post-op
        return DIA<dop_result_t, decltype(reduce_node->get_post_op())>
            (reduce_node, reduce_node->get_post_op());
    }

    const std::vector<T> & evil_get_data() const {
        return std::vector<T>{T()};
    }

    std::string NodeString() {
        return node_->ToString();
    }

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
    DIANode<T>* node_;
    L local_lambda_;
};

} // namespace c7a

#endif // !C7A_API_DIA_HEADER

/******************************************************************************/
