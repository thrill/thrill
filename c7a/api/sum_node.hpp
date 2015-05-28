/*******************************************************************************
 * c7a/api/write_node.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_SUM_NODE_HEADER
#define C7A_API_SUM_NODE_HEADER

#include "action_node.hpp"
#include "function_stack.hpp"

namespace c7a {
    template <typename Input>
    class SumNode : public ActionNode<Input>
    {
    public:
        SumNode(Context& ctx,
                DIANode<Input>* parent)
              : ActionNode<Input>(ctx, { parent })
        {
            core::RunScope(this);
        }

        virtual ~SumNode() { }

        //! Executes the sum operation.
        void execute() override
        {

        }

        /*!
         * Produces an 'empty' function stack, which only contains the identity emitter function.
         * \return Empty function stack
         */
        auto ProduceStack() {
            // Hook Identity
            auto id_fn = [ = ](Input t, std::function<void(Input)> emit_func) {
                return emit_func(t);
            };

            FunctionStack<> stack;
            return stack.push(id_fn);
        }

        /*!
         * Returns "[SumNode]" as a string.
         * \return "[SumNode]"
         */
        std::string ToString() override
        {
            return "[SumNode] Id: " + std::to_string(this->data_id_);
        }
    };
} // namespace c7a

#endif //!C7A_API_SUM_NODE_HEADER

/******************************************************************************/
