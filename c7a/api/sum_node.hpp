/*******************************************************************************
 * c7a/api/write_node.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_SUM_NODE_HEADER
#define C7A_API_SUM_NODE_HEADER

#include "action_node.hpp"
#include "function_stack.hpp"
#include <c7a/net/net_group.hpp>

namespace c7a {
template <typename Input, typename Output, typename Stack, typename SumFunction>
class SumNode : public ActionNode<Input>
{
    static const bool debug = true;

    using sum_arg_0_t = typename FunctionTraits<SumFunction>::template arg<0>;

public:
    SumNode(Context& ctx,
            DIANode<Input>* parent,
            Stack& stack,
            SumFunction sum_function)
          : ActionNode<Input>(ctx, { parent }),
            sum_function_(sum_function),
            stack_(stack)
    {
        core::RunScope(this); // TODO(ms): find a way to move that to ActionNode

        // Hook PreOp(s)
        auto pre_op_fn = [=](Input input) {
            PreOp(input);
        };

        auto lop_chain = stack_.push(pre_op_fn).emit();

        parent->RegisterChild(lop_chain);
    }

    virtual ~SumNode() { }

    //! Executes the sum operation.
    void execute() override
    {
        MainOp();
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
private:
    //! Local stack
    Stack stack_;
    //! The sum function which is applied to two elements.
    SumFunction sum_function_;
    //! Local storage
    std::vector<sum_arg_0_t> elements_;
    // Sum to be returned
    Output sum;

    void PreOp(sum_arg_0_t input) {
        LOG << "PreOp: " << input;
        elements_.push_back(input);

        // TODO(ms): compute local sum in an online fashion,
    }

    void MainOp() {

        // TODO(ms): worker other than root must compute local sum too before forwarding it!
        // TODO(ms): Where do we get the net group from?
        std::unique_ptr<net::NetGroup> group = std::unique_ptr<net::NetGroup>(new net::NetGroup(1, 1));

        // process the reduce
        group->ReduceToRoot<Output, SumFunction>(sum, sum_function_);

        // broadcast to all other worker
        group->Broadcast(sum);
    }

    void PostOp() {

    }
};

} // namespace c7a

#endif //!C7A_API_SUM_NODE_HEADER

/******************************************************************************/
