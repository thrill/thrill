/*******************************************************************************
 * c7a/api/zip_node.hpp
 *
 * DIANode for a reduce operation. Performs the actual reduce operation
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_ZIP_NODE_HEADER
#define C7A_API_ZIP_NODE_HEADER

#include <c7a/api/dop_node.hpp>
#include <c7a/api/context.hpp>
#include <c7a/api/function_stack.hpp>
#include <c7a/common/logger.hpp>

#include <unordered_map>
#include <functional>
#include <string>
#include <vector>

namespace c7a {

//! \addtogroup api Interface
//! \{

/*!
 * A DIANode which performs a Zip operation. Zip combines two DIAs
 * element-by-element. The ZipNode stores the zip_function UDF. The chainable
 * LOps are stored in the Stack.
 *
 * \tparam Output Output type of the Zip operation.
 *
 * \tparam Stack1 Function stack, which contains the chained lambdas between the
 * last and this DIANode for first input DIA.
 *
 * \tparam Stack2 Function stack, which contains the chained lambdas between the
 * last and this DIANode for secondt input DIA.
 *
 * \tparam Zip_Function Type of the ZipFunction.
 */
template <typename Input1, typename Input2, typename Output,
          typename Stack1, typename Stack2, typename ZipFunction>
class TwoZipNode : public DOpNode<Output>
{
    static const bool debug = true;

    using zip_arg_0_t = typename FunctionTraits<ZipFunction>::template arg<0>;
    using zip_arg_1_t = typename FunctionTraits<ZipFunction>::template arg<1>;

public:
    /*!
     * Constructor for a ZipNode.
     *
     * \param ctx Reference to the Context, which gives iterators for data
     * \param parent1 First parent of the ZipNode
     * \param parent2 Second parent of the ZipNode
     * \param stack1 Function stack with all lambdas between the parent and this node for first DIA
     * \param stack2 Function stack with all lambdas between the parent and this node for second DIA
     * \param zip_function Zip function used to zip elements.
     */
    TwoZipNode(Context& ctx,
               DIANode<Input1>* parent1,
               DIANode<Input2>* parent2,
               Stack1& stack1,
               Stack2& stack2,
               ZipFunction zip_function)
        : DOpNode<Output>(ctx, { parent1, parent2 }),
          stack1_(stack1),
          stack2_(stack2),
          zip_function_(zip_function)
    {
        // Hook PreOp(s)
        auto pre_op1_fn = [ = ](Input1 input) {
                              PreOp(input);
                          };
        auto pre_op2_fn = [ = ](Input2 input) {
                              PreOpSecond(input);
                          };
        auto lop_chain1 = stack1_.push(pre_op1_fn).emit();
        auto lop_chain2 = stack2_.push(pre_op2_fn).emit();

        parent1->RegisterChild(lop_chain1);
        parent2->RegisterChild(lop_chain2);
    }

    /*!
     * Actually executes the zip operation. Uses the member functions PreOp,
     * MainOp and PostOp.
     */
    void execute() override
    {
        MainOp();
        // get data from data manager
        data::BlockIterator<Output> it = (this->context_).get_data_manager().template GetLocalBlocks<Output>(this->data_id_);
        // loop over inputs
        while (it.HasNext()) {
            auto item = it.Next();
            for (auto func : DIANode<Output>::callbacks_) {
                func(item);
            }
        }
    }

    /*!
     * TODO(an): I have no idea...
     */
    auto ProduceStack() {
        // Hook PostOp
        auto post_op_fn = [ = ](Output elem, std::function<void(Output)> emit_func) {
                              return PostOp(elem, emit_func);
                          };

        FunctionStack<> stack;
        return stack.push(post_op_fn);
    }

    /*!
     * Returns "[ZipNode]" as a string.
     * \return "[ZipNode]"
     */
    std::string ToString() override
    {
        return "[ZipNode]";
    }

private:
    //! Local stacks
    Stack1 stack1_;
    Stack2 stack2_;
    //! Zip function
    ZipFunction zip_function_;
    //! Local storage
    std::vector<zip_arg_0_t> elements1_;
    std::vector<zip_arg_1_t> elements2_;

    //! Zip PreOp does nothing. First part of Zip is a PrefixSum, which needs a
    //! global barrier.
    void PreOp(zip_arg_0_t input)
    {
        LOG << "PreOp(First): " << input;
        elements1_.push_back(input);
    }

    // TODO(an): Theoretically we need two PreOps?
    void PreOpSecond(zip_arg_1_t input)
    {
        LOG << "PreOp(Second): " << input;
        elements2_.push_back(input);
    }

    //!Recieve elements from other workers.
    void MainOp()
    {
        //TODO(an): (as soon as we have network) Compute PrefixSum of number of elements in both parent nodes.

        //TODO(an): Deterministic computation about index to worker mapping.

        //TODO(an): Use network to send and recieve data through network iterators

        data::BlockEmitter<Output> emit = (this->context_).get_data_manager().template GetLocalEmitter<Output>(this->data_id_);

        unsigned int smaller = elements1_.size() < elements2_.size() ?
                               elements1_.size() :
                               elements2_.size();

        LOG << "MainOp: ";

        for (size_t i = 0; i < smaller; ++i) {
            Output zipped = zip_function_(elements1_[i], elements2_[i]);
            LOG << zipped;
            emit(zipped);
        }
    }

    //! Use the ZipFunction to Zip workers
    void PostOp(Output input, std::function<void(Output)> emit_func)
    {
        LOG << "PostOp: " << input;
        emit_func(input);
    }
};

} // namespace c7a

//! \}

#endif // !C7A_API_ZIP_NODE_HEADER

/******************************************************************************/
