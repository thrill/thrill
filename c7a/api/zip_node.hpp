/*******************************************************************************
 * c7a/api/zip_node.hpp
 *
 * DIANode for a reduce operation. Performs the actual reduce operation
 ******************************************************************************/

#ifndef C7A_API_ZIP_NODE_HEADER
#define C7A_API_ZIP_NODE_HEADER

#include <unordered_map>
#include <functional>
#include "dop_node.hpp"
#include "context.hpp"
#include "function_stack.hpp"
#include "../common/logger.hpp"

namespace c7a {
//! \addtogroup api Interface
//! \{

/*!
 * A DIANode which performs a Zip operation. Zip combines two DIAs element-by-element. The ZipNode
 * stores the zip_function UDF. The chainable LOps are stored in the Stack.
 *
 * \tparam T Output type of the Zip operation.
 * \tparam Stack1 Function stack, which contains the chained lambdas between the last and this DIANode
 * for first input DIA.
 * \tparam Stack2 Function stack, which contains the chained lambdas between the last and this DIANode
 * for secondt input DIA.
 * \tparam Zip_Function Type of the ZipFunction.
 */
template <typename T, typename Stack1, typename Stack2, typename ZipFunction>
class TwoZipNode : public DOpNode<T>
{
public:
    /*!
     * Constructor for a ZipNode.
     *
     * \param ctx Reference to the Context, which gives iterators for data
     * \param parents Vector of parents. Has size 2, as Zip has two parents
     * \param stack1 Function stack with all lambdas between the parent and this node for first DIA
     * \param stack2 Function stack with all lambdas between the parent and this node for second DIA
     * \param zip_function Zip function used to zip elements.
     */
    TwoZipNode(Context& ctx,
               const std::vector<DIABase*>& parents,
               Stack1& stack1,
               Stack2& stack2,
               ZipFunction zip_function)
        : DOpNode<T>(ctx, parents),
          stack1_(stack1),
          stack2_(stack2),
          zip_function_(zip_function)
    { }

    /*!
     * Returns "[ZipNode]" as a string.
     * \return "[ZipNode]"
     */
    std::string ToString() override
    {
        // Create string
        std::string str
            = std::string("[ZipNode]");
        return str;
    }

    /*!
     * Actually executes the zip operation. Uses the member functions PreOp, MainOp and PostOp.
     */
    void execute() override
    {
        PreOp();
        MainOp();
        PostOp();
    }

    /*!
     * TODO: I have no idea...
     */
    auto ProduceStack() {
        using zip_t
                  = typename FunctionTraits<ZipFunction>::result_type;

        auto id_fn = [ = ](zip_t t, std::function<void(zip_t)> emit_func) {
                         return emit_func(t);
                     };

        FunctionStack<> stack;
        return stack.push(id_fn);
    }

private:
    //! Local stacks
    Stack1 stack1_;
    Stack2 stack2_;
    //! Zip function
    ZipFunction zip_function_;
    //!Unique ID of this node, used by the DataManager
    data::DIAId post_data_id_;    

    //! Zip PreOp does nothing. First part of Zip is a PrefixSum, which needs a global barrier.
    void PreOp() {
        assert((this->parents_).size() == 2);
    }

   
    //!Recieve elements from other workers.
    void MainOp() { 
        //TODO: (as soon as we have network) Compute PrefixSum of number of elements in both parent nodes.

        //TODO: Deterministic computation about index to worker mapping.

        //TODO: Use network to send and recieve data through network iterators

        assert((this->parents_).size() == 2);      

    }

    //! Use the ZipFunction to Zip workers
    void PostOp()
    {
        using zip_result_t = typename FunctionTraits<ZipFunction>::result_type;
        using zip_arg_0_t = typename FunctionTraits<ZipFunction>::template arg<0>;
        using zip_arg_1_t = typename FunctionTraits<ZipFunction>::template arg<1>;
        
        data::DIAId pid1 = this->get_parents()[0]->get_data_id();
        data::DIAId pid2 = this->get_parents()[1]->get_data_id();

        data::BlockIterator<zip_arg_0_t> it1 = (this->context_).get_data_manager().template GetLocalBlocks<zip_arg_0_t>(pid1);
        data::BlockIterator<zip_arg_1_t> it2 = (this->context_).get_data_manager().template GetLocalBlocks<zip_arg_1_t>(pid2);

	std::vector<zip_arg_0_t> elements1;
	std::vector<zip_arg_1_t> elements2;

	
	auto save_fn1 = [&elements1](zip_arg_0_t input) {
	  elements1.push_back(input);
	};
        auto lop_chain1 = stack1_.push(save_fn1).emit();

	auto save_fn2 = [&elements2](zip_arg_1_t input) {
	  elements2.push_back(input);
	};
        auto lop_chain2 = stack2_.push(save_fn2).emit();


        data::BlockEmitter<zip_result_t> emit = (this->context_).get_data_manager().template GetLocalEmitter<zip_result_t>(this->data_id_);

        while (it1.HasNext() && it2.HasNext()) {
            auto item1 = it1.Next();
            auto item2 = it2.Next();

	    lop_chain1(item1);
	    lop_chain2(item2);
  
	}

	for (size_t i = 0; i < elements1.size(); i++) {
	  std::cout << zip_function_(elements1[i], elements2[i]) << std::endl;
	  emit(zip_function_(elements1[i], elements2[i]));
	}

    }
};

} // namespace c7a

//! \}

#endif // !C7A_API_REDUCE_NODE_HEADER

/******************************************************************************/
