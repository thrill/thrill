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
 * A DIANode which performs a Reduce operation. Reduce groups the elements in a DIA by their
 * key and reduces every key bucket to a single element each. The ReduceNode stores the
 * key_extractor and the reduce_function UDFs. The chainable LOps ahead of the Reduce operation
 * are stored in the Stack. The ReduceNode has the type T, which is the result type of the
 * reduce_function.
 *
 * \tparam T Output type of the Reduce operation
 * \tparam Stack Function stack, which contains the chained lambdas between the last and this DIANode.
 * \tparam KeyExtractor Type of the key_extractor function.
 * \tparam ReduceFunction Type of the reduce_function
 */
template <typename T, typename U, typename Stack1, typename Stack2, typename ZipFunction>
class TwoZipNode : public DOpNode<T>
{
public:
    /*!
     * Constructor for a ReduceNode. Sets the DataManager, parents, stack, key_extractor and reduce_function.
     *
     * \param data_manager Reference to the DataManager, which gives iterators for data
     * \param parents Vector of parents. Has size 1, as a reduce node only has a single parent
     * \param stack Function stack with all lambdas between the parent and this node
     * \param key_extractor Key extractor function
     * \param reduce_function Reduce function
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
    {
        // This new DIA Allocate is needed to send data from Pre to Main
        // TODO: use network iterate later
        post_data_id_ = (this->context_).get_data_manager().AllocateDIA();
    }

    /*!
     * Returns "[ReduceNode]" as a string.
     * \return "[ReduceNode]"
     */
    std::string ToString() override
    {
        // Create string
        std::string str
            = std::string("[ZipNode]");
        return str;
    }

    /*!
     * Actually executes the reduce operation. Uses the member functions PreOp, MainOp and PostOp.
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

        
        data::DIAId pid1 = this->get_parents()[0]->get_data_id();
        data::DIAId pid2 = this->get_parents()[1]->get_data_id();

        data::BlockIterator<T> it1 = (this->context_).get_data_manager().template GetLocalBlocks<T>(pid1);
        data::BlockIterator<U> it2 = (this->context_).get_data_manager().template GetLocalBlocks<U>(pid2);

        data::BlockEmitter<zip_result_t> emit = (this->context_).get_data_manager().template GetLocalEmitter<zip_result_t>(post_data_id_);

        while (it1.HasNext() && it2.HasNext()) {
            auto item1 = it1.Next();
            auto item2 = it2.Next();

            //std::cout << zip_function_(item1, item2) << std::endl;
            emit(zip_function_(item1, item2));
        }
    }
};
} // namespace c7a

//! \}

#endif // !C7A_API_REDUCE_NODE_HEADER

/******************************************************************************/
