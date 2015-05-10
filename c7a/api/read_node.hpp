/*******************************************************************************
 * c7a/api/read_node.hpp
 *
 * DIANode for a reduce operation. Performs the actual reduce operation
 ******************************************************************************/

#ifndef C7A_API_READ_NODE_HEADER
#define C7A_API_READ_NODE_HEADER

#include "../common/logger.hpp"
#include "dop_node.hpp"
#include "function_stack.hpp"
#include <string>

namespace c7a {
//! \addtogroup api Interface
//! \{

/*!
 * A DIANode which performs a Read operation. Read reads a file from the file system and
 * emits it to the DataManager according to a given read function.
 *
 * \tparam T Output type of the Read operation.
 * \tparam ReadFunction Type of the read function.
 */
template <typename T, typename ReadFunction>
class ReadNode : public DOpNode<T>
{
public:
    /*!
    * Constructor for a ReadNode. Sets the DataManager, parents, read_function and file path.
    *
    * \param ctx Reference to Context, which holds references to data and network.
    * \param parents Vector of parents. Is empty, as read has no previous operations
    * \param read_function Read function, which defines how each line of the file is read and emitted
    * \param path_in Path of the input file
    */
    ReadNode(Context& ctx,
             const std::vector<DIABase*>& parents,
             ReadFunction read_function,
             std::string path_in)
        : DOpNode<T>(ctx, parents),
          read_function_(read_function),
          path_in_(path_in)
    { }
    virtual ~ReadNode() { }

    //!Executes the read operation. Reads a file line by line and emits it to the DataManager after
    //!applying the read function on it.
    void execute()
    {
        // BlockEmitter<T> GetLocalEmitter(DIAId id) {
        SpacingLogger(true) << "READING data with id" << this->data_id_;

        std::ifstream file(path_in_);
        assert(file.good());

        data::InputLineIterator iter = (this->context_).get_data_manager().GetInputLineIterator(file);
        data::BlockEmitter<T> emit = (this->context_).get_data_manager().template GetLocalEmitter<T>(this->data_id_);

        std::string line;
        while (iter.HasNext()) {
            //SpacingLogger(true) << iter.Next();
            emit(read_function_(iter.Next()));
        }
    }

    /*!
     * Produces an 'empty' function stack, which only contains the identity emitter function.
     * \return Empty function stack
     */
    auto ProduceStack() {
        using read_t
                  = typename FunctionTraits<ReadFunction>::result_type;

        auto id_fn = [ = ](read_t t, std::function<void(read_t)> emit_func) {
                         return emit_func(t);
                     };

        FunctionStack<> stack;
        return stack.push(id_fn);
    }

    /*!
     * Returns "[ReadNode]" as a string.
     * \return "[ReadNode]"
     */
    std::string ToString() override
    {
        // Create string
        std::string str
            = std::string("[ReadNode]");
        return str;
    }

private:
    //! The read function which is applied on every line read.
    ReadFunction read_function_;
    //! Path of the input file.
    std::string path_in_;
};
} // namespace c7a

//! \}

#endif // !C7A_API_READ_NODE_HEADER

/******************************************************************************/
