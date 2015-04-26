/*******************************************************************************
 * c7a/api/dop_node.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/

#ifndef C7A_API_READ_NODE_HEADER
#define C7A_API_READ_NODE_HEADER

namespace c7a {

template <typename T, typename ReadFunction>
class ReadNode : public DOpNode<T> {
public: 
    ReadNode(data::DataManager &data_manager, 
             const std::vector<DIABase*>& parents, 
             ReadFunction read_function) 
        : DOpNode<T>(data_manager, parents), 
        read_function_(read_function) {};
    virtual ~ReadNode() {} 

    void execute() {};

    std::string ToString() override {
        // Extract type
        using key_t = typename FunctionTraits<ReadFunction>::result_type;
        // Create string
        std::string str 
            = std::string("[ReadNode]");
        return str;
    }

private: 
    ReadFunction read_function_;
};

} // namespace c7a

#endif // !C7A_API_READ_NODE_HEADER

/******************************************************************************/
