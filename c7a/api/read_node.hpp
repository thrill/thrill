/*******************************************************************************
 * c7a/api/dop_node.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/

#ifndef C7A_API_READ_NODE_HEADER
#define C7A_API_READ_NODE_HEADER

template <typename T, typename ReadFunction>
class ReadNode : public DOpNode<T> {
public: 
    ReadNode(std::vector<DIABase> parents, ReadFunction read_function) : DOpNode<T>(parents), read_function_(read_function) {};
    virtual ~ReadNode() {} 

    void execute() {};

    std::string toString() {
        using key_t = typename FunctionTraits<ReadFunction>::result_type;
        std::string str = std::string("[ReadNode/Type=[") + typeid(T).name() + "]";
        return str;
    }

private: 
    ReadFunction read_function_;
};

#endif // !C7A_API_READ_NODE_HEADER

/******************************************************************************/
