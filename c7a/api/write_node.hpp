/*******************************************************************************
 * c7a/api/dop_node.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/

#ifndef C7A_API_WRITE_NODE_HEADER
#define C7A_API_WRITE_NODE_HEADER

template <typename T, typename WriteFunction>
class WriteNode : public ActionNode<T> {
public: 
    WriteNode(std::vector<DIABase> parents, WriteFunction write_function) : ActionNode<T>(parents), write_function_(write_function) {};
    virtual ~WriteNode() {} 

    void execute() {};

    std::string toString() {
        using key_t = typename FunctionTraits<WriteFunction>::result_type;
        std::string str = std::string("[WriteNode/Type=[") + typeid(T).name() + "]";
        return str;
    }

private: 
    WriteFunction write_function_;
};

#endif // !C7A_API_WRITE_NODE_HEADER

/******************************************************************************/
