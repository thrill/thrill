/*******************************************************************************
 * c7a/api/dop_node.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/

#ifndef C7A_API_WRITE_NODE_HEADER
#define C7A_API_WRITE_NODE_HEADER

namespace c7a {

template <typename T, typename WriteFunction>
class WriteNode : public ActionNode<T> {
public: 
    WriteNode(data::DataManager &data_manager, 
              const DIABaseVector& parents, 
              WriteFunction write_function) 
        : ActionNode<T>(data_manager, parents), write_function_(write_function) {};
    virtual ~WriteNode() {} 

    void execute() {};

    std::string toString() override {
        std::string str 
            = std::string("[WriteNode] Id: ") + std::to_string(DIABase::data_id_);
        return str;
    }

private: 
    WriteFunction write_function_;
};

} // namespace c7a

#endif // !C7A_API_WRITE_NODE_HEADER

/******************************************************************************/
