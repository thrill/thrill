/*******************************************************************************
 * c7a/api/dop_node.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/

#ifndef C7A_API_READ_NODE_HEADER
#define C7A_API_READ_NODE_HEADER

#include "../common/logger.hpp"
#include <string>

namespace c7a {

template <typename T, typename ReadFunction>
class ReadNode : public DOpNode<T> {
public: 
    ReadNode(data::DataManager &data_manager, 
             const std::vector<DIABase*>& parents, 
             ReadFunction read_function,
             std::string path_in) 
        : DOpNode<T>(data_manager, parents), 
        read_function_(read_function),
        path_in_(path_in)
        {};
    virtual ~ReadNode() {} 

    void execute() {
        // BlockEmitter<T> GetLocalEmitter(DIAId id) {
        SpacingLogger(true) << "READING data with id" << this->data_id_;
        data::BlockEmitter<T> emit = (this->data_manager_).template GetLocalEmitter<T>(this->data_id_);

        std::ifstream infile(path_in_);
        assert(infile.good());

        std::string line;
        while(std::getline(infile, line)) {
            emit(read_function_(line));
        }
    };

    std::string ToString() override {
        // Create string
        std::string str 
            = std::string("[ReadNode]");
        return str;
    }

private: 
    ReadFunction read_function_;
    std::string path_in_;
};

} // namespace c7a

#endif // !C7A_API_READ_NODE_HEADER

/******************************************************************************/
