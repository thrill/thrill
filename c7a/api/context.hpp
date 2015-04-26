/*******************************************************************************
 * c7a/api/context.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/

#ifndef C7A_API_CONTEXT_HEADER
#define C7A_API_CONTEXT_HEADER

#include <cassert>
#include <fstream>
#include <string>
#include <vector>

#include "dia.hpp"
#include "dia_node.hpp"
#include "read_node.hpp"
#include "../data/data_manager.hpp"

namespace c7a {

class Context {
public:
    Context() {};
    virtual ~Context() { }

    template <typename read_fn_t>
    auto ReadFromFileSystem(const std::string& filepath,
                            const read_fn_t &read_fn)
    {
        // Set types
        using read_result_t = typename FunctionTraits<read_fn_t>::result_type;
        using ReadResultNode = ReadNode<read_result_t, read_fn_t>;

        // Create initial lambda function for chaining
        auto id = [](read_result_t t) { return t; };
        // Return new DIA with id function and no parent
        return DIA<read_result_t, decltype(id)>
            (new ReadResultNode(data_manager_, {}, read_fn, filepath), id);
    }

    template <typename T, typename L, typename write_fn_t>
    void WriteToFileSystem(DIA<T, L> dia, std::string filepath,
                           const write_fn_t& write_fn)
    {
        (void) filepath; //TODO remove | to supress warning
        (void) dia ;     //TODO remove | to supress warning
        (void) write_fn;     //TODO remove | to supress warning
    }
private:
    data::DataManager data_manager_;
};

} // namespace c7a

#endif // !C7A_API_CONTEXT_HEADER

/******************************************************************************/
