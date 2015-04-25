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
    auto ReadFromFileSystem(std::string filepath,
                            const read_fn_t &read_fn) {
        static_assert(FunctionTraits<read_fn_t>::arity == 1, "error");
        using read_result_t = typename FunctionTraits<read_fn_t>::result_type;
        (void) filepath; //TODO remove | to supress warning
        // std::vector<read_result_t> output;
        // std::ifstream infile(filepath);
        // std::string line;
        // while (std::getline(infile, line)) {
        //     output.push_back(read_fn(line));
        // }

        // std::vector<DIABase> test;
        using ReadResultNode = ReadNode<read_result_t, read_fn_t>;

        return DIA<read_result_t>(new ReadResultNode(data_manager_, {}, read_fn));
    }

    template <typename T, typename write_fn_t>
    void WriteToFileSystem(DIA<T> dia, std::string filepath,
                           const write_fn_t& write_fn)
    {
        (void) filepath; //TODO remove | to supress warning
        (void) dia ;     //TODO remove | to supress warning
        (void) write_fn;     //TODO remove | to supress warning
        //  static_assert(FunctionTraits<write_fn_t>::arity == 1, "error");
        //  std::ofstream outfile(filepath);
        //  for (auto element : dia.evil_get_data()) {
        //      outfile << write_fn(element) << std::endl;
        //  }
        //  outfile.close();
    }
private:
    data::DataManager data_manager_;
};

} // namespace c7a

#endif // !C7A_API_CONTEXT_HEADER

/******************************************************************************/
