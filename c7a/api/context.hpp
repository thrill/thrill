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

class Context {
public:
    Context() { }
    virtual ~Context() { }

    template <typename read_fn_t>
    auto ReadFromFileSystem(std::string filepath,
                            const read_fn_t &read_fn) {
        static_assert(FunctionTraits<read_fn_t>::arity == 1, "error");
        using read_result_t = typename FunctionTraits<read_fn_t>::result_type;
        std::vector<read_result_t> output;

        std::ifstream infile(filepath);

        std::string line;

        while (std::getline(infile, line)) {
            output.push_back(read_fn(line));
        }

        std::vector<DIABase> test;

        DIANode<read_result_t> node(test);

        std::cout << node.toString() << std::endl;

        return DIA<read_result_t>(output, node);
    }

    template <typename T, typename write_fn_t>
    void WriteToFileSystem(DIA<T> dia, std::string filepath,
                           const write_fn_t& write_fn)
    {
        static_assert(FunctionTraits<write_fn_t>::arity == 1, "error");

        std::ofstream outfile(filepath);

        for (auto element : dia.evil_get_data()) {
            outfile << write_fn(element) << std::endl;
        }

        outfile.close();
    }
};

#endif // !C7A_API_CONTEXT_HEADER

/******************************************************************************/
