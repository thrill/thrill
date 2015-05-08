/*******************************************************************************
 * c7a/api/context.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#ifndef C7A_API_CONTEXT_HEADER
#define C7A_API_CONTEXT_HEADER

#include <cassert>
#include <fstream>
#include <string>
#include <vector>

#include "../data/data_manager.hpp"

namespace c7a {

class Context
{
public:
    Context() { }
    virtual ~Context() { }

    data::DataManager & get_data_manager()
    {
        return data_manager_;
    }

private:
    data::DataManager data_manager_;
};

} // namespace c7a

#endif // !C7A_API_CONTEXT_HEADER

/******************************************************************************/
