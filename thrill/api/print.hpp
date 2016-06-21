/*******************************************************************************
 * thrill/api/print.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_PRINT_HEADER
#define THRILL_API_PRINT_HEADER

#include <thrill/api/gather.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace api {

template <typename ValueType, typename Stack>
void DIA<ValueType, Stack>::Print(const std::string& name, std::ostream& os) const {
    assert(IsValid());

    using GatherNode = api::GatherNode<ValueType>;

    std::vector<ValueType> output;

    auto node = common::MakeCounting<GatherNode>(*this, "Print", 0, &output);

    node->RunScope();

    if (node->context().my_rank() == 0)
    {
        os << name
           << " --- Begin DIA.Print() --- size=" << output.size() << '\n';
        for (size_t i = 0; i < output.size(); ++i) {
            os << name << '[' << i << "]: " << output[i] << '\n';
        }
        os << name
           << " --- End DIA.Print() --- size=" << output.size() << std::endl;
    }
}

template <typename ValueType, typename Stack>
void DIA<ValueType, Stack>::Print(const std::string& name) const {
    return Print(name, std::cout);
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_PRINT_HEADER

/******************************************************************************/
