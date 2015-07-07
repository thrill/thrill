/*******************************************************************************
 * c7a/api/types.hpp
 *
 * Typedefs.
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_TYPES_HEADER
#define C7A_API_TYPES_HEADER

#include <c7a/api/dia_base.hpp>

#include <vector>

namespace c7a {
namespace api {

class DIABase;

using DIABaseVector = std::vector<DIABase*>;
using DIABasePtr = DIABase *;
using DIASharedVector = std::vector<std::shared_ptr<DIABase> >;
using DIASharedPtr = std::shared_ptr<DIABase>;

} // namespace api
} // namespace c7a

#endif // !C7A_API_TYPES_HEADER

/******************************************************************************/
