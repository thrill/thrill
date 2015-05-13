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

#include "dia_base.hpp"

namespace c7a {

class DIABase;

using DIABaseVector = std::vector<DIABase*>;
using DIABasePtr = DIABase*;
using DIASharedVector = std::vector<std::shared_ptr<DIABase> >;
using DIASharedPtr = std::shared_ptr<DIABase>;

} // namespace c7a

#endif // !C7A_API_TYPES_HEADER

/******************************************************************************/
