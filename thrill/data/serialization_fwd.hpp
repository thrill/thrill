/*******************************************************************************
 * thrill/data/serialization_fwd.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_SERIALIZATION_FWD_HEADER
#define THRILL_DATA_SERIALIZATION_FWD_HEADER

namespace thrill {
namespace data {

//! \addtogroup data_layer
//! \{

/*************** Base Template and Callable Serialize/Deserialize *************/

template <typename Archive, typename T, typename Enable = void>
struct Serialization;

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_SERIALIZATION_FWD_HEADER

/******************************************************************************/
