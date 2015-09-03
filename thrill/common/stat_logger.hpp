/*******************************************************************************
 * thrill/common/stat_logger.hpp
 *
 * Logger for Stat-Lines, which creates basic JSON lines
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_STAT_LOGGER_HEADER
#define THRILL_COMMON_STAT_LOGGER_HEADER

#include <thrill/common/logger.hpp>

namespace thrill {
namespace common {

class StatLogger {

protected:
    //! collector stream
    std::basic_ostringstream<
        char, std::char_traits<char>, LoggerAllocator<char> > oss_;

	size_t elements_ = 0;

public:
	StatLogger() {
		oss_ << "{";
	};

    //! output any type, including io manipulators
    template <typename AnyType>
    StatLogger& operator << (const AnyType& at) {
		if (elements_ > 0) {
			if (elements_ % 2 == 0) {
				oss_ << ", ";
			} else {
				oss_ << ": ";
			}
		}
		elements_++;
        oss_ << "\"" << at << "\"";
        return *this;
    }

    //! destructor: output a } and a newline
    ~StatLogger() {
		assert (elements_ % 2 == 0);
		oss_ << "}\n";
		std::cout << oss_.str();
	};

};


#define STAT ::thrill::common::StatLogger()



} // namespace common
} // namespace thrill

#endif //!THRILL_COMMON_LOGGER_HEADER

/******************************************************************************/
