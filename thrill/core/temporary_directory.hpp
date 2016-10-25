/*******************************************************************************
 * thrill/core/temporary_directory.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_TEMPORARY_DIRECTORY_HEADER
#define THRILL_CORE_TEMPORARY_DIRECTORY_HEADER

#include <string>

namespace thrill {
namespace core {

/*!
 * A class which creates a temporary directory in the current directory and
 * returns it via get(). When the object is destroyed the temporary directory is
 * wiped non-recursively.
 */
class TemporaryDirectory
{
public:
    //! Create a temporary directory, returns its name without trailing /.
    static std::string make_directory(const char* sample = "thrill-testsuite-");

    //! wipe temporary directory NON RECURSIVELY!
    static void wipe_directory(const std::string& tmp_dir, bool do_rmdir);

    TemporaryDirectory()
        : dir_(make_directory())
    { }

    ~TemporaryDirectory() {
        wipe_directory(dir_, true);
    }

    //! non-copyable: delete copy-constructor
    TemporaryDirectory(const TemporaryDirectory&) = delete;
    //! non-copyable: delete assignment operator
    TemporaryDirectory& operator = (const TemporaryDirectory&) = delete;

    //! return the temporary directory name
    const std::string& get() const { return dir_; }

    //! wipe contents of directory
    void wipe() const {
        wipe_directory(dir_, false);
    }

private:
    std::string dir_;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_TEMPORARY_DIRECTORY_HEADER

/******************************************************************************/
