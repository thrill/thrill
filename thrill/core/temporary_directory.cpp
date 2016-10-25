/*******************************************************************************
 * thrill/core/temporary_directory.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/core/temporary_directory.hpp>

#include <thrill/common/logger.hpp>
#include <thrill/common/system_exception.hpp>

#if !defined(_MSC_VER)

#include <dirent.h>
#include <unistd.h>

#else

#include <io.h>
#include <windows.h>

#endif

namespace thrill {
namespace core {

#if defined(_MSC_VER)

std::string TemporaryDirectory::make_directory(const char* sample) {

    char temp_file_path[MAX_PATH + 1] = { 0 };
    unsigned success = ::GetTempFileName(".", sample, 0, temp_file_path);
    if (!success) {
        throw common::ErrnoException(
                  "Could not allocate temporary directory "
                  + std::string(temp_file_path));
    }

    if (!DeleteFile(temp_file_path)) {
        throw common::ErrnoException(
                  "Could not create temporary directory "
                  + std::string(temp_file_path));
    }

    if (!CreateDirectory(temp_file_path, nullptr)) {
        throw common::ErrnoException(
                  "Could not create temporary directory "
                  + std::string(temp_file_path));
    }

    return temp_file_path;
}

void TemporaryDirectory::wipe_directory(
    const std::string& tmp_dir, bool do_rmdir) {

    WIN32_FIND_DATA ff_data;
    HANDLE h = FindFirstFile((tmp_dir + "\\*").c_str(), &ff_data);

    if (h == INVALID_HANDLE_VALUE) {
        throw common::ErrnoException(
                  "FindFirstFile failed:" + std::to_string(GetLastError()));
    }

    do {
        if (!(ff_data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)) {
            std::string path = tmp_dir + "\\" + ff_data.cFileName;

            if (!DeleteFile(path.c_str())) {
                sLOG1 << "Could not unlink temporary file" << path
                      << ":" << strerror(errno);
            }
        }
    } while (FindNextFile(h, &ff_data) != 0);

    DWORD e = GetLastError();
    if (e != ERROR_NO_MORE_FILES) {
        throw common::ErrnoException(
                  "FindFirstFile failed:" + std::to_string(GetLastError()));
    }

    if (!do_rmdir) return;

    if (!RemoveDirectory(tmp_dir.c_str())) {
        throw common::ErrnoException(
                  "Could not remove temporary directory " + tmp_dir);
    }
}

#else

std::string TemporaryDirectory::make_directory(const char* sample) {

    std::string tmp_dir = std::string(sample) + "XXXXXX";
    // evil const_cast, but mkdtemp replaces the XXXXXX with something
    // unique. it also mkdirs.
    char* p = mkdtemp(const_cast<char*>(tmp_dir.c_str()));

    if (p == nullptr) {
        throw common::ErrnoException(
                  "Could create temporary directory " + tmp_dir);
    }

    return tmp_dir;
}

void TemporaryDirectory::wipe_directory(
    const std::string& tmp_dir, bool do_rmdir) {
    DIR* d = opendir(tmp_dir.c_str());
    if (d == nullptr) {
        throw common::ErrnoException(
                  "Could open temporary directory " + tmp_dir);
    }

    struct dirent* de, entry;
    while (readdir_r(d, &entry, &de) == 0 && de != nullptr) {
        // skip ".", "..", and also hidden files (don't create them).
        if (de->d_name[0] == '.') continue;

        std::string path = tmp_dir + "/" + de->d_name;
        int r = unlink(path.c_str());
        if (r != 0)
            sLOG1 << "Could not unlink temporary file" << path
                  << ":" << strerror(errno);
    }

    closedir(d);

    if (!do_rmdir) return;

    if (rmdir(tmp_dir.c_str()) != 0) {
        sLOG1 << "Could not unlink temporary directory" << tmp_dir
              << ":" << strerror(errno);
    }
}

#endif

} // namespace core
} // namespace thrill

/******************************************************************************/
