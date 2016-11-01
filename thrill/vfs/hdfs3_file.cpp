/*******************************************************************************
 * thrill/vfs/hdfs3_file.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/vfs/hdfs3_file.hpp>

#include <thrill/common/die.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/string.hpp>

#if THRILL_HAVE_LIBHDFS3
#include <hdfs/hdfs.h>
#endif

#include <algorithm>
#include <limits>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace thrill {
namespace vfs {

#if THRILL_HAVE_LIBHDFS3

// flag to output debug info from S3
static constexpr bool debug = false;

//! connection map to HDFS namenode
static std::unordered_map<std::string, hdfsFS> s_hdfs_map;

//! mutex to protect hdfs_map
static std::mutex s_hdfs_mutex;

/******************************************************************************/

void Hdfs3Initialize() {
    // nothing to do
}

void Hdfs3Deinitialize() {
    std::unique_lock<std::mutex> lock(s_hdfs_mutex);
    for (auto& hdfs : s_hdfs_map) {
        hdfsDisconnect(hdfs.second);
    }
    s_hdfs_map.clear();
}

/******************************************************************************/
// Helper Methods

hdfsFS Hdfs3FindConnection(const std::string& hostport) {
    std::unique_lock<std::mutex> lock(s_hdfs_mutex);

    auto it = s_hdfs_map.find(hostport);
    if (it != s_hdfs_map.end())
        return it->second;

    // split host:port
    std::vector<std::string> splitted = common::Split(hostport, ':', 2);
    uint16_t port;

    if (splitted.size() == 1) {
        port = 8020;
    }
    else {
        if (!common::from_str<uint16_t>(splitted[1], port))
            die("Could not parse port in host:port \"" << hostport << "\"");
    }

    // split user@host
    std::vector<std::string> user_split = common::Split(splitted[0], '@', 2);
    const char* host, * user;

    if (user_split.size() == 1) {
        host = user_split[0].c_str();
        user = nullptr;
    }
    else {
        user = user_split[0].c_str();
        host = user_split[1].c_str();
    }

    hdfsBuilder* builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(builder, host);
    hdfsBuilderSetNameNodePort(builder, port);
    if (user)
        hdfsBuilderSetUserName(builder, user);

    hdfsFS hdfs = hdfsBuilderConnect(builder);
    if (!hdfs)
        die("Could not connect to HDFS server \"" << hostport << "\""
            ": " << hdfsGetLastError());

    s_hdfs_map[hostport] = hdfs;
    return hdfs;
}

/******************************************************************************/
// List Directory Contents on HDFS

void Hdfs3Glob(const std::string& _path, const GlobType& gtype,
               FileList& filelist) {

    std::string path = _path;
    // crop off hdfs://
    die_unless(common::StartsWith(path, "hdfs://"));
    path = path.substr(7);

    // split uri into host/path
    std::vector<std::string> splitted = common::Split(path, '/', 2);

    hdfsFS fs = Hdfs3FindConnection(splitted[0]);
    std::string hosturi = "hdfs://" + splitted[0];

    // prepend root /
    splitted[1] = "/" + splitted[1];

    // list directory
    int num_entries = 0;
    hdfsFileInfo* list = hdfsListDirectory(
        fs, splitted[1].c_str(), &num_entries);

    if (!list) return;

    for (int i = 0; i < num_entries; ++i) {
        FileInfo fi;

        fi.path = list[i].mName;
        // remove leading slashes
        while (fi.path.size() >= 2 && fi.path[0] == '/' && fi.path[1] == '/')
            fi.path.erase(fi.path.begin(), fi.path.begin() + 1);
        // prepend host uri
        fi.path = hosturi + fi.path;

        if (list[i].mKind == kObjectKindFile) {
            if (gtype == GlobType::All || gtype == GlobType::File) {
                // strangely full file name globs return the file with a / at
                // the end.
                while (fi.path.back() == '/')
                    fi.path.resize(fi.path.size() - 1);
                fi.type = Type::File;
                fi.size = list[i].mSize;
                filelist.emplace_back(fi);
            }
        }
        else if (list[i].mKind == kObjectKindDirectory) {
            if (gtype == GlobType::All || gtype == GlobType::Directory) {
                fi.type = Type::Directory;
                fi.size = list[i].mSize;
                filelist.emplace_back(fi);
            }
        }
    }

    hdfsFreeFileInfo(list, num_entries);
}

/******************************************************************************/
// Stream Reading from HDFS

class Hdfs3ReadStream : public vfs::ReadStream
{
public:
    Hdfs3ReadStream(hdfsFS fs, hdfsFile file,
                    uint64_t start_byte, uint64_t /* byte_count */)
        : fs_(fs), file_(file) {

        int err = hdfsSeek(fs_, file_, start_byte);
        die_unless(err == 0);
    }

    ~Hdfs3ReadStream() {
        close();
    }

    ssize_t read(void* data, size_t size) final {
        return hdfsRead(fs_, file_, data, size);
    }

    void close() final {
        if (!file_) return;

        hdfsCloseFile(fs_, file_);
        file_ = nullptr;
    }

private:
    //! HDFS connection
    hdfsFS fs_;

    //! HDFS file handler
    hdfsFile file_;
};

ReadStreamPtr Hdfs3OpenReadStream(
    const std::string& _path, const common::Range& range) {

    std::string path = _path;
    // crop off hdfs://
    die_unless(common::StartsWith(path, "hdfs://"));
    path = path.substr(7);

    // split uri into host/path
    std::vector<std::string> splitted = common::Split(path, '/', 2);
    die_unless(splitted.size() == 2);

    // prepend root /
    splitted[1] = "/" + splitted[1];

    hdfsFS fs = Hdfs3FindConnection(splitted[0]);

    // construct file handler
    hdfsFile file = hdfsOpenFile(
        fs, splitted[1].c_str(), O_RDONLY, /* bufferSize */ 0,
        /* replication */ 0, /* blocksize */ 0);
    if (!file)
        die("Could not open HDFS file \"" << _path << "\": " << hdfsGetLastError());

    return common::MakeCounting<Hdfs3ReadStream>(
        fs, file, /* start_byte */ range.begin, /* byte_count */ range.size());
}

/******************************************************************************/
// Stream Writing to HDFS

class Hdfs3WriteStream : public vfs::WriteStream
{
public:
    Hdfs3WriteStream(hdfsFS fs, hdfsFile file)
        : fs_(fs), file_(file) { }

    ~Hdfs3WriteStream() {
        close();
    }

    ssize_t write(const void* data, size_t size) final {
        return hdfsWrite(fs_, file_, data, size);
    }

    void close() final {
        if (!file_) return;

        hdfsCloseFile(fs_, file_);
        file_ = nullptr;
    }

private:
    //! HDFS connection
    hdfsFS fs_;

    //! HDFS file handler
    hdfsFile file_;
};

WriteStreamPtr Hdfs3OpenWriteStream(const std::string& _path) {

    std::string path = _path;
    // crop off hdfs://
    die_unless(common::StartsWith(path, "hdfs://"));
    path = path.substr(7);

    // split uri into host/path
    std::vector<std::string> splitted = common::Split(path, '/', 2);

    // prepend root /
    splitted[1] = "/" + splitted[1];

    hdfsFS fs = Hdfs3FindConnection(splitted[0]);

    // construct file handler
    hdfsFile file = hdfsOpenFile(
        fs, splitted[1].c_str(), O_WRONLY, /* bufferSize */ 0,
        /* replication */ 0, /* blocksize */ 0);
    if (!file)
        die("Could not open HDFS file \"" << _path << "\": " << hdfsGetLastError());

    return common::MakeCounting<Hdfs3WriteStream>(fs, file);
}

#else   // !THRILL_HAVE_LIBHDFS3

void Hdfs3Initialize()
{ }

void Hdfs3Deinitialize()
{ }

void Hdfs3Glob(const std::string& /* path */, const GlobType& /* gtype */,
               FileList& /* filelist */) {
    die("hdfs:// is not available, because Thrill was built without libhdfs3.");
}

ReadStreamPtr Hdfs3OpenReadStream(
    const std::string& /* path */, const common::Range& /* range */) {
    die("hdfs:// is not available, because Thrill was built without libhdfs3.");
}

WriteStreamPtr Hdfs3OpenWriteStream(const std::string& /* path */) {
    die("hdfs:// is not available, because Thrill was built without libhdfs3.");
}

#endif  // !THRILL_HAVE_LIBHDFS3

} // namespace vfs
} // namespace thrill

/******************************************************************************/
