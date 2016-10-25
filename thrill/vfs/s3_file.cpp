/*******************************************************************************
 * thrill/vfs/s3_file.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#if THRILL_USE_AWS
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#endif

#include <thrill/api/context.hpp>
#include <thrill/common/porting.hpp>
#include <thrill/common/string.hpp>
#include <thrill/common/system_exception.hpp>
#include <thrill/vfs/s3_file.hpp>

#include <algorithm>
#include <string>
#include <vector>

namespace thrill {
namespace vfs {

std::shared_ptr<S3File> S3OpenReadStream(
    const FileInfo& file, const api::Context& ctx,
    const common::Range& my_range, bool compressed) {

    static constexpr bool debug = false;

    LOG1 << "Opening file";

    // Amount of additional bytes read after end of range
    size_t maximum_line_length = 64 * 1024;

    Aws::S3::Model::GetObjectRequest getObjectRequest;

    std::string path_without_s3 = file.path.substr(5);

    std::vector<std::string> splitted = common::Split(
        path_without_s3, '/', (std::string::size_type)2);

    assert(splitted.size() == 2);

    getObjectRequest.SetBucket(splitted[0]);
    getObjectRequest.SetKey(splitted[1]);

    LOG << "Attempting to read from bucket " << splitted[0] << " with key "
        << splitted[1] << "!";

    size_t range_start = 0;
    if (!compressed) {
        std::string range = "bytes=";
        bool use_range_ = false;
        if (my_range.begin > file.size_ex_psum) {
            range += std::to_string(my_range.begin - file.size_ex_psum);
            range_start = my_range.begin - file.size_ex_psum;
            use_range_ = true;
        }
        else {
            range += "0";
        }

        range += "-";
        if (my_range.end + maximum_line_length < file.size_inc_psum()) {
            range += std::to_string(file.size - (file.size_inc_psum() -
                                                 my_range.end -
                                                 maximum_line_length));
            use_range_ = true;
        }

        if (use_range_)
            getObjectRequest.SetRange(range);
    }

    LOG1 << "Get...";
    auto outcome = ctx.s3_client()->GetObject(getObjectRequest);
    LOG1 << "...Got";

    if (!outcome.IsSuccess())
        throw common::ErrnoException(
                  "Download from S3 Errored: " + outcome.GetError().GetMessage());

    if (!compressed) {
        return std::make_shared<S3File>(outcome.GetResultWithOwnership(),
                                        range_start);
    }
    else {
        // this constructor opens a zip_stream
        return std::make_shared<S3File>(outcome.GetResultWithOwnership());
    }
}

std::shared_ptr<S3File> S3OpenWriteStream(
    const std::string& path, const api::Context& ctx) {
    return std::make_shared<S3File>(ctx.s3_client(), path);
}

} // namespace vfs
} // namespace thrill

/******************************************************************************/
