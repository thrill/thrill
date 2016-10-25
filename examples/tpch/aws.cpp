/*******************************************************************************
 * examples/tpch/aws.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/config/AWSProfileConfigLoader.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/platform/Platform.h>
#include <aws/core/platform/Time.h>
#include <aws/core/utils/crypto/CryptoStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>

#include <thrill/common/logger.hpp>

#include <iostream>

int main() {
    std::string home = getenv("HOME");
    std::string profilepath = home + "/awsprofile";

    Aws::SDKOptions options;
    Aws::InitAPI(options);
    static const char* ALLOCATION_TAG = "Minimal_AWS_S3_Example";
    Aws::String TestFileName = "/home/anoe/testfile";

    // Create client configuration file
    Aws::Client::ClientConfiguration config;
    config.scheme = Aws::Http::Scheme::HTTPS;
    config.connectTimeoutMs = 30000;
    config.requestTimeoutMs = 30000;

    Aws::Config::AWSConfigFileProfileConfigLoader writer(profilepath, false);
    writer.Load();
    auto profile = writer.GetProfiles().at("default");

    config.region = profile.GetRegion();

    // create S3 client
    static std::shared_ptr<Aws::S3::S3Client> s3Client;

    Aws::Auth::AWSCredentials creds = profile.GetCredentials();

    s3Client = Aws::MakeShared<Aws::S3::S3Client>(ALLOCATION_TAG,
                                                  creds,
                                                  config);

    Aws::S3::Model::ListObjectsRequest listObjectsRequest;
    listObjectsRequest.SetBucket("thrill-data");
    listObjectsRequest.SetPrefix("tbl");

    Aws::S3::Model::GetObjectRequest getObjectRequest;
    getObjectRequest.SetBucket("thrill-data");
    getObjectRequest.SetKey("tbl/part.tbl");

    auto listObjectsOutcome = s3Client->ListObjects(listObjectsRequest);

    if (!listObjectsOutcome.IsSuccess()) {
        LOG1 << "listObjects.IsNoSuccess()";
        return -1;
    }

    for (const auto& object : listObjectsOutcome.GetResult().GetContents()) {
        std::cout << object.GetKey() << "," << object.GetSize() << ","
                  << object.GetOwner().GetDisplayName() << std::endl;
    }

    auto getObjectOutcome = s3Client->GetObject(getObjectRequest);

    if (!getObjectOutcome.IsSuccess()) {
        LOG1 << "getObject.IsNoSuccess()";
        return -1;
    }

    std::cout << getObjectOutcome.GetResult().GetBody().rdbuf();

    Aws::ShutdownAPI(options);
    return 0;
}

/******************************************************************************/
