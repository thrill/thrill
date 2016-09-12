
/*******************************************************************************
 * misc/aws/config_writer.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/platform/Platform.h>
#include <aws/core/platform/Time.h>
#include <aws/s3/S3Client.h>
#include <aws/core/config/AWSProfileConfigLoader.h>

#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>

#include <iostream>

int main(int argc, char* argv[])
{
    Aws::SDKOptions options;
    Aws::InitAPI(options);

    //!Config here (don't commit your secret key to github please :))
    auto region = Aws::Region::EU_WEST_1;
    Aws::String access_key_id("LULNO");
    Aws::String secret_key("MUCHSECRECY");

    Aws::Auth::AWSCredentials creds(access_key_id, secret_key);

    Aws::Config::Profile profile;
    profile.SetName("default");
    profile.SetCredentials(creds);
    profile.SetRegion(region);

    thrill::common::CmdlineParser clp;

    std::string output_path;
    clp.AddParamString("output", output_path,
                           "output file pattern");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    Aws::Map<Aws::String, Aws::Config::Profile> profiles;
    profiles["default"] = profile;
    Aws::Config::AWSConfigFileProfileConfigLoader writer(output_path, false);

    writer.PersistProfiles(profiles);

    LOG1 << "success, writing default profile";



    Aws::ShutdownAPI(options);
    return 0;

}

/******************************************************************************/
