/*******************************************************************************
 * c7a/<file>
 *
 * Google Test main program which calls tests.
 ******************************************************************************/

#include <gtest/gtest.h>

std::string g_workpath;

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);

    if (argc <= 1) {
        std::cout << argv[0]
                  << " requires parameter: <test input files path>"
                  << std::endl;
    } else {
        g_workpath = argv[1];
    }

    return RUN_ALL_TESTS();
}

/******************************************************************************/
