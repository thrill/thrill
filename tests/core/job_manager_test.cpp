/*******************************************************************************
 * tests/core/job_manager_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/core/job_manager.hpp>
#include <c7a/api/bootstrap.hpp>
#include <gtest/gtest.h>

using namespace c7a;

static const bool debug = true;

TEST(JobManager, ConstructMockAndTearDown) {
    api::ExecuteLocalMock(
        4, 1, [](core::JobManager&, size_t node_id) {
            sLOG << "node_id" << node_id;
        });
}

/******************************************************************************/
