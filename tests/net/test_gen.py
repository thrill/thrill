#!/usr/bin/env python
##########################################################################
# tests/net/test_gen.py
#
# Part of Project Thrill.
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Available under the BSD-2 license in the LICENSE file.
##########################################################################

import cog
import re


def generate_group_tests(testname, runner):

    lines = [line.rstrip('\n')
             for line in open('tests/net/group_test_base.hpp')]

    p1 = re.compile('^static void Test([A-Za-z0-9]+)\(')

    for ln in lines:
        m1 = p1.match(ln)
        if m1:
            func = m1.group(1)
            cog.outl("TEST(%s, %s) {" % (testname, func))
            cog.outl("    %s(Test%s);" % (runner, func))
            cog.outl("}")


def generate_flow_control_tests(testname, runner):

    lines = [line.rstrip('\n')
             for line in open('tests/net/flow_control_test_base.hpp')]

    p1 = re.compile('^static void Test([A-Za-z0-9]+)\(')

    for ln in lines:
        m1 = p1.match(ln)
        if m1:
            func = m1.group(1)
            cog.outl("TEST(FlowControl%s, %s) {" % (testname, func))
            cog.outl("    %s(Test%s);" % (runner, func))
            cog.outl("}")


##########################################################################
