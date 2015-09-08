#!/usr/bin/env python
##########################################################################
# tests/net/group_test_gen.py
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
#
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program.  If not, see <http://www.gnu.org/licenses/>.
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


def generate_dispatcher_tests(testname, runner, dispatcher):

    lines = [line.rstrip('\n')
             for line in open('tests/net/group_test_base.hpp')]

    p2 = re.compile('^static void DispatcherTest([A-Za-z0-9]+)\(')

    for ln in lines:
        m2 = p2.match(ln)
        if m2:
            func = m2.group(1)
            cog.outl("TEST(%s, Dispatcher%s) {" % (testname, func))
            cog.outl("    %s(" % (runner))
            cog.outl("        DispatcherTest%s<%s>);" % (func, dispatcher))
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
