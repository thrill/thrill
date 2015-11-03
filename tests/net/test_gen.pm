################################################################################
# tests/net/test_gen.pm
#
# Part of Project Thrill.
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

sub generate_group_tests {
    my ($group, $runner) = @_;

    open(F, "tests/net/group_test_base.hpp") or die;
    while (my $ln = <F>) {
        next unless $ln =~ /^static void Test(?<test>[A-Za-z0-9]+)\(/;
        print "TEST($group, $+{test}) {\n";
        print "    $runner(Test$+{test});\n";
        print "}\n";
    }
    close(F);
}

sub generate_flow_control_tests {
    my ($group, $runner) = @_;

    open(F, "tests/net/flow_control_test_base.hpp") or die;
    while (my $ln = <F>) {
        next unless $ln =~ /^static void Test(?<test>[A-Za-z0-9]+)\(/;
        print "TEST($group, $+{test}) {\n";
        print "    $runner(Test$+{test});\n";
        print "}\n";
    }
    close(F);
}

1;

################################################################################
