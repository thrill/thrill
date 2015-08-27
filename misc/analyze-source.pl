#!/usr/bin/perl -w
################################################################################
# misc/analyze-source.pl
#
# Copyright (C) 2014-2015 Timo Bingmann <tb@panthema.net>
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
################################################################################

# print multiple email addresses
my $email_multimap = 0;

# launch emacsen for each error
my $launch_emacs = 0;

# write changes to files (dangerous!)
my $write_changes = 0;

# have autopep8 python formatter?
my $have_autopep8;

# have cogapp code generator?
my $have_cogapp;

# function testing whether to uncrustify a path
sub filter_uncrustify($) {
    my ($path) = @_;

    return 1;
}

use strict;
use warnings;
use Text::Diff;
use File::stat;

my %includemap;
my %authormap;

my @source_filelist;

sub expect_error($$$$) {
    my ($path,$ln,$str,$expect) = @_;

    print("Bad header line $ln in $path\n");
    print("Expected $expect\n");
    print("Got      $str\n");

    system("emacsclient -n $path") if $launch_emacs;
}

sub expect($$\@$) {
    my ($path,$ln,$data,$expect) = @_;

    if ($$data[$ln] ne $expect) {
        expect_error($path,$ln,$$data[$ln],$expect);
        # insert line with expected value
        splice(@$data, $ln, 0, $expect);
    }
}

sub expectr($$\@$$) {
    my ($path,$ln,$data,$expect,$replace_regex) = @_;

    if ($$data[$ln] ne $expect) {
        expect_error($path,$ln,$$data[$ln],$expect);
        # replace line with expected value if like regex
        if ($$data[$ln] =~ m/$replace_regex/) {
            $$data[$ln] = $expect;
        } else {
            splice(@$data, $ln, 0, $expect);
        }
    }
}

sub expect_re($$\@$) {
    my ($path,$ln,$data,$expect) = @_;

    if ($$data[$ln] !~ m/$expect/) {
        expect_error($path, $ln, $$data[$ln], "/$expect/");
    }
}

sub removeif($$\@$) {
    my ($path,$ln,$data,$expect) = @_;

    if ($$data[$ln] eq $expect) {
        # insert line with expected value
        splice(@$data, $ln, 1);
    }
}

# check equality of two arrays
sub array_equal {
    my ($a1ref,$a2ref) = @_;

    my @a1 = @{$a1ref};
    my @a2 = @{$a2ref};

    return 0 if scalar(@a1) != scalar(@a2);

    for my $i (0..scalar(@a1)-1) {
        return 0 if $a1[$i] ne $a2[$i];
    }

    return 1;
}

# run $text through a external pipe (@program)
sub filter_program {
    my $text = shift;
    my @program = @_;

    # fork and read output
    my $child1 = open(my $fh, "-|") // die("$0: fork: $!");
    if ($child1 == 0) {
        # fork and print text
        my $child2 = open(STDIN, "-|") // die("$0: fork: $!");
        if ($child2 == 0) {
            print $text;
            exit;
        }
        else {
            exec(@program) or die("$0: exec: $!");
        }
    }
    else {
        my @output = <$fh>;
        close($fh) or warn("$0: close: $!");
        return @output;
    }
}

sub process_cpp {
    my ($path) = @_;

    #print "$path\n";

    # check permissions
    my $st = stat($path) or die("Cannot stat() file $path: $!");
    if ($st->mode & 0133) {
        print("Wrong mode ".sprintf("%o", $st->mode)." on $path\n");
        if ($write_changes) {
            chmod(0644, $path) or die("Cannot chmod() file $path: $!");
        }
    }

    # read file
    open(F, $path) or die("Cannot read file $path: $!");
    my @data = <F>;
    close(F);

    unless ($path =~ /\.dox$/) {
        push(@source_filelist, $path);
    }

    my @origdata = @data;

    # first check whether there are cog lines and execute them
    if ($have_cogapp) {
        my $have_coglines = 0;
        foreach my $ln (@data) {
            if ($ln =~ /\[\[\[cog/) {
                $have_coglines = 1;
                last;
            }
        }

        if ($have_coglines) {
            # pipe file through cog.py
            my $data = join("", @data);
            @data = filter_program($data, "cog.py", "-");
        }
    }

    # put all #include lines into the includemap
    foreach my $ln (@data)
    {
        if ($ln =~ m!\s*#\s*include\s*([<"]\S+[">])!) {
            $includemap{$1}{$path} = 1;
        }
    }

    # check source header
    my $i = 0;
    if ($data[$i] =~ m!// -.*- mode:!) { ++$i; } # skip emacs mode line

    expect($path, $i, @data, "/".('*'x79)."\n"); ++$i;
    expectr($path, $i, @data, " * $path\n", qr/^ \* /); ++$i;
    expect($path, $i, @data, " *\n"); ++$i;

    # skip over custom file comments
    my $j = $i;
    while ($data[$i] !~ /^ \* Part of Project Thrill/) {
        expect_re($path, $i, @data, '^ \*( .*)?\n$');
        if (++$i >= @data) {
            $i = $j; # restore read position
            last;
        }
    }

    # check "Part of Project Thrill"
    expect($path, $i-1, @data, " *\n");
    expect($path, $i, @data, " * Part of Project Thrill.\n"); ++$i;
    expect($path, $i, @data, " *\n"); ++$i;

    # read authors
    while ($data[$i] =~ /^ \* Copyright \(C\) ([0-9-]+(, [0-9-]+)*) (?<name>[^0-9<]+)( <(?<mail>[^>]+)>)?\n/) {
        #print "Author: $+{name} - $+{mail}\n";
        $authormap{$+{name}}{$+{mail} || ""} = 1;
        die unless ++$i < @data;
    }

    # otherwise check license
    expect($path, $i, @data, " *\n"); ++$i;
    expectr($path, $i, @data, " * This file has no license. Only Chunk Norris can compile it.\n", qr/^ \*/); ++$i;
    expect($path, $i, @data, " ".('*'x78)."/\n"); ++$i;

    # check include guard name
    if ($path =~ m!\.(h|h.in|hpp)$!)
    {
        expect($path, $i, @data, "\n"); ++$i;

        # construct include guard macro name: PROGRAM_FILE_NAME_HEADER
        my $guard = $path;
        $guard =~ s!thrill/!!;
        $guard =~ tr!/-!__!;
        $guard =~ s!\.h(pp)?(\.in)?$!!;
        $guard = "THRILL_".uc($guard)."_HEADER";
        #print $guard."\n";x

        expect($path, $i, @data, "#pragma once\n"); ++$i;
        expectr($path, $i, @data, "#ifndef $guard\n", qr/^#ifndef /); ++$i;
        expectr($path, $i, @data, "#define $guard\n", qr/^#define /); ++$i;
        removeif($path, $i, @data, "#pragma once\n");

        my $n = scalar(@data)-1;
        expectr($path, $n-2, @data, "#endif // !$guard\n", qr/^#endif /);
    }

    # check for comments that have no space after // or //!
    for(my $i = 0; $i < @data-1; ++$i) {
        next if $data[$i] =~ m@^(\s*//)( |! |/|-|\*)@;
        $data[$i] =~ s@^(\s*//!?)([^ ].+)$@$1 $2@;
    }

    # check terminating /****/ comment
    {
        my $n = scalar(@data)-1;
        if ($data[$n] !~ m!^/\*{78}/$!) {
            push(@data, "\n");
            push(@data, "/".('*'x78)."/\n");
        }
    }

    for(my $i = 0; $i < @data-1; ++$i) {
        # check for @-style doxygen commands
        if ($data[$i] =~ m!\@(param|tparam|return|result|brief|details|c|i)\s!) {
            print("found \@-style doxygen command in $path:$i\n");
        }

        # check for assert() in test cases
        if ($path =~ /^tests/) {
            if ($data[$i] =~ m/(?<!static_)assert\(/) {
                print("found assert() in test $path:$i\n");
            }
        }

        # check for NULL -> replace with nullptr.
        if ($data[$i] =~ s/\bNULL\b/nullptr/g) {
            print("replacing NULL in $path:$i\n");
        }

        # check for typedef, issue warnings.
        if ($data[$i] =~ m/\btypedef\b/) {
        #if ($data[$i] =~ s/\btypedef\b(.+)\b(\w+);$/using $2 = $1;/) {
            print("found typedef in $path:$i\n");
        }
    }

    # run uncrustify if in filter
    if (filter_uncrustify($path))
    {
        my $data = join("", @data);
        @data = filter_program($data, "uncrustify", "-q", "-c", "misc/uncrustify.cfg", "-l", "CPP");

        # manually add blank line after "namespace xyz {" and before "} // namespace xyz"
        my @namespace;
        for(my $i = 0; $i < @data-1; ++$i)
        {
            if ($data[$i] =~ m!^namespace (\S+) {!) {
                push(@namespace, $1);
            }
            elsif ($data[$i] =~ m!^namespace {!) {
                push(@namespace, "");
            }
            elsif ($data[$i] =~ m!^}\s+//\s+namespace\s+(\S+)\s*$!) {
                if (@namespace == 0) {
                    print "$path\n";
                    print "    NAMESPACE UNBALANCED! @ $i\n";
                    #system("emacsclient -n $path");
                }
                else {
                    # quiets wrong uncrustify indentation
                    $data[$i] =~ s!}\s+//\s+namespace!} // namespace!;
                    expectr($path, $i, @data, "} // namespace ".$namespace[-1]."\n",
                            qr!^}\s+//\s+namespace!);
                }
                if ($data[$i-1] !~ m!^}\s+// namespace!) {
                    splice(@data, $i, 0, "\n"); ++$i;
                }
                pop(@namespace);
            }
            elsif ($data[$i] =~ m!^}\s+//\s+namespace\s*$!) {
                if (@namespace == 0) {
                    print "$path\n";
                    print "    NAMESPACE UNBALANCED! @ $i\n";
                    #system("emacsclient -n $path");
                }
                else {
                    # quiets wrong uncrustify indentation
                    $data[$i] =~ s!}\s+//\s+namespace!} // namespace!;
                    expectr($path, $i, @data, "} // namespace\n",
                            qr!^}\s+//\s+namespace!);
                }
                if ($data[$i-1] !~ m!^}\s+// namespace!) {
                    splice(@data, $i, 0, "\n"); ++$i;
                }
                pop(@namespace);
            }
        }
        if (@namespace != 0) {
            print "$path\n";
            print "    NAMESPACE UNBALANCED!\n";
            #system("emacsclient -n $path");
        }

        # change misformatted [ = ] of lambdas to [=]
        for(my $i = 0; $i < @data-1; ++$i) {
            $data[$i] =~ s/\[ = \]/[=]/g;
            $data[$i] =~ s/\[ ([=&]),/[$1,/g;
        }
    }

    return if array_equal(\@data, \@origdata);

    print "$path\n";
    print diff(\@origdata, \@data);
    #system("emacsclient -n $path");

    if ($write_changes)
    {
        open(F, "> $path") or die("Cannot write $path: $!");
        print(F join("", @data));
        close(F);
    }
}

sub process_pl_cmake {
    my ($path) = @_;

    # check permissions
    if ($path !~ /\.pl$/) {
        my $st = stat($path) or die("Cannot stat() file $path: $!");
        if ($st->mode & 0133) {
            print("Wrong mode ".sprintf("%o", $st->mode)." on $path\n");
            if ($write_changes) {
                chmod(0644, $path) or die("Cannot chmod() file $path: $!");
            }
        }
    }

    # read file
    open(F, $path) or die("Cannot read file $path: $!");
    my @data = <F>;
    close(F);

    my @origdata = @data;

    # check source header
    my $i = 0;
    if ($data[$i] =~ m/#!/) { ++$i; } # bash line
    expect($path, $i, @data, ('#'x80)."\n"); ++$i;
    expectr($path, $i, @data, "# $path\n", qr/^# /); ++$i;
    expect($path, $i, @data, "#\n"); ++$i;

    # skip over comment
    while ($data[$i] ne ('#'x80)."\n") {
        expect_re($path, $i, @data, '^#( .*)?\n$');
        return unless ++$i < @data;
    }

    expect($path, $i, @data, ('#'x80)."\n"); ++$i;

    # check terminating ####### comment
    {
        my $n = scalar(@data)-1;
        if ($data[$n] !~ m!^#{80}$!) {
            push(@data, "\n");
            push(@data, ("#"x80)."\n");
        }
    }

    return if array_equal(\@data, \@origdata);

    print "$path\n";
    print diff(\@origdata, \@data);
    #system("emacsclient -n $path");

    if ($write_changes)
    {
        open(F, "> $path") or die("Cannot write $path: $!");
        print(F join("", @data));
        close(F);
    }
}

sub process_py {
    my ($path) = @_;

    # read file
    open(F, $path) or die("Cannot read file $path: $!");
    my @data = <F>;
    close(F);

    my @origdata = @data;

    # check source header
    my $i = 0;
    expect($path, $i, @data, "#!/usr/bin/env python\n"); ++$i;
    expect($path, $i, @data, ('#'x74)."\n"); ++$i;
    expectr($path, $i, @data, "# $path\n", qr/^# /); ++$i;
    expect($path, $i, @data, "#\n"); ++$i;

    # skip over comment
    while ($data[$i] ne ('#'x74)."\n") {
        expect_re($path, $i, @data, '^#( .*)?\n$');
        return unless ++$i < @data;
    }

    expect($path, $i, @data, ('#'x74)."\n"); ++$i;

    # check terminating ####### comment
    {
        my $n = scalar(@data)-1;
        if ($data[$n] !~ m!^#{74}$!) {
            push(@data, "\n");
            push(@data, ("#"x74)."\n");
        }
    }

    # run python source through autopep8
    if ($have_autopep8)
    {
        my $data = join("", @data);
        @data = filter_program($data, "autopep8", "-");
    }

    return if array_equal(\@data, \@origdata);

    print "$path\n";
    print diff(\@origdata, \@data);
    #system("emacsclient -n $path");

    if ($write_changes)
    {
        open(F, "> $path") or die("Cannot write $path: $!");
        print(F join("", @data));
        close(F);
    }
}

sub process_swig {
    my ($path) = @_;

    #print "$path\n";

    # check permissions
    my $st = stat($path) or die("Cannot stat() file $path: $!");
    if ($st->mode & 0133) {
        print("Wrong mode ".sprintf("%o", $st->mode)." on $path\n");
        if ($write_changes) {
            chmod(0644, $path) or die("Cannot chmod() file $path: $!");
        }
    }

    # read file
    open(F, $path) or die("Cannot read file $path: $!");
    my @data = <F>;
    close(F);

    my @origdata = @data;

    # first check whether there are cog lines and execute them
    if ($have_cogapp) {
        my $have_coglines = 0;
        foreach my $ln (@data) {
            if ($ln =~ /\[\[\[cog/) {
                $have_coglines = 1;
                last;
            }
        }

        if ($have_coglines) {
            # pipe file through cog.py
            my $data = join("", @data);
            @data = filter_program($data, "cog.py", "-");
        }
    }

    # check source header
    my $i = 0;
    if ($data[$i] =~ m!// -.*- mode:!) { ++$i; } # skip emacs mode line

    expect($path, $i, @data, "/".('*'x79)."\n"); ++$i;
    expectr($path, $i, @data, " * $path\n", qr/^ \* /); ++$i;
    expect($path, $i, @data, " *\n"); ++$i;

    # skip over custom file comments
    my $j = $i;
    while ($data[$i] !~ /^ \* Part of Project Thrill/) {
        expect_re($path, $i, @data, '^ \*( .*)?\n$');
        if (++$i >= @data) {
            $i = $j; # restore read position
            last;
        }
    }

    # check "Part of Project Thrill"
    expect($path, $i-1, @data, " *\n");
    expect($path, $i, @data, " * Part of Project Thrill.\n"); ++$i;
    expect($path, $i, @data, " *\n"); ++$i;

    # read authors
    while ($data[$i] =~ /^ \* Copyright \(C\) ([0-9-]+(, [0-9-]+)*) (?<name>[^0-9<]+)( <(?<mail>[^>]+)>)?\n/) {
        #print "Author: $+{name} - $+{mail}\n";
        $authormap{$+{name}}{$+{mail} || ""} = 1;
        die unless ++$i < @data;
    }

    # otherwise check license
    expect($path, $i, @data, " *\n"); ++$i;
    expectr($path, $i, @data, " * This file has no license. Only Chunk Norris can compile it.\n", qr/^ \*/); ++$i;
    expect($path, $i, @data, " ".('*'x78)."/\n"); ++$i;

    # check terminating /****/ comment
    {
        my $n = scalar(@data)-1;
        if ($data[$n] !~ m!^/\*{78}/$!) {
            push(@data, "\n");
            push(@data, "/".('*'x78)."/\n");
        }
    }

    return if array_equal(\@data, \@origdata);

    print "$path\n";
    print diff(\@origdata, \@data);
    #system("emacsclient -n $path");

    if ($write_changes)
    {
        open(F, "> $path") or die("Cannot write $path: $!");
        print(F join("", @data));
        close(F);
    }
}

### Main ###

foreach my $arg (@ARGV) {
    if ($arg eq "-w") { $write_changes = 1; }
    elsif ($arg eq "-e") { $launch_emacs = 1; }
    elsif ($arg eq "-m") { $email_multimap = 1; }
    else {
        print "Unknown parameter: $arg\n";
    }
}

(-e "thrill/CMakeLists.txt")
    or die("Please run this script in the Thrill source base directory.");

# check uncrustify's version:
my ($uncrustver) = filter_program("", "uncrustify", "--version");
($uncrustver eq "uncrustify 0.61\n")
    or die("Requires uncrustify 0.61 to run correctly. ".
           "See https://github.com/PdF14-MR/thrill/wiki/Uncrustify-as-local-pre-commit-hook");

$have_autopep8 = 1;
my ($check_autopep8) = filter_program("", "autopep8", "--version");
if (!$check_autopep8 || $check_autopep8 !~ /^autopep8/) {
    $have_autopep8 = 0;
    warn("Could not find autopep8 - automatic python formatter.");
}

$have_cogapp = 1;
my ($check_cogapp) = filter_program("", "cog.py", "-version");
if (!$check_cogapp || $check_cogapp !~ /^Cog/) {
    $have_cogapp = 0;
    warn("Could not find cogapp - python code generator formatter.");
}

use File::Find;
my @filelist;
find(sub { !-d && push(@filelist, $File::Find::name) }, ".");

foreach my $file (@filelist)
{
    $file =~ s!./!! or die("File does not start ./");

    if ($file =~ m!^(b[0-9]*|build.*)(\/|$)!) {
        # skip common build directory names
    }
    elsif ($file =~ m!^extlib/!) {
        # skip external libraries
    }
    elsif ($file =~ /\.(h|cpp|hpp|h\.in|dox)$/) {
        process_cpp($file);
    }
    elsif ($file =~ /\.pl$/) {
        process_pl_cmake($file);
    }
    elsif ($file =~ /^swig.*\.py$/) {
        process_py($file);
    }
    elsif ($file =~ m!(^|/)CMakeLists\.txt$!) {
        process_pl_cmake($file);
    }
    elsif ($file =~ /\.(i)$/) {
        process_swig($file);
    }
    # recognize further files
    elsif ($file =~ m!^\.git/!) {
    }
    elsif ($file =~ m!^misc/!) {
    }
    elsif ($file =~ m!CPPLINT\.cfg$!) {
    }
    elsif ($file =~ m!^doxygen-html/!) {
    }
    elsif ($file =~ m!^tests/.*\.(dat|plot)$!) {
        # data files of tests
    }
    # skip all additional files in source root
    elsif ($file =~ m!^[^/]+$!) {
    }
    else {
        print "Unknown file type $file\n";
    }
}

# print includes to includemap.txt
if (0)
{
    print "Writing includemap:\n";
    foreach my $inc (sort keys %includemap)
    {
        print "$inc => ".scalar(keys %{$includemap{$inc}})." [";
        print join(",", sort keys %{$includemap{$inc}}). "]\n";
    }
}

# print authors to AUTHORS
print "Writing AUTHORS:\n";
open(A, "> AUTHORS");
foreach my $a (sort keys %authormap)
{
    my $mail = $authormap{$a};
    if ($email_multimap) {
        $mail = join(",", sort keys %{$mail});
    }
    else {
        $mail = (sort keys(%{$mail}))[0]; # pick first
    }
    $mail = $mail ? " <$mail>" : "";

    print "  $a$mail\n";
    print A "$a$mail\n";
}
close(A);

# check includemap for C-style headers
{

    my @cheaders = qw(assert.h ctype.h errno.h fenv.h float.h inttypes.h
                      limits.h locale.h math.h signal.h stdarg.h stddef.h
                      stdlib.h stdio.h string.h time.h);

    foreach my $ch (@cheaders)
    {
        $ch = "<$ch>";
        next if !$includemap{$ch};
        print "Replace c-style header $ch in\n";
        print "    [".join(",", sort keys %{$includemap{$ch}}). "]\n";
    }
}

# run cpplint.py
{
    my @lintlist;

    foreach my $path (@source_filelist)
    {
        #next if $path =~ /exclude/;

        push(@lintlist, $path);
    }

    system("cpplint.py", "--counting=total", "--extensions=h,c,cc,hpp,cpp", @lintlist);
}

################################################################################
