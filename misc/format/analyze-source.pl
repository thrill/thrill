#!/usr/bin/perl -w
################################################################################
# misc/format/analyze-source.pl
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2014-2015 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

# print multiple email addresses
my $email_multimap = 0;

# launch emacsen for each error
my $launch_emacs = 0;

# write changes to files (dangerous!)
my $write_changes = 0;

# have autopep8 python formatter?
my $have_autopep8;

# function testing whether to uncrustify a path
sub filter_uncrustify($) {
    my ($path) = @_;

    return 1;
}

use strict;
use warnings;
use Text::Diff;
use File::stat;
use List::Util qw(min);

my %include_list;
my %include_map;
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

sub process_inline_perl {
    my @data = @_;

    for(my $i = 0; $i < @data; ++$i)
    {
        if ($data[$i] =~ /\[\[\[perl/) {
            # try to find the following ]]] line
            my $j = $i + 1;
            while ($j < @data && $data[$j] !~ /\]\]\]/) {
                $j++;
            }

            # no ]]] found.
            die("[[[perl is missing closing ]]]") if $j >= @data;

            # extract lines
            my $prog = join("", @data[$i+1..$j-1]);

            # evaluate the program
            my $output;
            {
                # return STDOUT to $output
                open local(*STDOUT), '>', \$output or die $!;
                eval $prog;
            }
            if ($@) {
                print "Perl inline: -------------\n";
                print "$prog\n";
                print "--------------------------\n";
                die("failed with $@");
            }
            #print "output: ".$output."\n";

            # try to find the following [[[end]]] line
            my $k = $j + 1;
            while ($k < @data && $data[$k] !~ /\[\[\[end\]\]\]/ && $data[$k] !~ /\[\[\[perl/) {
                $k++;
            }

            # not found: insert [[[end]]]
            if ($data[$k] !~ /\[\[\[end\]\]\]/) {
                $k = $j + 1;
                $output .= "\n// [[[end]]]";
            }

            my @output = split(/\n/, $output);
            $output[$_] .= "\n" foreach (0..@output-1);

            splice(@data, $j + 1, $k - ($j + 1), @output);
        }
    }

    return @data;
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

    # first check whether there are [[[perl lines and execute them
    @data = process_inline_perl(@data);

    # put all #include lines into the includelist+map
    for my $i (0...@data-1)
    {
        if ($data[$i] =~ m!\s*#\s*include\s*[<"](\S+)[">]!) {
            $include_list{$1} = 1;
            $include_map{$path}{$1} = 1;

            if ($1 eq "<thrill/thrill.hpp>" && $path !~ /\.dox$/) {
                print "\n";
                print "$path:$i: NEVER NEVER NEVER use thrill/thrill.hpp in our sources.\n";
                print "\n";
            }
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
        expect_re($path, $i, @data, '^ \*( .*|$)$');
        if (++$i >= @data) {
            $i = $j; # restore read position
            last;
        }
    }

    # check "Part of Project Thrill"
    expect($path, $i-1, @data, " *\n");
    expectr($path, $i, @data,
            " * Part of Project Thrill - http://project-thrill.org\n",
            qr/^ \* Part of Project Thrill/); ++$i;
    expect($path, $i, @data, " *\n"); ++$i;

    # read authors
    while ($data[$i] =~ /^ \* Copyright \(C\) ([0-9-]+(, [0-9-]+)*) (?<name>[^0-9<]+)( <(?<mail>[^>]+)>)?\n/) {
        #print "Author: $+{name} - $+{mail}\n";
        $authormap{$+{name}}{$+{mail} || ""} = 1;
        die unless ++$i < @data;
    }

    # otherwise check license
    expect($path, $i, @data, " *\n"); ++$i;
    expectr($path, $i, @data, " * All rights reserved. Published under the BSD-2 license in the LICENSE file.\n", qr/^ \*/); ++$i;
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

        # check for double underscore identifiers
        if ($data[$i] =~ m@\s__(?!(attribute__|sync_|builtin_|has_feature|FILE__|LINE__|FUNCTION__|PRETTY_FUNCTION__|GNUC__|linux__|APPLE__|FreeBSD__|clang__|STDC_WANT_SECURE_LIB__))@) {
            print("double underscore identifier found in $path:$i\n");
            print("$data[$i]\n");
        }

        # check for single underscore + uppercase identifiers
        if ($data[$i] =~ m@\s_(?!(GNU_SOURCE|WIN32|MSC_VER|UNICODE|DEBUG|ASSERTE|S_))[A-Z]@) {
            print("underscore + uppercase identifier found in $path:$i\n");
            print("$data[$i]\n");
        }

        if ($path !~ m!^frontends/swig_!) {
            # check for typedef, issue warnings.
            #if ($data[$i] =~ m/\btypedef\b/) {
            if ($data[$i] =~ s/\btypedef\b(.+)\b(\w+);$/using $2 = $1;/) {
                print("found typedef in $path:$i\n");
            }
        }
    }

    # run uncrustify if in filter
    if (filter_uncrustify($path))
    {
        my $data = join("", @data);
        @data = filter_program($data, "uncrustify", "-q", "-c", "misc/format/uncrustify.cfg", "-l", "CPP");

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
    if ($path =~ /\.(pl|sh)$/) {
        my $st = stat($path) or die("Cannot stat() file $path: $!");
        if (($st->mode & 0777) != 0755) {
            print("Wrong mode ".sprintf("%o", $st->mode)." on $path\n");
            if ($write_changes) {
                chmod(0755, $path) or die("Cannot chmod() file $path: $!");
            }
        }
    }
    else {
        my $st = stat($path) or die("Cannot stat() file $path: $!");
        if (($st->mode & 0777) != 0644) {
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

    # skip over custom file comments
    my $j = $i;
    while ($data[$i] !~ /^# Part of Project Thrill/) {
        expect_re($path, $i, @data, '^#( .*|$)');
        if (++$i >= @data) {
            $i = $j; # restore read position
            last;
        }
    }

    # check "Part of Project Thrill"
    expect($path, $i-1, @data, "#\n");
    expectr($path, $i, @data,
            "# Part of Project Thrill - http://project-thrill.org\n",
            qr/^# Part of Project Thrill/); ++$i;
    expect($path, $i, @data, "#\n"); ++$i;

    # read authors
    while ($data[$i] =~ /^# Copyright \(C\) ([0-9-]+(, [0-9-]+)*) (?<name>[^0-9<]+)( <(?<mail>[^>]+)>)?\n/) {
        #print "Author: $+{name} - $+{mail}\n";
        $authormap{$+{name}}{$+{mail} || ""} = 1;
        die unless ++$i < @data;
    }

    # otherwise check license
    expect($path, $i, @data, "#\n"); ++$i;
    expectr($path, $i, @data, "# All rights reserved. Published under the BSD-2 license in the LICENSE file.\n", qr/^# /); ++$i;

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
    expect_re($path, $i, @data, "^#!/usr/bin/env python"); ++$i;
    expect($path, $i, @data, ('#'x74)."\n"); ++$i;
    expectr($path, $i, @data, "# $path\n", qr/^# /); ++$i;
    expect($path, $i, @data, "#\n"); ++$i;

    # skip over custom file comments
    my $j = $i;
    while ($data[$i] !~ /^# Part of Project Thrill/) {
        expect_re($path, $i, @data, '^#( .*|$)');
        if (++$i >= @data) {
            $i = $j; # restore read position
            last;
        }
    }

    # check "Part of Project Thrill"
    expect($path, $i-1, @data, "#\n");
    expectr($path, $i, @data,
            "# Part of Project Thrill - http://project-thrill.org\n",
            qr/^# Part of Project Thrill/); ++$i;
    expect($path, $i, @data, "#\n"); ++$i;

    # read authors
    while ($data[$i] =~ /^# Copyright \(C\) ([0-9-]+(, [0-9-]+)*) (?<name>[^0-9<]+)( <(?<mail>[^>]+)>)?\n/) {
        #print "Author: $+{name} - $+{mail}\n";
        $authormap{$+{name}}{$+{mail} || ""} = 1;
        die unless ++$i < @data;
    }

    # otherwise check license
    expect($path, $i, @data, "#\n"); ++$i;
    expectr($path, $i, @data, "# All rights reserved. Published under the BSD-2 license in the LICENSE file.\n", qr/^# /); ++$i;

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

    # first check whether there are [[[perl lines and execute them
    @data = process_inline_perl(@data);

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
    expectr($path, $i, @data,
            " * Part of Project Thrill - http://project-thrill.org\n",
            qr/^ \* Part of Project Thrill/); ++$i;
    expect($path, $i, @data, " *\n"); ++$i;

    # read authors
    while ($data[$i] =~ /^ \* Copyright \(C\) ([0-9-]+(, [0-9-]+)*) (?<name>[^0-9<]+)( <(?<mail>[^>]+)>)?\n/) {
        #print "Author: $+{name} - $+{mail}\n";
        $authormap{$+{name}}{$+{mail} || ""} = 1;
        die unless ++$i < @data;
    }

    # otherwise check license
    expect($path, $i, @data, " *\n"); ++$i;
    expectr($path, $i, @data, " * All rights reserved. Published under the BSD-2 license in the LICENSE file.\n", qr/^ \*/); ++$i;
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

sub process_doc_images_pdf {
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

    my $svg_path = $path;
    $svg_path =~ s/\.pdf$/\.svg/;

    my $svg_st = stat($svg_path);

    if (!$svg_st || $svg_st->mtime < $st->mtime) {
        print("$path is newer than the SVG $svg_path.\n");

        if ($write_changes) {
            print("running pdf2svg $path $svg_path\n");
            system("pdf2svg", $path, $svg_path) == 0 or die("pdf2svg failed: $!");
        }
    }
}

### Main ###

$ENV{PYTHONDONTWRITEBYTECODE} = "1";

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
($uncrustver eq "uncrustify 0.62\n")
    or die("Requires uncrustify 0.62 to run correctly. Got: $uncrustver");

$have_autopep8 = 0;
my ($check_autopep8) = filter_program("", "autopep8", "--version");
if (!$check_autopep8 || $check_autopep8 !~ /^autopep8/) {
    $have_autopep8 = 0;
    warn("Could not find autopep8 - automatic python formatter.");
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
    elsif ($file =~ /\.p[lm]$/) {
        process_pl_cmake($file);
    }
    elsif ($file =~ /\.py$/) {
        process_py($file);
    }
    elsif ($file =~ /\.(sh|awk)$/) {
        process_pl_cmake($file);
    }
    elsif ($file =~ m!(^|/)CMakeLists\.txt$!) {
        process_pl_cmake($file);
    }
    elsif ($file =~ /\.(i)$/) {
        process_swig($file);
    }
    elsif ($file =~ /^doc\/images\/.*\.pdf$/) {
        # use pdf2svg to convert pdfs to svgs for doxygen.
        process_doc_images_pdf($file);
    }
    elsif ($file =~ /^doc\/images\/.*\.svg$/) {
    }
    # recognize further files
    elsif ($file =~ m!^\.git/!) {
    }
    elsif ($file =~ m!^misc/!) {
    }
    elsif ($file =~ m!^run/.*\.(md|json)$!) {
    }
    elsif ($file =~ m!^tests/inputs/!) {
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

# check include_list for C-style headers
{

    my @cheaders = qw(assert.h ctype.h errno.h fenv.h float.h inttypes.h
                      limits.h locale.h math.h signal.h stdarg.h stddef.h
                      stdlib.h stdio.h string.h time.h);

    foreach my $ch (@cheaders)
    {
        next if !$include_list{$ch};
        print "Replace c-style header $ch in\n";
        print "    [".join(",", sort keys %{$include_list{$ch}}). "]\n";
    }
}

# print includes
if (0)
{
    print "Writing include_map:\n";
    foreach my $inc (sort keys %include_map)
    {
        print "$inc => ".scalar(keys %{$include_map{$inc}})." [";
        print join(",", sort keys %{$include_map{$inc}}). "]\n";
    }
}

# try to find cycles in includes
if (1)
{
    my %graph = %include_map;

    # Tarjan's Strongly Connected Components Algorithm
    # https://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm

    my $index = 0;
    my @S = [];
    my %vi; # vertex info

    sub strongconnect {
        my ($v) = @_;

        # Set the depth index for v to the smallest unused index
        $vi{$v}{index} = $index;
        $vi{$v}{lowlink} = $index++;
        push(@S, $v);
        $vi{$v}{onStack} = 1;

        # Consider successors of v
        foreach my $w (keys %{$graph{$v}}) {
            if (!defined $vi{$w}{index}) {
                # Successor w has not yet been visited; recurse on it
                strongconnect($w);
                $vi{$w}{lowlink} = min($vi{$v}{lowlink}, $vi{$w}{lowlink});
            }
            elsif ($vi{$w}{onStack}) {
                # Successor w is in stack S and hence in the current SCC
                $vi{$v}{lowlink} = min($vi{$v}{lowlink}, $vi{$w}{index})
            }
        }

        # If v is a root node, pop the stack and generate an SCC
        if ($vi{$v}{lowlink} == $vi{$v}{index}) {
            # start a new strongly connected component
            my @SCC;
            my $w;
            do {
                $w = pop(@S);
                $vi{$w}{onStack} = 0;
                # add w to current strongly connected component
                push(@SCC, $w);
            } while ($w ne $v);
            # output the current strongly connected component (only if it is not
            # a singleton)
            if (@SCC != 1) {
                print "-------------------------------------------------";
                print "Found cycle:\n";
                print "    $_\n" foreach @SCC;
                print "end cycle\n";
            }
        }
    }

    foreach my $v (keys %graph) {
        next if defined $vi{$v}{index};
        strongconnect($v);
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
