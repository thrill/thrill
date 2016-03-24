/*******************************************************************************
 * thrill/common/cmdline_parser.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_CMDLINE_PARSER_HEADER
#define THRILL_COMMON_CMDLINE_PARSER_HEADER

#include <iostream>
#include <string>
#include <vector>

namespace thrill {
namespace common {

/**

Command line parser which automatically fills variables and prints nice usage
messages.

This is a straightforward command line parser in C++, which will recognize short
options -s, long options --long and parameters, both required and optional. It
will automatically parse integers and <b>byte sizes</b> with SI/IEC suffixes
(e.g. 1 GiB). It also works with lists of strings, e.g. multiple filenames.

\snippet tests/common/cmdline_parser_example.cpp example

When running the program above without arguments, it will print:
\verbatim
$ ./common_cmdline_parser_example
Missing required argument for parameter 'filename'

Usage: ./common_cmdline_parser_example [options] <filename>

This may some day be a useful program, which solves many serious problems of
the real world and achives global peace.

Author: Timo Bingmann <tb@panthema.net>

Parameters:
  filename  A filename to process
Options:
  -r, --rounds N  Run N rounds of the experiment.
  -s, --size      Number of bytes to process.
\endverbatim

Nice output, notice the line wrapping of the description and formatting of
parameters and arguments. These too are wrapped if the description is too long.

We now try to give the program some arguments:
\verbatim
$ ./common_cmdline_parser_example -s 2GiB -r 42 /dev/null
Option -s, --size set to 2147483648.
Option -r, --rounds N set to 42.
Parameter filename set to "/dev/null".
Command line parsed okay.
Parameters:
  filename        (string)            "/dev/null"
Options:
  -r, --rounds N  (unsigned integer)  42
  -s, --size      (bytes)             2147483648
\endverbatim

The output shows pretty much what happens. The command line parser is by default
in a verbose mode outputting all arguments and values parsed. The debug summary
shows to have values the corresponding variables were set.

One feature worth naming is that the parser also supports lists of strings,
i.e. \c std::vector<std::string> via \ref CmdlineParser::AddParamStringlist()
and similar.

*/
class CmdlineParser
{
private:
    // forward declaration of Argument classes
    struct Argument;
    struct ArgumentFlag;
    struct ArgumentInt;
    struct ArgumentUInt;
    struct ArgumentSizeT;
    struct ArgumentDouble;
    struct ArgumentBytes32;
    struct ArgumentBytes64;
    struct ArgumentString;
    struct ArgumentStringlist;

private:
    //! option and parameter list type
    using ArgumentList = std::vector<Argument*>;

    //! list of options available
    ArgumentList optlist_;
    //! list of parameters, both required and optional
    ArgumentList paramlist_;

    //! formatting width for options, '-s, --switch <#>'
    int opt_maxlong_ = 8;
    //! formatting width for parameters, 'param <#>'
    int param_maxlong_ = 8;

    //! argv[0] for usage.
    const char* progname_ = nullptr;

    //! verbose processing of arguments
    bool verbose_process_ = false;

    //! user set description of program, will be wrapped
    std::string description_;
    //! user set author of program, will be wrapped
    std::string author_;

    //! set line wrap length
    unsigned int linewrap_ = 80;

    //! maximum length of a TypeName() result
    static constexpr int maxtypename_ = 16;

private:
    //! update maximum formatting width for new option
    void CalcOptMax(const Argument* arg);

    //! update maximum formatting width for new parameter
    void CalcParamMax(const Argument* arg);

public:
    //! Wrap a long string at spaces into lines. Prefix is added
    //! unconditionally to each line. Lines are wrapped after wraplen
    //! characters if possible.
    static void
    OutputWrap(std::ostream& os, const std::string& text, size_t wraplen,
               size_t indent_first = 0, size_t indent_rest = 0,
               size_t current = 0, size_t indent_newline = 0);

public:
    //! Delete all added arguments
    ~CmdlineParser();

    //! Set description of program, text will be wrapped
    void SetDescription(const std::string& description);

    //! Set author of program, will be wrapped.
    void SetAuthor(const std::string& author);

    //! Set verbose processing of command line arguments
    void SetVerboseProcess(bool verbose_process);

    // ************************************************************************

    //! add boolean option flag -key, --longkey [keytype] with description and
    //! store to dest
    void AddFlag(char key, const std::string& longkey,
                 const std::string& keytype, bool& dest,
                 const std::string& desc);

    //! add signed integer option -key, --longkey [keytype] with description
    //! and store to dest
    void AddInt(char key, const std::string& longkey,
                const std::string& keytype, int& dest,
                const std::string& desc);

    //! add unsigned integer option -key, --longkey [keytype] with description
    //! and store to dest
    void AddUInt(char key, const std::string& longkey,
                 const std::string& keytype, unsigned int& dest,
                 const std::string& desc);

    //! add size_t option -key, --longkey [keytype] with description and store
    //! to dest
    void AddSizeT(char key, const std::string& longkey,
                  const std::string& keytype, size_t& dest,
                  const std::string& desc);

    //! add double option -key, --longkey [keytype] with description and store
    //! to dest
    void AddDouble(char key, const std::string& longkey,
                   const std::string& keytype, double& dest,
                   const std::string& desc);

    //! add SI/IEC suffixes byte size option -key, --longkey [keytype] and
    //! store to 64-bit dest
    void AddBytes(char key, const std::string& longkey,
                  const std::string& keytype, uint32_t& dest,
                  const std::string& desc);

    //! add SI/IEC suffixes byte size option -key, --longkey [keytype] and
    //! store to 64-bit dest
    void AddBytes(char key, const std::string& longkey,
                  const std::string& keytype, uint64_t& dest,
                  const std::string& desc);

    //! add string option -key, --longkey [keytype] and store to dest
    void AddString(char key, const std::string& longkey,
                   const std::string& keytype, std::string& dest,
                   const std::string& desc);

    //! add string list option -key, --longkey [keytype] and store to dest
    void AddStringlist(char key, const std::string& longkey,
                       const std::string& keytype,
                       std::vector<std::string>& dest,
                       const std::string& desc);

    //! add boolean option flag -key, --longkey with description and store to
    //! dest
    void AddFlag(char key, const std::string& longkey, bool& dest,
                 const std::string& desc);

    //! add signed integer option -key, --longkey with description and store to
    //! dest
    void AddInt(char key, const std::string& longkey, int& dest,
                const std::string& desc);

    //! add unsigned integer option -key, --longkey [keytype] with description
    //! and store to dest
    void AddUInt(char key, const std::string& longkey, unsigned int& dest,
                 const std::string& desc);

    //! add size_t option -key, --longkey [keytype] with description and store
    //! to dest
    void AddSizeT(char key, const std::string& longkey, size_t& dest,
                  const std::string& desc);

    //! add double option -key, --longkey [keytype] with description and store
    //! to dest
    void AddDouble(char key, const std::string& longkey, double& dest,
                   const std::string& desc);

    //! add SI/IEC suffixes byte size option -key, --longkey [keytype] and
    //! store to 32-bit dest
    void AddBytes(char key, const std::string& longkey, uint32_t& dest,
                  const std::string& desc);

    //! add SI/IEC suffixes byte size option -key, --longkey [keytype] and
    //! store to 64-bit dest
    void AddBytes(char key, const std::string& longkey, uint64_t& dest,
                  const std::string& desc);

    //! add string option -key, --longkey [keytype] and store to dest
    void AddString(char key, const std::string& longkey, std::string& dest,
                   const std::string& desc);

    //! add string list option -key, --longkey [keytype] and store to dest
    void AddStringlist(char key, const std::string& longkey,
                       std::vector<std::string>& dest, const std::string& desc);

    // ************************************************************************

    //! add signed integer parameter [name] with description and store to dest
    void AddParamInt(const std::string& name, int& dest,
                     const std::string& desc);

    //! add unsigned integer parameter [name] with description and store to dest
    void AddParamUInt(const std::string& name, unsigned int& dest,
                      const std::string& desc);

    //! add size_t parameter [name] with description and store to dest
    void AddParamSizeT(const std::string& name, size_t& dest,
                       const std::string& desc);

    //! add double parameter [name] with description and store to dest
    void AddParamDouble(const std::string& name, double& dest,
                        const std::string& desc);

    //! add SI/IEC suffixes byte size parameter [name] with description and
    //! store to dest
    void AddParamBytes(const std::string& name, uint32_t& dest,
                       const std::string& desc);

    //! add SI/IEC suffixes byte size parameter [name] with description and
    //! store to dest
    void AddParamBytes(const std::string& name, uint64_t& dest,
                       const std::string& desc);

    //! add string parameter [name] with description and store to dest
    void AddParamString(const std::string& name, std::string& dest,
                        const std::string& desc);

    //! add string list parameter [name] with description and store to dest.
    //! \warning this parameter must be last, as it will gobble all non-option
    //! arguments!
    void AddParamStringlist(const std::string& name,
                            std::vector<std::string>& dest,
                            const std::string& desc);

    // ************************************************************************

    //! add optional signed integer parameter [name] with description and store
    //! to dest
    void AddOptParamInt(const std::string& name, int& dest,
                        const std::string& desc);

    //! add optional unsigned integer parameter [name] with description and
    //! store to dest
    void AddOptParamUInt(const std::string& name, unsigned int& dest,
                         const std::string& desc);

    //! add optional size_t parameter [name] with description and store to dest
    void AddOptParamSizeT(const std::string& name, size_t& dest,
                          const std::string& desc);

    //! add optional double parameter [name] with description and store to dest
    void AddOptParamDouble(const std::string& name, double& dest,
                           const std::string& desc);

    //! add optional SI/IEC suffixes byte size parameter [name] with
    //! description and store to dest
    void AddOptParamBytes(const std::string& name, uint32_t& dest,
                          const std::string& desc);

    //! add optional SI/IEC suffixes byte size parameter [name] with
    //! description and store to dest
    void AddOptParamBytes(const std::string& name, uint64_t& dest,
                          const std::string& desc);

    //! add optional string parameter [name] with description and store to dest
    void AddOptParamString(const std::string& name, std::string& dest,
                           const std::string& desc);

    //! add optional string parameter [name] with description and store to dest
    //! \warning this parameter must be last, as it will gobble all non-option
    //! arguments!
    void AddOptParamStringlist(const std::string& name,
                               std::vector<std::string>& dest,
                               const std::string& desc);

    // ************************************************************************

    //! output nicely formatted usage information including description of all
    //! parameters and options.
    void PrintUsage(std::ostream& os = std::cout);

private:
    //! print error about option.
    void PrintOptionError(int argc, const char* const* argv,
                          const Argument* arg,
                          std::ostream& os);

    //! print error about parameter.
    void PrintParamError(int argc, const char* const* argv,
                         const Argument* arg,
                         std::ostream& os);

public:
    //! parse command line options as specified by the options and parameters
    //! added.
    //! \return true if command line is okay and all required parameters are
    //! present.
    bool Process(int argc, const char* const* argv,
                 std::ostream& os = std::cout);

    //! print nicely formatted result of processing
    void PrintResult(std::ostream& os = std::cout);
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_CMDLINE_PARSER_HEADER

/******************************************************************************/
