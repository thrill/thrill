/*******************************************************************************
 * c7a/common/cmdline_parser.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_CMDLINE_PARSER_HEADER
#define C7A_COMMON_CMDLINE_PARSER_HEADER

#include <iostream>
#include <string>
#include <vector>

namespace c7a {
namespace common {

//! Parse a string like "343KB" or "44 GiB" into the corresponding size in
//! bytes. Returns the number of bytes and sets ok = true if the string could
//! be parsed correctly. If no units indicator is given, use def_unit in
//! k/m/g/t/p (powers of ten) or in K/M/G/T/P (power of two).
bool ParseSiIecUnits(const std::string& str, uint64_t& size, char def_unit = 0);

//! Format a byte size using SI (K, M, G, T) suffixes (powers of ten). Returns
//! "123 M" or similar.
std::string FormatSiUnits(uint64_t number);

//! Format a byte size using IEC (Ki, Mi, Gi, Ti) suffixes (powers of
//! two). Returns "123 Ki" or similar.
std::string FormatIecUnits(uint64_t number);

/******************************************************************************/

/**
 * Command line parser which automatically fills variables and prints nice
 * usage messages.
 *
 * This is a straightforward command line parser in C++, which will recognize
 * short options -s, long options --long and parameters, both required and
 * optional. It will automatically parse integers and <b>byte sizes</b> with
 * SI/IEC suffixes (e.g. 1 GiB). It also works with lists of strings,
 * e.g. multiple filenames.
 *
 * Maybe most important it will nicely format the options and parameters
 * description using word wrapping.
 */
class CmdlineParser
{
protected:
    // forward declaration of Argument classes
    struct Argument;
    struct ArgumentFlag;
    struct ArgumentInt;
    struct ArgumentUInt;
    struct ArgumentDouble;
    struct ArgumentBytes32;
    struct ArgumentBytes64;
    struct ArgumentString;
    struct ArgumentStringlist;

protected:
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
    bool verbose_process_ = true;

    //! user set description of program, will be wrapped
    std::string description_;
    //! user set author of program, will be wrapped
    std::string author_;

    //! set line wrap length
    unsigned int linewrap_ = 80;

    //! maximum length of a TypeName() result
    static const int maxtypename_ = 16;

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
} // namespace c7a

#endif // !C7A_COMMON_CMDLINE_PARSER_HEADER

/******************************************************************************/
