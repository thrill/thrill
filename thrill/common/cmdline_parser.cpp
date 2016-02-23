/*******************************************************************************
 * thrill/common/cmdline_parser.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/cmdline_parser.hpp>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace thrill {
namespace common {

/******************************************************************************/
// Argument and Struct Hierarchy below it.

//! base class of all options and parameters
struct CmdlineParser::Argument
{
    //! single letter short option, or 0 is none
    char        key_;
    //! long option key or name for parameters
    std::string longkey_;
    //! option type description, e.g. "<#>" to indicate numbers
    std::string keytype_;
    //! longer description, which will be wrapped
    std::string desc_;
    //! required, process() fails if the option/parameter is not found.
    bool        required_;
    //! found during processing of command line
    bool        found_;
    //! repeated argument, i.e. std::vector<std::string>
    bool        repeated_;

    //! contructor filling most attributes
    Argument(char key, const std::string& longkey,
             const std::string& keytype,
             const std::string& desc, bool required)
        : key_(key), longkey_(longkey), keytype_(keytype),
          desc_(desc), required_(required), found_(false),
          repeated_(false)
    { }

    //! empty virtual destructor
    virtual ~Argument() { }

    //! return formatted type name to user
    virtual const char * TypeName() const = 0;

    //! process one item from command line for this argument
    virtual bool Process(int& argc, const char* const*& argv) = 0;

    //! format value to ostream
    virtual void PrintValue(std::ostream& os) const = 0;

    //! return 'longkey [keytype]'
    std::string ParamText() const {
        std::string s = longkey_;
        if (keytype_.size()) {
            s += ' ' + keytype_;
        }
        return s;
    }

    //! return '-s, --longkey [keytype]'
    std::string OptionText() const {
        std::string s;
        if (key_) {
            s += '-';
            s += key_;
            s += ", ";
        }
        s += "--";
        s += longkey_;
        if (keytype_.size()) {
            s += ' ' + keytype_;
        }
        return s;
    }
};

//! specialization of argument for boolean flags (can only be set to true).
struct CmdlineParser::ArgumentFlag final : public Argument
{
    //! reference to boolean to set to true
    bool& dest_;

    //! contructor filling most attributes
    ArgumentFlag(char key, const std::string& longkey,
                 const std::string& keytype,
                 const std::string& desc, bool required, bool& dest)
        : Argument(key, longkey, keytype, desc, required),
          dest_(dest)
    { }

    const char * TypeName() const final
    { return "flag"; }

    //! "process" argument: just set to true, no argument is used.
    bool Process(int&, const char* const*&) final {
        dest_ = true;
        return true;
    }

    void PrintValue(std::ostream& os) const final
    { os << (dest_ ? "true" : "false"); }
};

//! specialization of argument for integer options or parameters
struct CmdlineParser::ArgumentInt final : public Argument
{
    int& dest_;

    //! contructor filling most attributes
    ArgumentInt(char key, const std::string& longkey,
                const std::string& keytype,
                const std::string& desc, bool required, int& dest)
        : Argument(key, longkey, keytype, desc, required),
          dest_(dest)
    { }

    const char * TypeName() const final
    { return "integer"; }

    //! parse signed integer using sscanf.
    bool Process(int& argc, const char* const*& argv) final {
        if (argc == 0) return false;
        if (sscanf(argv[0], "%d", &dest_) == 1) {
            --argc, ++argv;
            return true;
        }
        else {
            return false;
        }
    }

    void PrintValue(std::ostream& os) const final
    { os << dest_; }
};

//! specialization of argument for unsigned integer options or parameters
struct CmdlineParser::ArgumentUInt final : public Argument
{
    unsigned int& dest_;

    //! contructor filling most attributes
    ArgumentUInt(char key, const std::string& longkey,
                 const std::string& keytype,
                 const std::string& desc, bool required,
                 unsigned int& dest)
        : Argument(key, longkey, keytype, desc, required),
          dest_(dest)
    { }

    const char * TypeName() const final
    { return "unsigned integer"; }

    //! parse unsigned integer using sscanf.
    bool Process(int& argc, const char* const*& argv) final {
        if (argc == 0) return false;
        if (sscanf(argv[0], "%u", &dest_) == 1) {
            --argc, ++argv;
            return true;
        }
        else {
            return false;
        }
    }

    void PrintValue(std::ostream& os) const final
    { os << dest_; }
};

//! specialization of argument for size_t options or parameters
struct CmdlineParser::ArgumentSizeT final : public Argument
{
    size_t& dest_;

    //! contructor filling most attributes
    ArgumentSizeT(char key, const std::string& longkey,
                  const std::string& keytype,
                  const std::string& desc, bool required,
                  size_t& dest)
        : Argument(key, longkey, keytype, desc, required),
          dest_(dest)
    { }

    const char * TypeName() const final
    { return "size_t"; }

    //! parse size_t using sscanf.
    bool Process(int& argc, const char* const*& argv) final {
        if (argc == 0) return false;
        if (sscanf(argv[0], "%zu", &dest_) == 1) {
            --argc, ++argv;
            return true;
        }
        else {
            return false;
        }
    }

    void PrintValue(std::ostream& os) const final
    { os << dest_; }
};

//! specialization of argument for double options or parameters
struct CmdlineParser::ArgumentDouble final : public Argument
{
    double& dest_;

    //! contructor filling most attributes
    ArgumentDouble(char key, const std::string& longkey,
                   const std::string& keytype,
                   const std::string& desc, bool required,
                   double& dest)
        : Argument(key, longkey, keytype, desc, required),
          dest_(dest)
    { }

    const char * TypeName() const final
    { return "double"; }

    //! parse unsigned integer using sscanf.
    bool Process(int& argc, const char* const*& argv) final {
        if (argc == 0) return false;
        if (sscanf(argv[0], "%lf", &dest_) == 1) {
            --argc, ++argv;
            return true;
        }
        else {
            return false;
        }
    }

    void PrintValue(std::ostream& os) const final
    { os << dest_; }
};

//! specialization of argument for SI/IEC suffixes byte size options or
//! parameters
struct CmdlineParser::ArgumentBytes32 final : public Argument
{
    uint32_t& dest_;

    //! contructor filling most attributes
    ArgumentBytes32(char key, const std::string& longkey,
                    const std::string& keytype,
                    const std::string& desc, bool required, uint32_t& dest)
        : Argument(key, longkey, keytype, desc, required),
          dest_(dest)
    { }

    const char * TypeName() const final
    { return "bytes"; }

    //! parse byte size using SI/IEC parser from stxxl.
    bool Process(int& argc, const char* const*& argv) final {
        if (argc == 0) return false;
        uint64_t dest;
        if (ParseSiIecUnits(argv[0], dest) &&
            (uint64_t)(dest_ = (uint32_t)dest) == dest) {
            --argc, ++argv;
            return true;
        }
        else {
            return false;
        }
    }

    void PrintValue(std::ostream& os) const final
    { os << dest_; }
};

//! specialization of argument for SI/IEC suffixes byte size options or
//! parameters
struct CmdlineParser::ArgumentBytes64 final : public Argument
{
    uint64_t& dest_;

    //! contructor filling most attributes
    ArgumentBytes64(char key, const std::string& longkey,
                    const std::string& keytype,
                    const std::string& desc, bool required, uint64_t& dest)
        : Argument(key, longkey, keytype, desc, required),
          dest_(dest)
    { }

    const char * TypeName() const final
    { return "bytes"; }

    //! parse byte size using SI/IEC parser from stxxl.
    bool Process(int& argc, const char* const*& argv) final {
        if (argc == 0) return false;
        if (ParseSiIecUnits(argv[0], dest_)) {
            --argc, ++argv;
            return true;
        }
        else {
            return false;
        }
    }

    void PrintValue(std::ostream& os) const final
    { os << dest_; }
};

//! specialization of argument for string options or parameters
struct CmdlineParser::ArgumentString final : public Argument
{
    std::string& dest_;

    //! contructor filling most attributes
    ArgumentString(char key, const std::string& longkey,
                   const std::string& keytype,
                   const std::string& desc, bool required,
                   std::string& dest)
        : Argument(key, longkey, keytype, desc, required),
          dest_(dest)
    { }

    const char * TypeName() const final
    { return "string"; }

    //! "process" string argument just by storing it.
    bool Process(int& argc, const char* const*& argv) final {
        if (argc == 0) return false;
        dest_ = argv[0];
        --argc, ++argv;
        return true;
    }

    void PrintValue(std::ostream& os) const final
    { os << '"' << dest_ << '"'; }
};

//! specialization of argument for multiple string options or parameters
struct CmdlineParser::ArgumentStringlist final : public Argument
{
    std::vector<std::string>& dest_;

    //! contructor filling most attributes
    ArgumentStringlist(char key, const std::string& longkey,
                       const std::string& keytype,
                       const std::string& desc, bool required,
                       std::vector<std::string>& dest)
        : Argument(key, longkey, keytype, desc, required),
          dest_(dest) {
        repeated_ = true;
    }

    const char * TypeName() const final
    { return "string list"; }

    //! "process" string argument just by storing it in vector.
    bool Process(int& argc, const char* const*& argv) final {
        if (argc == 0) return false;
        dest_.push_back(argv[0]);
        --argc, ++argv;
        return true;
    }

    void PrintValue(std::ostream& os) const final {
        os << '[';
        for (size_t i = 0; i < dest_.size(); ++i)
        {
            if (i != 0) os << ',';
            os << '"' << dest_[i] << '"';
        }
        os << ']';
    }
};

/******************************************************************************/

//! update maximum formatting width for new option
void CmdlineParser::CalcOptMax(const Argument* arg) {
    opt_maxlong_ = std::max(static_cast<int>(arg->OptionText().size() + 2),
                            opt_maxlong_);
}

//! update maximum formatting width for new parameter
void CmdlineParser::CalcParamMax(const Argument* arg) {
    param_maxlong_ = std::max(static_cast<int>(arg->ParamText().size() + 2),
                              param_maxlong_);
}

/******************************************************************************/

void CmdlineParser::OutputWrap(
    std::ostream& os, const std::string& text, size_t wraplen,
    size_t indent_first, size_t indent_rest,
    size_t current, size_t indent_newline) {
    std::string::size_type t = 0;
    size_t indent = indent_first;
    while (t != text.size())
    {
        std::string::size_type to = t, lspace = t;

        // scan forward in text until we hit a newline or wrap point
        while (to != text.size() &&
               to + current + indent < t + wraplen &&
               text[to] != '\n')
        {
            if (text[to] == ' ') lspace = to;
            ++to;
        }

        // go back to last space
        if (to != text.size() && text[to] != '\n' &&
            lspace != t) to = lspace + 1;

        // output line
        os << std::string(indent, ' ')
           << text.substr(t, to - t) << std::endl;

        current = 0;
        indent = indent_rest;

        // skip over last newline
        if (to != text.size() && text[to] == '\n') {
            indent = indent_newline;
            ++to;
        }

        t = to;
    }
}

/******************************************************************************/

//! Delete all added arguments
CmdlineParser::~CmdlineParser() {
    for (size_t i = 0; i < optlist_.size(); ++i)
        delete optlist_[i];
    optlist_.clear();

    for (size_t i = 0; i < paramlist_.size(); ++i)
        delete paramlist_[i];
    paramlist_.clear();
}

//! Set description of program, text will be wrapped
void CmdlineParser::SetDescription(const std::string& description) {
    description_ = description;
}

//! Set author of program, will be wrapped.
void CmdlineParser::SetAuthor(const std::string& author) {
    author_ = author;
}

//! Set verbose processing of command line arguments
void CmdlineParser::SetVerboseProcess(bool verbose_process) {
    verbose_process_ = verbose_process;
}

/******************************************************************************/

//! add boolean option flag -key, --longkey [keytype] with description and
//! store to dest
void CmdlineParser::AddFlag(char key, const std::string& longkey,
                            const std::string& keytype, bool& dest,
                            const std::string& desc) {
    optlist_.push_back(
        new ArgumentFlag(key, longkey, keytype, desc, false, dest));
    CalcOptMax(optlist_.back());
}

//! add signed integer option -key, --longkey [keytype] with description
//! and store to dest
void CmdlineParser::AddInt(char key, const std::string& longkey,
                           const std::string& keytype, int& dest,
                           const std::string& desc) {
    optlist_.push_back(
        new ArgumentInt(key, longkey, keytype, desc, false, dest));
    CalcOptMax(optlist_.back());
}

//! add unsigned integer option -key, --longkey [keytype] with description
//! and store to dest
void CmdlineParser::AddUInt(char key, const std::string& longkey,
                            const std::string& keytype, unsigned int& dest,
                            const std::string& desc) {
    optlist_.push_back(
        new ArgumentUInt(key, longkey, keytype, desc, false, dest));
    CalcOptMax(optlist_.back());
}

//! add size_t option -key, --longkey [keytype] with description and store to
//! dest
void CmdlineParser::AddSizeT(char key, const std::string& longkey,
                             const std::string& keytype, size_t& dest,
                             const std::string& desc) {
    optlist_.push_back(
        new ArgumentSizeT(key, longkey, keytype, desc, false, dest));
    CalcOptMax(optlist_.back());
}

//! add double option -key, --longkey [keytype] with description and store
//! to dest
void CmdlineParser::AddDouble(char key, const std::string& longkey,
                              const std::string& keytype, double& dest,
                              const std::string& desc) {
    optlist_.push_back(
        new ArgumentDouble(key, longkey, keytype, desc, false, dest));
    CalcOptMax(optlist_.back());
}

//! add SI/IEC suffixes byte size option -key, --longkey [keytype] and
//! store to 64-bit dest
void CmdlineParser::AddBytes(char key, const std::string& longkey,
                             const std::string& keytype, uint32_t& dest,
                             const std::string& desc) {
    optlist_.push_back(
        new ArgumentBytes32(key, longkey, keytype, desc, false, dest));
    CalcOptMax(optlist_.back());
}

//! add SI/IEC suffixes byte size option -key, --longkey [keytype] and
//! store to 64-bit dest
void CmdlineParser::AddBytes(char key, const std::string& longkey,
                             const std::string& keytype, uint64_t& dest,
                             const std::string& desc) {
    optlist_.push_back(
        new ArgumentBytes64(key, longkey, keytype, desc, false, dest));
    CalcOptMax(optlist_.back());
}

//! add string option -key, --longkey [keytype] and store to dest
void CmdlineParser::AddString(char key, const std::string& longkey,
                              const std::string& keytype, std::string& dest,
                              const std::string& desc) {
    optlist_.push_back(
        new ArgumentString(key, longkey, keytype, desc, false, dest));
    CalcOptMax(optlist_.back());
}

//! add string list option -key, --longkey [keytype] and store to dest
void CmdlineParser::AddStringlist(char key, const std::string& longkey,
                                  const std::string& keytype,
                                  std::vector<std::string>& dest,
                                  const std::string& desc) {
    optlist_.push_back(
        new ArgumentStringlist(key, longkey, keytype, desc, false, dest));
    CalcOptMax(optlist_.back());
}

//! add boolean option flag -key, --longkey with description and store to
//! dest
void CmdlineParser::AddFlag(char key, const std::string& longkey, bool& dest,
                            const std::string& desc) {
    return AddFlag(key, longkey, "", dest, desc);
}

//! add signed integer option -key, --longkey with description and store to
//! dest
void CmdlineParser::AddInt(char key, const std::string& longkey, int& dest,
                           const std::string& desc) {
    return AddInt(key, longkey, "", dest, desc);
}

//! add unsigned integer option -key, --longkey [keytype] with description
//! and store to dest
void CmdlineParser::AddUInt(
    char key, const std::string& longkey, unsigned int& dest,
    const std::string& desc) {
    return AddUInt(key, longkey, "", dest, desc);
}

//! add size_t option -key, --longkey [keytype] with description and store to
//! dest
void CmdlineParser::AddSizeT(
    char key, const std::string& longkey, size_t& dest,
    const std::string& desc) {
    return AddSizeT(key, longkey, "", dest, desc);
}

//! add double option -key, --longkey [keytype] with description and store
//! to dest
void CmdlineParser::AddDouble(
    char key, const std::string& longkey, double& dest,
    const std::string& desc) {
    return AddDouble(key, longkey, "", dest, desc);
}

//! add SI/IEC suffixes byte size option -key, --longkey [keytype] and
//! store to 32-bit dest
void CmdlineParser::AddBytes(
    char key, const std::string& longkey, uint32_t& dest,
    const std::string& desc) {
    return AddBytes(key, longkey, "", dest, desc);
}

//! add SI/IEC suffixes byte size option -key, --longkey [keytype] and
//! store to 64-bit dest
void CmdlineParser::AddBytes(
    char key, const std::string& longkey, uint64_t& dest,
    const std::string& desc) {
    return AddBytes(key, longkey, "", dest, desc);
}

//! add string option -key, --longkey [keytype] and store to dest
void CmdlineParser::AddString(
    char key, const std::string& longkey, std::string& dest,
    const std::string& desc) {
    return AddString(key, longkey, "", dest, desc);
}

//! add string list option -key, --longkey [keytype] and store to dest
void CmdlineParser::AddStringlist(
    char key, const std::string& longkey,
    std::vector<std::string>& dest, const std::string& desc) {
    return AddStringlist(key, longkey, "", dest, desc);
}

// ************************************************************************

//! add signed integer parameter [name] with description and store to dest
void CmdlineParser::AddParamInt(const std::string& name, int& dest,
                                const std::string& desc) {
    paramlist_.push_back(
        new ArgumentInt(0, name, "", desc, true, dest));
    CalcParamMax(paramlist_.back());
}

//! add unsigned integer parameter [name] with description and store to dest
void CmdlineParser::AddParamUInt(const std::string& name, unsigned int& dest,
                                 const std::string& desc) {
    paramlist_.push_back(
        new ArgumentUInt(0, name, "", desc, true, dest));
    CalcParamMax(paramlist_.back());
}

//! add size_t parameter [name] with description and store to dest
void CmdlineParser::AddParamSizeT(const std::string& name, size_t& dest,
                                  const std::string& desc) {
    paramlist_.push_back(
        new ArgumentSizeT(0, name, "", desc, true, dest));
    CalcParamMax(paramlist_.back());
}

//! add double parameter [name] with description and store to dest
void CmdlineParser::AddParamDouble(const std::string& name, double& dest,
                                   const std::string& desc) {
    paramlist_.push_back(
        new ArgumentDouble(0, name, "", desc, true, dest));
    CalcParamMax(paramlist_.back());
}

//! add SI/IEC suffixes byte size parameter [name] with description and
//! store to dest
void CmdlineParser::AddParamBytes(const std::string& name, uint32_t& dest,
                                  const std::string& desc) {
    paramlist_.push_back(
        new ArgumentBytes32(0, name, "", desc, true, dest));
    CalcParamMax(paramlist_.back());
}

//! add SI/IEC suffixes byte size parameter [name] with description and
//! store to dest
void CmdlineParser::AddParamBytes(const std::string& name, uint64_t& dest,
                                  const std::string& desc) {
    paramlist_.push_back(
        new ArgumentBytes64(0, name, "", desc, true, dest));
    CalcParamMax(paramlist_.back());
}

//! add string parameter [name] with description and store to dest
void CmdlineParser::AddParamString(const std::string& name, std::string& dest,
                                   const std::string& desc) {
    paramlist_.push_back(
        new ArgumentString(0, name, "", desc, true, dest));
    CalcParamMax(paramlist_.back());
}

//! add string list parameter [name] with description and store to dest.
//! \warning this parameter must be last, as it will gobble all non-option
//! arguments!
void CmdlineParser::AddParamStringlist(const std::string& name,
                                       std::vector<std::string>& dest,
                                       const std::string& desc) {
    paramlist_.push_back(
        new ArgumentStringlist(0, name, "", desc, true, dest));
    CalcParamMax(paramlist_.back());
}

// ************************************************************************

//! add optional signed integer parameter [name] with description and store
//! to dest
void CmdlineParser::AddOptParamInt(
    const std::string& name, int& dest, const std::string& desc) {
    paramlist_.push_back(
        new ArgumentInt(0, name, "", desc, false, dest));
    CalcParamMax(paramlist_.back());
}

//! add optional unsigned integer parameter [name] with description and
//! store to dest
void CmdlineParser::AddOptParamUInt(
    const std::string& name, unsigned int& dest, const std::string& desc) {
    paramlist_.push_back(
        new ArgumentUInt(0, name, "", desc, false, dest));
    CalcParamMax(paramlist_.back());
}

//! add optional size_t parameter [name] with description and store to dest
void CmdlineParser::AddOptParamSizeT(
    const std::string& name, size_t& dest, const std::string& desc) {
    paramlist_.push_back(
        new ArgumentSizeT(0, name, "", desc, false, dest));
    CalcParamMax(paramlist_.back());
}

//! add optional double parameter [name] with description and store to dest
void CmdlineParser::AddOptParamDouble(
    const std::string& name, double& dest, const std::string& desc) {
    paramlist_.push_back(
        new ArgumentDouble(0, name, "", desc, false, dest));
    CalcParamMax(paramlist_.back());
}

//! add optional SI/IEC suffixes byte size parameter [name] with
//! description and store to dest
void CmdlineParser::AddOptParamBytes(
    const std::string& name, uint32_t& dest, const std::string& desc) {
    paramlist_.push_back(
        new ArgumentBytes32(0, name, "", desc, false, dest));
    CalcParamMax(paramlist_.back());
}

//! add optional SI/IEC suffixes byte size parameter [name] with
//! description and store to dest
void CmdlineParser::AddOptParamBytes(
    const std::string& name, uint64_t& dest, const std::string& desc) {
    paramlist_.push_back(
        new ArgumentBytes64(0, name, "", desc, false, dest));
    CalcParamMax(paramlist_.back());
}

//! add optional string parameter [name] with description and store to dest
void CmdlineParser::AddOptParamString(
    const std::string& name, std::string& dest, const std::string& desc) {
    paramlist_.push_back(
        new ArgumentString(0, name, "", desc, false, dest));
    CalcParamMax(paramlist_.back());
}

//! add optional string parameter [name] with description and store to dest
//! \warning this parameter must be last, as it will gobble all non-option
//! arguments!
void CmdlineParser::AddOptParamStringlist(const std::string& name,
                                          std::vector<std::string>& dest,
                                          const std::string& desc) {
    paramlist_.push_back(
        new ArgumentStringlist(0, name, "", desc, false, dest));
    CalcParamMax(paramlist_.back());
}

/******************************************************************************/

void CmdlineParser::PrintUsage(std::ostream& os) {
    std::ios::fmtflags flags(os.flags());

    os << "Usage: " << progname_
       << (optlist_.size() ? " [options]" : "");

    for (ArgumentList::const_iterator it = paramlist_.begin();
         it != paramlist_.end(); ++it)
    {
        const Argument* arg = *it;

        os << (arg->required_ ? " <" : " [")
           << arg->longkey_
           << (arg->repeated_ ? " ..." : "")
           << (arg->required_ ? '>' : ']');
    }

    os << std::endl;

    if (description_.size())
    {
        os << std::endl;
        OutputWrap(os, description_, linewrap_);
    }
    if (author_.size())
    {
        os << "Author: " << author_ << std::endl;
    }

    if (description_.size() || author_.size())
        os << std::endl;

    if (paramlist_.size())
    {
        os << "Parameters:" << std::endl;

        for (ArgumentList::const_iterator it = paramlist_.begin();
             it != paramlist_.end(); ++it)
        {
            const Argument* arg = *it;

            os << "  " << std::setw(param_maxlong_) << std::left
               << arg->ParamText();
            OutputWrap(os, arg->desc_, linewrap_,
                       0, param_maxlong_ + 2, param_maxlong_ + 2, 8);
        }
    }

    if (optlist_.size())
    {
        os << "Options:" << std::endl;

        for (ArgumentList::const_iterator it = optlist_.begin();
             it != optlist_.end(); ++it)
        {
            const Argument* arg = *it;

            os << "  " << std::setw(opt_maxlong_) << std::left
               << arg->OptionText();
            OutputWrap(os, arg->desc_, linewrap_,
                       0, opt_maxlong_ + 2, opt_maxlong_ + 2, 8);
        }
    }

    os.flags(flags);
}

void CmdlineParser::PrintOptionError(
    int argc, const char* const* argv, const Argument* arg, std::ostream& os) {
    os << "Error: Argument ";
    if (argc != 0)
        os << '"' << argv[0] << '"';

    os << " for " << arg->TypeName() << " option " << arg->OptionText()
       << (argc == 0 ? " is missing!" : " is invalid!")
       << std::endl << std::endl;

    PrintUsage(os);
}

void CmdlineParser::PrintParamError(
    int argc, const char* const* argv, const Argument* arg, std::ostream& os) {
    os << "Error: Argument ";
    if (argc != 0)
        os << '"' << argv[0] << '"';

    os << " for " << arg->TypeName() << " parameter " << arg->ParamText()
       << (argc == 0 ? " is missing!" : " is invalid!")
       << std::endl << std::endl;

    PrintUsage(os);
}

bool CmdlineParser::Process(
    int argc, const char* const* argv, std::ostream& os) {
    progname_ = argv[0];
    --argc, ++argv;

    // search for help string and output help
    for (int i = 0; i < argc; ++i)
    {
        if (strcmp(argv[i], "-h") == 0 ||
            strcmp(argv[i], "--help") == 0)
        {
            PrintUsage(os);
            return false;
        }
    }

    // current argument in paramlist_
    ArgumentList::iterator argi = paramlist_.begin();
    bool end_optlist = false;

    while (argc != 0)
    {
        const char* arg = argv[0];

        if (arg[0] == '-' && !end_optlist)
        {
            // option, advance to argument
            --argc, ++argv;
            if (arg[1] == '-')
            {
                if (arg[2] == '-') {
                    end_optlist = true;
                }
                else {
                    // long option
                    ArgumentList::const_iterator oi = optlist_.begin();
                    for ( ; oi != optlist_.end(); ++oi)
                    {
                        if ((arg + 2) == (*oi)->longkey_)
                        {
                            if (!(*oi)->Process(argc, argv))
                            {
                                PrintOptionError(argc, argv, *oi, os);
                                return false;
                            }
                            else if (verbose_process_)
                            {
                                os << "Option " << (*oi)->OptionText()
                                   << " set to ";
                                (*oi)->PrintValue(os);
                                os << '.' << std::endl;
                            }
                            break;
                        }
                    }
                    if (oi == optlist_.end())
                    {
                        os << "Error: Unknown option \"" << arg << "\"."
                           << std::endl << std::endl;
                        PrintUsage(os);
                        return false;
                    }
                }
            }
            else
            {
                // short option
                if (arg[1] == 0) {
                    os << "Invalid option \"" << arg << '"' << std::endl;
                }
                else {
                    ArgumentList::const_iterator oi = optlist_.begin();
                    for ( ; oi != optlist_.end(); ++oi)
                    {
                        if (arg[1] == (*oi)->key_)
                        {
                            if (!(*oi)->Process(argc, argv))
                            {
                                PrintOptionError(argc, argv, *oi, os);
                                return false;
                            }
                            else if (verbose_process_)
                            {
                                os << "Option " << (*oi)->OptionText()
                                   << " set to ";
                                (*oi)->PrintValue(os);
                                os << '.' << std::endl;
                            }
                            break;
                        }
                    }
                    if (oi == optlist_.end())
                    {
                        os << "Error: Unknown option \"" << arg << "\"."
                           << std::endl << std::endl;
                        PrintUsage(os);
                        return false;
                    }
                }
            }
        }
        else
        {
            if (argi != paramlist_.end())
            {
                if (!(*argi)->Process(argc, argv))
                {
                    PrintParamError(argc, argv, *argi, os);
                    return false;
                }
                else if (verbose_process_)
                {
                    os << "Parameter " << (*argi)->ParamText() << " set to ";
                    (*argi)->PrintValue(os);
                    os << '.' << std::endl;
                }
                (*argi)->found_ = true;
                if (!(*argi)->repeated_)
                    ++argi;
            }
            else
            {
                os << "Error: Unexpected extra argument \"" << argv[0] << "\"."
                   << std::endl << std::endl;
                --argc, ++argv;
                PrintUsage(os);
                return false;
            }
        }
    }

    bool good = true;

    for (ArgumentList::const_iterator it = paramlist_.begin();
         it != paramlist_.end(); ++it)
    {
        if ((*it)->required_ && !(*it)->found_) {
            os << "Error: Argument for parameter "
               << (*it)->longkey_ << " is required!" << std::endl;
            good = false;
        }
    }

    if (!good) {
        os << std::endl;
        PrintUsage(os);
    }

    return good;
}

void CmdlineParser::PrintResult(std::ostream& os) {
    std::ios::fmtflags flags(os.flags());

    int maxlong = std::max(param_maxlong_, opt_maxlong_);

    if (paramlist_.size())
    {
        os << "Parameters:" << std::endl;

        for (ArgumentList::const_iterator it = paramlist_.begin();
             it != paramlist_.end(); ++it)
        {
            const Argument* arg = *it;

            os << "  " << std::setw(maxlong) << std::left << arg->ParamText();

            std::string typestr = "(" + std::string(arg->TypeName()) + ")";
            os << std::setw(maxtypename_ + 4) << typestr;

            arg->PrintValue(os);

            os << std::endl;
        }
    }

    if (optlist_.size())
    {
        os << "Options:" << std::endl;

        for (ArgumentList::const_iterator it = optlist_.begin();
             it != optlist_.end(); ++it)
        {
            const Argument* arg = *it;

            os << "  " << std::setw(maxlong) << std::left << arg->OptionText();

            std::string typestr = "(" + std::string(arg->TypeName()) + ")";
            os << std::setw(maxtypename_ + 4) << std::left << typestr;

            arg->PrintValue(os);

            os << std::endl;
        }
    }

    os.flags(flags);
}

/******************************************************************************/

//! Parse a string like "343KB" or " 44 GiB " into the corresponding size in
//! bytes.
bool ParseSiIecUnits(const char* str, uint64_t& size, char default_unit) {
    char* endptr;
    size = strtoul(str, &endptr, 10);
    if (!endptr) return false;                    // parse failed, no number

    while (endptr[0] == ' ') ++endptr;            // skip over spaces

    // multiply with base ^ power
    unsigned int base = 1000;
    unsigned int power = 0;

    if (endptr[0] == 'k' || endptr[0] == 'K')
        power = 1, ++endptr;
    else if (endptr[0] == 'm' || endptr[0] == 'M')
        power = 2, ++endptr;
    else if (endptr[0] == 'g' || endptr[0] == 'G')
        power = 3, ++endptr;
    else if (endptr[0] == 't' || endptr[0] == 'T')
        power = 4, ++endptr;
    else if (endptr[0] == 'p' || endptr[0] == 'P')
        power = 5, ++endptr;

    // switch to power of two (only if power was set above)
    if ((endptr[0] == 'i' || endptr[0] == 'I') && power != 0)
        base = 1024, ++endptr;

    // byte indicator
    if (endptr[0] == 'b' || endptr[0] == 'B') {
        ++endptr;
    }
    else if (power == 0)
    {
        // no explicit power indicator, and no 'b' or 'B' -> apply default unit
        switch (default_unit)
        {
        default: break;
        case 'k': power = 1, base = 1000;
            break;
        case 'm': power = 2, base = 1000;
            break;
        case 'g': power = 3, base = 1000;
            break;
        case 't': power = 4, base = 1000;
            break;
        case 'p': power = 5, base = 1000;
            break;
        case 'K': power = 1, base = 1024;
            break;
        case 'M': power = 2, base = 1024;
            break;
        case 'G': power = 3, base = 1024;
            break;
        case 'T': power = 4, base = 1024;
            break;
        case 'P': power = 5, base = 1024;
            break;
        }
    }

    // skip over spaces
    while (endptr[0] == ' ') ++endptr;

    // multiply size
    for (unsigned int p = 0; p < power; ++p)
        size *= base;

    return (endptr[0] == 0);
}

//! Format number as something like 1 TB
std::string FormatSiUnits(uint64_t number) {
    // may not overflow, std::numeric_limits<uint64_t>::max() == 16 EiB
    double multiplier = 1000.0;
    static const char* SI_endings[] = {
        "", "k", "M", "G", "T", "P", "E"
    };
    unsigned int scale = 0;
    double number_d = static_cast<double>(number);
    while (number_d >= multiplier) {
        number_d /= multiplier;
        ++scale;
    }
    std::ostringstream out;
    out << std::fixed << std::setprecision(3) << number_d
        << ' ' << SI_endings[scale];
    return out.str();
}

//! Format number as something like 1 TiB
std::string FormatIecUnits(uint64_t number) {
    // may not overflow, std::numeric_limits<uint64_t>::max() == 16 EiB
    double multiplier = 1024.0;
    static const char* IEC_endings[] = {
        "", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei"
    };
    unsigned int scale = 0;
    double number_d = static_cast<double>(number);
    while (number_d >= multiplier) {
        number_d /= multiplier;
        ++scale;
    }
    std::ostringstream out;
    out << std::fixed << std::setprecision(3) << number_d
        << ' ' << IEC_endings[scale];
    return out.str();
}

} // namespace common
} // namespace thrill

/******************************************************************************/
