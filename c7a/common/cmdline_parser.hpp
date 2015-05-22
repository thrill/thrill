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

#include <cstddef>
#include <cstdio>
#include <iostream>
#include <vector>
#include <sstream>
#include <string>
#include <algorithm>

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
    //! base class of all options and parameters
    struct Argument
    {
        //! single letter short option, or 0 is none
        char               key_;
        //! long option key or name for parameters
        std::string        longkey_;
        //! option type description, e.g. "<#>" to indicate numbers
        std::string        keytype_;
        //! longer description, which will be wrapped
        std::string        desc_;
        //! required, process() fails if the option/parameter is not found.
        bool               required_;
        //! found during processing of command line
        bool               found_;
        //! repeated argument, i.e. std::vector<std::string>
        bool               repeated_;

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
        virtual bool       Process(int& argc, const char* const*& argv) = 0;

        //! format value to ostream
        virtual void       PrintValue(std::ostream& os) const = 0;

        //! return 'longkey [keytype]'
        std::string        ParamText() const {
            std::string s = longkey_;
            if (keytype_.size()) {
                s += ' ' + keytype_;
            }
            return s;
        }

        //! return '-s, --longkey [keytype]'
        std::string        OptionText() const {
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
    struct ArgumentFlag : public Argument
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

        virtual const char * TypeName() const
        { return "flag"; }

        //! "process" argument: just set to true, no argument is used.
        virtual bool Process(int&, const char* const*&) {
            dest_ = true;
            return true;
        }

        virtual void PrintValue(std::ostream& os) const
        { os << (dest_ ? "true" : "false"); }
    };

    //! specialization of argument for integer options or parameters
    struct ArgumentInt : public Argument
    {
        int& dest_;

        //! contructor filling most attributes
        ArgumentInt(char key, const std::string& longkey,
                    const std::string& keytype,
                    const std::string& desc, bool required, int& dest)
            : Argument(key, longkey, keytype, desc, required),
              dest_(dest)
        { }

        virtual const char * TypeName() const
        { return "integer"; }

        //! parse signed integer using sscanf.
        virtual bool Process(int& argc, const char* const*& argv) {
            if (argc == 0) return false;
            if (sscanf(argv[0], "%d", &dest_) == 1) {
                --argc, ++argv;
                return true;
            }
            else {
                return false;
            }
        }

        virtual void PrintValue(std::ostream& os) const
        { os << dest_; }
    };

    //! specialization of argument for unsigned integer options or parameters
    struct ArgumentUInt : public Argument
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

        virtual const char * TypeName() const
        { return "unsigned integer"; }

        //! parse unsigned integer using sscanf.
        virtual bool Process(int& argc, const char* const*& argv) {
            if (argc == 0) return false;
            if (sscanf(argv[0], "%u", &dest_) == 1) {
                --argc, ++argv;
                return true;
            }
            else {
                return false;
            }
        }

        virtual void PrintValue(std::ostream& os) const
        { os << dest_; }
    };

    //! specialization of argument for double options or parameters
    struct ArgumentDouble : public Argument
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

        virtual const char * TypeName() const
        { return "double"; }

        //! parse unsigned integer using sscanf.
        virtual bool Process(int& argc, const char* const*& argv) {
            if (argc == 0) return false;
            if (sscanf(argv[0], "%lf", &dest_) == 1) {
                --argc, ++argv;
                return true;
            }
            else {
                return false;
            }
        }

        virtual void PrintValue(std::ostream& os) const
        { os << dest_; }
    };

    //! specialization of argument for SI/IEC suffixes byte size options or
    //! parameters
    struct ArgumentBytes32 : public Argument
    {
        uint32_t& dest_;

        //! contructor filling most attributes
        ArgumentBytes32(char key, const std::string& longkey,
                        const std::string& keytype,
                        const std::string& desc, bool required, uint32_t& dest)
            : Argument(key, longkey, keytype, desc, required),
              dest_(dest)
        { }

        virtual const char * TypeName() const
        { return "bytes"; }

        //! parse byte size using SI/IEC parser from stxxl.
        virtual bool Process(int& argc, const char* const*& argv) {
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

        virtual void PrintValue(std::ostream& os) const
        { os << dest_; }
    };

    //! specialization of argument for SI/IEC suffixes byte size options or
    //! parameters
    struct ArgumentBytes64 : public Argument
    {
        uint64_t& dest_;

        //! contructor filling most attributes
        ArgumentBytes64(char key, const std::string& longkey,
                        const std::string& keytype,
                        const std::string& desc, bool required, uint64_t& dest)
            : Argument(key, longkey, keytype, desc, required),
              dest_(dest)
        { }

        virtual const char * TypeName() const
        { return "bytes"; }

        //! parse byte size using SI/IEC parser from stxxl.
        virtual bool Process(int& argc, const char* const*& argv) {
            if (argc == 0) return false;
            if (ParseSiIecUnits(argv[0], dest_)) {
                --argc, ++argv;
                return true;
            }
            else {
                return false;
            }
        }

        virtual void PrintValue(std::ostream& os) const
        { os << dest_; }
    };

    //! specialization of argument for string options or parameters
    struct ArgumentString : public Argument
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

        virtual const char * TypeName() const
        { return "string"; }

        //! "process" string argument just by storing it.
        virtual bool Process(int& argc, const char* const*& argv) {
            if (argc == 0) return false;
            dest_ = argv[0];
            --argc, ++argv;
            return true;
        }

        virtual void PrintValue(std::ostream& os) const
        { os << '"' << dest_ << '"'; }
    };

    //! specialization of argument for multiple string options or parameters
    struct ArgumentStringlist : public Argument
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

        virtual const char * TypeName() const
        { return "string list"; }

        //! "process" string argument just by storing it in vector.
        virtual bool Process(int& argc, const char* const*& argv) {
            if (argc == 0) return false;
            dest_.push_back(argv[0]);
            --argc, ++argv;
            return true;
        }

        virtual void PrintValue(std::ostream& os) const {
            os << '[';
            for (size_t i = 0; i < dest_.size(); ++i)
            {
                if (i != 0) os << ',';
                os << '"' << dest_[i] << '"';
            }
            os << ']';
        }
    };

protected:
    //! option and parameter list type
    typedef std::vector<Argument*> ArgumentList;

    //! list of options available
    ArgumentList optlist_;
    //! list of parameters, both required and optional
    ArgumentList paramlist_;

    //! formatting width for options, '-s, --switch <#>'
    int opt_maxlong_;
    //! formatting width for parameters, 'param <#>'
    int param_maxlong_;

    //! argv[0] for usage.
    const char* progname_;

    //! verbose processing of arguments
    bool verbose_process_;

    //! user set description of program, will be wrapped
    std::string description_;
    //! user set author of program, will be wrapped
    std::string author_;

    //! set line wrap length
    unsigned int linewrap_;

    //! maximum length of a TypeName() result
    static const int maxtypename_ = 16;

private:
    //! update maximum formatting width for new option
    void CalcOptMax(const Argument* arg) {
        opt_maxlong_ = std::max(static_cast<int>(arg->OptionText().size() + 2),
                                opt_maxlong_);
    }

    //! update maximum formatting width for new parameter
    void CalcParamMax(const Argument* arg) {
        param_maxlong_ = std::max(static_cast<int>(arg->ParamText().size() + 2),
                                  param_maxlong_);
    }

public:
    //! Wrap a long string at spaces into lines. Prefix is added
    //! unconditionally to each line. Lines are wrapped after wraplen
    //! characters if possible.
    static void
    OutputWrap(std::ostream& os, const std::string& text, size_t wraplen,
               size_t indent_first = 0, size_t indent_rest = 0,
               size_t current = 0, size_t indent_newline = 0);

public:
    //! Construct new command line parser
    CmdlineParser()
        : opt_maxlong_(8),
          param_maxlong_(8),
          progname_(NULL),
          verbose_process_(true),
          linewrap_(80)
    { }

    //! Delete all added arguments
    ~CmdlineParser() {
        for (size_t i = 0; i < optlist_.size(); ++i)
            delete optlist_[i];
        optlist_.clear();

        for (size_t i = 0; i < paramlist_.size(); ++i)
            delete paramlist_[i];
        paramlist_.clear();
    }

    //! Set description of program, text will be wrapped
    void SetDescription(const std::string& description) {
        description_ = description;
    }

    //! Set author of program, will be wrapped.
    void SetAuthor(const std::string& author) {
        author_ = author;
    }

    //! Set verbose processing of command line arguments
    void SetVerboseProcess(bool verbose_process) {
        verbose_process_ = verbose_process;
    }

    // ************************************************************************

    //! add boolean option flag -key, --longkey [keytype] with description and
    //! store to dest
    void AddFlag(char key, const std::string& longkey,
                 const std::string& keytype, bool& dest,
                 const std::string& desc) {
        optlist_.push_back(
            new ArgumentFlag(key, longkey, keytype, desc, false, dest));
        CalcOptMax(optlist_.back());
    }

    //! add signed integer option -key, --longkey [keytype] with description
    //! and store to dest
    void AddInt(char key, const std::string& longkey,
                const std::string& keytype, int& dest,
                const std::string& desc) {
        optlist_.push_back(
            new ArgumentInt(key, longkey, keytype, desc, false, dest));
        CalcOptMax(optlist_.back());
    }

    //! add unsigned integer option -key, --longkey [keytype] with description
    //! and store to dest
    void AddUInt(char key, const std::string& longkey,
                 const std::string& keytype, unsigned int& dest,
                 const std::string& desc) {
        optlist_.push_back(
            new ArgumentUInt(key, longkey, keytype, desc, false, dest));
        CalcOptMax(optlist_.back());
    }

    //! add double option -key, --longkey [keytype] with description and store
    //! to dest
    void AddDouble(char key, const std::string& longkey,
                   const std::string& keytype, double& dest,
                   const std::string& desc) {
        optlist_.push_back(
            new ArgumentDouble(key, longkey, keytype, desc, false, dest));
        CalcOptMax(optlist_.back());
    }

    //! add SI/IEC suffixes byte size option -key, --longkey [keytype] and
    //! store to 64-bit dest
    void AddBytes(char key, const std::string& longkey,
                  const std::string& keytype, uint32_t& dest,
                  const std::string& desc) {
        optlist_.push_back(
            new ArgumentBytes32(key, longkey, keytype, desc, false, dest));
        CalcOptMax(optlist_.back());
    }

    //! add SI/IEC suffixes byte size option -key, --longkey [keytype] and
    //! store to 64-bit dest
    void AddBytes(char key, const std::string& longkey,
                  const std::string& keytype, uint64_t& dest,
                  const std::string& desc) {
        optlist_.push_back(
            new ArgumentBytes64(key, longkey, keytype, desc, false, dest));
        CalcOptMax(optlist_.back());
    }

    //! add string option -key, --longkey [keytype] and store to dest
    void AddString(char key, const std::string& longkey,
                   const std::string& keytype, std::string& dest,
                   const std::string& desc) {
        optlist_.push_back(
            new ArgumentString(key, longkey, keytype, desc, false, dest));
        CalcOptMax(optlist_.back());
    }

    //! add string list option -key, --longkey [keytype] and store to dest
    void AddStringlist(char key, const std::string& longkey,
                       const std::string& keytype,
                       std::vector<std::string>& dest,
                       const std::string& desc) {
        optlist_.push_back(
            new ArgumentStringlist(key, longkey, keytype, desc, false, dest));
        CalcOptMax(optlist_.back());
    }

    //! add boolean option flag -key, --longkey with description and store to
    //! dest
    void AddFlag(char key, const std::string& longkey, bool& dest,
                 const std::string& desc)
    { return AddFlag(key, longkey, "", dest, desc); }

    //! add signed integer option -key, --longkey with description and store to
    //! dest
    void AddInt(char key, const std::string& longkey, int& dest,
                const std::string& desc)
    { return AddInt(key, longkey, "", dest, desc); }

    //! add unsigned integer option -key, --longkey [keytype] with description
    //! and store to dest
    void AddUInt(char key, const std::string& longkey, unsigned int& dest,
                 const std::string& desc)
    { return AddUInt(key, longkey, "", dest, desc); }

    //! add double option -key, --longkey [keytype] with description and store
    //! to dest
    void AddDouble(char key, const std::string& longkey, double& dest,
                   const std::string& desc)
    { return AddDouble(key, longkey, "", dest, desc); }

    //! add SI/IEC suffixes byte size option -key, --longkey [keytype] and
    //! store to 32-bit dest
    void AddBytes(char key, const std::string& longkey, uint32_t& dest,
                  const std::string& desc)
    { return AddBytes(key, longkey, "", dest, desc); }

    //! add SI/IEC suffixes byte size option -key, --longkey [keytype] and
    //! store to 64-bit dest
    void AddBytes(char key, const std::string& longkey, uint64_t& dest,
                  const std::string& desc)
    { return AddBytes(key, longkey, "", dest, desc); }

    //! add string option -key, --longkey [keytype] and store to dest
    void AddString(char key, const std::string& longkey, std::string& dest,
                   const std::string& desc)
    { return AddString(key, longkey, "", dest, desc); }

    //! add string list option -key, --longkey [keytype] and store to dest
    void AddStringlist(char key, const std::string& longkey,
                       std::vector<std::string>& dest, const std::string& desc)
    { return AddStringlist(key, longkey, "", dest, desc); }

    // ************************************************************************

    //! add signed integer parameter [name] with description and store to dest
    void AddParamInt(const std::string& name, int& dest,
                     const std::string& desc) {
        paramlist_.push_back(
            new ArgumentInt(0, name, "", desc, true, dest));
        CalcParamMax(paramlist_.back());
    }

    //! add unsigned integer parameter [name] with description and store to dest
    void AddParamUInt(const std::string& name, unsigned int& dest,
                      const std::string& desc) {
        paramlist_.push_back(
            new ArgumentUInt(0, name, "", desc, true, dest));
        CalcParamMax(paramlist_.back());
    }

    //! add double parameter [name] with description and store to dest
    void AddParamDouble(const std::string& name, double& dest,
                        const std::string& desc) {
        paramlist_.push_back(
            new ArgumentDouble(0, name, "", desc, true, dest));
        CalcParamMax(paramlist_.back());
    }

    //! add SI/IEC suffixes byte size parameter [name] with description and
    //! store to dest
    void AddParamBytes(const std::string& name, uint32_t& dest,
                       const std::string& desc) {
        paramlist_.push_back(
            new ArgumentBytes32(0, name, "", desc, true, dest));
        CalcParamMax(paramlist_.back());
    }

    //! add SI/IEC suffixes byte size parameter [name] with description and
    //! store to dest
    void AddParamBytes(const std::string& name, uint64_t& dest,
                       const std::string& desc) {
        paramlist_.push_back(
            new ArgumentBytes64(0, name, "", desc, true, dest));
        CalcParamMax(paramlist_.back());
    }

    //! add string parameter [name] with description and store to dest
    void AddParamString(const std::string& name, std::string& dest,
                        const std::string& desc) {
        paramlist_.push_back(
            new ArgumentString(0, name, "", desc, true, dest));
        CalcParamMax(paramlist_.back());
    }

    //! add string list parameter [name] with description and store to dest.
    //! \warning this parameter must be last, as it will gobble all non-option
    //! arguments!
    void AddParamStringlist(const std::string& name,
                            std::vector<std::string>& dest,
                            const std::string& desc) {
        paramlist_.push_back(
            new ArgumentStringlist(0, name, "", desc, true, dest));
        CalcParamMax(paramlist_.back());
    }

    // ************************************************************************

    //! add optional signed integer parameter [name] with description and store
    //! to dest
    void AddOptParamInt(const std::string& name, int& dest,
                        const std::string& desc) {
        paramlist_.push_back(
            new ArgumentInt(0, name, "", desc, false, dest));
        CalcParamMax(paramlist_.back());
    }

    //! add optional unsigned integer parameter [name] with description and
    //! store to dest
    void AddOptParamUInt(const std::string& name, unsigned int& dest,
                         const std::string& desc) {
        paramlist_.push_back(
            new ArgumentUInt(0, name, "", desc, false, dest));
        CalcParamMax(paramlist_.back());
    }

    //! add optional double parameter [name] with description and store to dest
    void AddOptParamDouble(const std::string& name, double& dest,
                           const std::string& desc) {
        paramlist_.push_back(
            new ArgumentDouble(0, name, "", desc, false, dest));
        CalcParamMax(paramlist_.back());
    }

    //! add optional SI/IEC suffixes byte size parameter [name] with
    //! description and store to dest
    void AddOptParamBytes(const std::string& name, uint32_t& dest,
                          const std::string& desc) {
        paramlist_.push_back(
            new ArgumentBytes32(0, name, "", desc, false, dest));
        CalcParamMax(paramlist_.back());
    }

    //! add optional SI/IEC suffixes byte size parameter [name] with
    //! description and store to dest
    void AddOptParamBytes(const std::string& name, uint64_t& dest,
                          const std::string& desc) {
        paramlist_.push_back(
            new ArgumentBytes64(0, name, "", desc, false, dest));
        CalcParamMax(paramlist_.back());
    }

    //! add optional string parameter [name] with description and store to dest
    void AddOptParamString(const std::string& name, std::string& dest,
                           const std::string& desc) {
        paramlist_.push_back(
            new ArgumentString(0, name, "", desc, false, dest));
        CalcParamMax(paramlist_.back());
    }

    //! add optional string parameter [name] with description and store to dest
    //! \warning this parameter must be last, as it will gobble all non-option
    //! arguments!
    void AddOptParamStringlist(const std::string& name,
                               std::vector<std::string>& dest,
                               const std::string& desc) {
        paramlist_.push_back(
            new ArgumentStringlist(0, name, "", desc, false, dest));
        CalcParamMax(paramlist_.back());
    }

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
