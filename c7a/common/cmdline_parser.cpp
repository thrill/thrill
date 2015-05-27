/*******************************************************************************
 * c7a/common/cmdline_parser.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/common/cmdline_parser.hpp>

#include <iomanip>
#include <cstring>
#include <string>
#include <algorithm>

namespace c7a {
namespace common {

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

void CmdlineParser::PrintUsage(std::ostream& os) {
    std::ios state(NULL);
    state.copyfmt(os);

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

    os.copyfmt(state);
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

bool CmdlineParser::Process(int argc, const char* const* argv, std::ostream& os) {
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
    std::ios state(NULL);
    state.copyfmt(os);

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

    os.copyfmt(state);
}

/******************************************************************************/

//! Parse a string like "343KB" or " 44 GiB " into the corresponding size in
//! bytes.
bool ParseSiIecUnits(const std::string& str, uint64_t& size, char def_unit) {
    char* endptr;
    size = strtoul(str.c_str(), &endptr, 10);
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
        switch (def_unit)
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
    static const char* SIendings[] = {
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
        << ' ' << SIendings[scale];
    return out.str();
}

//! Format number as something like 1 TiB
std::string FormatIecUnits(uint64_t number) {
    // may not overflow, std::numeric_limits<uint64_t>::max() == 16 EiB
    double multiplier = 1024.0;
    static const char* IECendings[] = {
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
        << ' ' << IECendings[scale];
    return out.str();
}

} // namespace common
} // namespace c7a

/******************************************************************************/
