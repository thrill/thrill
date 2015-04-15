/*******************************************************************************
 * tbt/common/logger.hpp
 *
 * Simple and less simple logging classes.
 ******************************************************************************/

#ifndef TBT_COMMON_LOGGER_HEADER
#define TBT_COMMON_LOGGER_HEADER

#include <iostream>
#include <sstream>
#include <stdexcept>

/*!
 * A simple logging class which outputs a std::endl during destruction.
 * Depending on the real parameter the output may be suppressed.
 */
class Logger
{
protected:
    //! real output or suppress
    bool m_real;

public:
    //! constructor: if real = false the output is suppressed.
    Logger(bool real)
        : m_real(real)
    { }

    //! output any type, including io manipulators
    template <typename AnyType>
    Logger& operator << (AnyType at)
    {
        if (m_real) std::cout << at;
        return *this;
    }

    //! destructor: output a newline
    ~Logger()
    {
        if (m_real) std::cout << std::endl;
    }
};

/*!
 * A logging class which outputs spaces between elements pushed via
 * operator<<. Depending on the real parameter the output may be suppressed.
 */
class SpacingLogger
{
protected:
    //! real output or suppress
    bool m_real;
    //! true until the first element it outputted.
    bool m_first;

public:
    //! constructor: if real = false the output is suppressed.
    SpacingLogger(bool real)
        : m_real(real), m_first(true)
    { }

    //! output any type, including io manipulators
    template <typename AnyType>
    SpacingLogger& operator << (AnyType at)
    {
        if (!m_real) return *this;

        if (!m_first) std::cout << ' ';
        else m_first = false;

        std::cout << at;

        return *this;
    }

    //! destructor: output a newline
    ~SpacingLogger()
    {
        if (m_real)
            std::cout << std::endl;
    }
};

//! global debug flag.
static const bool debug = true;

// //! Default logging method: output if the local debug variable is true.
// #define LOG Logger(debug)

// //! Override default output: never or always output log.
// #define LOG0 Logger(false)
// #define LOG1 Logger(true)

// //! Explicitly specify the condition for logging
// #define LOGC(cond) Logger(cond)

//! Default logging method: output if the local debug variable is true.
#define sLOG SpacingLogger(debug)

//! Override default output: never or always output log.
#define sLOG0 SpacingLogger(false)
#define sLOG1 SpacingLogger(true)

//! Explicitly specify the condition for logging
#define sLOGC(cond) SpacingLogger(cond)

/******************************************************************************/

//! Instead of abort(), throw the output the message via an exception.
#define die(msg)                                            \
    do {                                                    \
        std::ostringstream oss;                             \
        oss << msg << " @ " << __FILE__ << ':' << __LINE__; \
        throw (std::runtime_error(oss.str()));              \
    } while (0)

//! Check condition X and die miserably if false. Same as assert() except this
//! is also active in Release mode.
#define die_unless(X) \
    do { if (!(X)) die("Assertion \"" #X "\" failed"); } while (0)

//! Check that X == Y or die miserably, but output the values of X and Y for
//! better debugging.
#define die_unequal(X, Y)                          \
    do {                                           \
        if ((X) != (Y))                            \
            die("Inequality: " #X " != " #Y " : "  \
                "\"" << "\" != \"" << Y << "\"");  \
    } while (0)

#endif // !TBT_COMMON_LOGGER_HEADER

/******************************************************************************/
