/*******************************************************************************
 * c7a/common/logger.hpp
 *
 * Simple and less simple logging classes.
 ******************************************************************************/

#ifndef C7A_COMMON_LOGGER_HEADER
#define C7A_COMMON_LOGGER_HEADER

#include <iostream>
#include <sstream>
#include <stdexcept>
#include <mutex>

namespace c7a {

/*!
 * A simple logging class which outputs a std::endl during destruction.
 * Depending on the real parameter the output may be suppressed.
 */
class Logger
{
protected:
    //! real output or suppress
    bool m_real;

    //! the global mutex of logger and spacing logger
    static std::mutex mutex_;

    friend class SpacingLogger;

    //! lock the global mutex of spacing logger for serialized output in
    //! multi-threaded programs.
    std::unique_lock<std::mutex> lock_;

public:
    //! constructor: if real = false the output is suppressed.
    explicit Logger(bool real)
        : m_real(real), lock_(Logger::mutex_)
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

    //! lock the global mutex of spacing logger for serialized output in
    //! multi-threaded programs.
    std::unique_lock<std::mutex> lock_;

public:
    //! constructor: if real = false the output is suppressed.
    explicit SpacingLogger(bool real)
        : m_real(real), m_first(true), lock_(Logger::mutex_)
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
#define LOG ::c7a::Logger(debug)

// //! Override default output: never or always output log.
#define LOG0 ::c7a::Logger(false)
#define LOG1 ::c7a::Logger(true)

// //! Explicitly specify the condition for logging
#define LOGC(cond) ::c7a::Logger(cond)

//! Default logging method: output if the local debug variable is true.
#define sLOG ::c7a::SpacingLogger(debug)

//! Override default output: never or always output log.
#define sLOG0 ::c7a::SpacingLogger(false)
#define sLOG1 ::c7a::SpacingLogger(true)

//! Explicitly specify the condition for logging
#define sLOGC(cond) ::c7a::SpacingLogger(cond)

/******************************************************************************/

//! Instead of abort(), throw the output the message via an exception.
#define die(msg)                                            \
    do {                                                    \
        std::ostringstream oss;                             \
        oss << msg << " @ " << __FILE__ << ':' << __LINE__; \
        throw std::runtime_error(oss.str());                \
    } while (0)

//! Check condition X and die miserably if false. Same as assert() except this
//! is also active in Release mode.
#define die_unless(X) \
    do { if (!(X)) die("Assertion \"" #X "\" failed"); } while (0)

//! Check that X == Y or die miserably, but output the values of X and Y for
//! better debugging.
#define die_unequal(X, Y)                         \
    do {                                          \
        if ((X) != (Y))                           \
            die("Inequality: " #X " != " #Y " : " \
                "\"" << "\" != \"" << Y << "\""); \
    } while (0)

} // namespace c7a

#endif // !C7A_COMMON_LOGGER_HEADER

/******************************************************************************/
