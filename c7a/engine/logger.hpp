///////////////////////////
// Taken from: http://www.cplusplus.com/forum/unices/132540/
///////////////////////////

// Logger.h
#include <iostream>
#include <string>
#include <mutex>

// Definition of a multithread safe singleton logger class
class Logger
{
public:
    // Returns a reference to the singleton Logger object
    static Logger& instance();

    // Logs a single message at the given log level
    void log(const std::string& inMessage);

protected:
    // Static variable for the one-and-only instance
    static Logger* pInstance;

private:
    Logger();
    static std::mutex sMutex;
};