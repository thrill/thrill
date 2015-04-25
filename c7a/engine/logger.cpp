#include <stdexcept>
#include "logger.hpp"

using namespace std;

Logger* Logger::pInstance = nullptr;

mutex Logger::sMutex;

Logger& Logger::instance()
{
    lock_guard<mutex> guard(sMutex);
    if (pInstance == nullptr)
        pInstance = new Logger();
    return *pInstance;
}

Logger::Logger() { }

void Logger::log(const string& inMessage)
{
    lock_guard<mutex> guard(sMutex);
    std::cout << inMessage << std::endl;
}
