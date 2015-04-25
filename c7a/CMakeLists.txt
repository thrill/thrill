################################################################################
# c7a/CMakeLists.txt
#
# CMake file for c7arader library
################################################################################
file(GLOB_RECURSE SRCS
         RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/api/*.cpp
         ${CMAKE_CURRENT_SOURCE_DIR}/api/*.hpp
         ${CMAKE_CURRENT_SOURCE_DIR}/common/*.cpp
         ${CMAKE_CURRENT_SOURCE_DIR}/common/*.hpp
         ${CMAKE_CURRENT_SOURCE_DIR}/communication/*.cpp
         ${CMAKE_CURRENT_SOURCE_DIR}/communication/*.hpp
         ${CMAKE_CURRENT_SOURCE_DIR}/data/*.cpp
         ${CMAKE_CURRENT_SOURCE_DIR}/data/*.hpp
         ${CMAKE_CURRENT_SOURCE_DIR}/engine/*.cpp
         ${CMAKE_CURRENT_SOURCE_DIR}/engine/*.hpp
)

add_library(c7a STATIC ${SRCS})

################################################################################
