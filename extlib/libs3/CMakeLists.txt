################################################################################
# extlib/libs3/CMakeLists.txt
#
# Build libS3 library
################################################################################

add_definitions(-DLIBS3_VER_MAJOR="2")
add_definitions(-DLIBS3_VER_MINOR="0")
add_definitions(-DLIBS3_VER="2.0")

# find packages
find_package(CURL REQUIRED)
include_directories(SYSTEM ${CURL_INCLUDE_DIRS})

find_package(OpenSSL REQUIRED)
include_directories(SYSTEM ${OPENSSL_INCLUDE_DIR})

find_package(LibXml2 REQUIRED)
include_directories(${LIBXML2_INCLUDE_DIR})

if(MINGW)
  add_definitions(-DLIBXML_STATIC)
endif()

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/libs3/inc/")

set(LIBS3_SOURCES
  libs3/src/bucket.c
  libs3/src/bucket_metadata.c
  libs3/src/error_parser.c
  libs3/src/general.c
  libs3/src/multipart.c
  libs3/src/object.c
  libs3/src/request.c
  libs3/src/request_context.c
  libs3/src/response_headers_handler.c
  libs3/src/service.c
  libs3/src/service_access_logging.c
  libs3/src/simplexml.c
  libs3/src/util.c
  # libs3/src/mingw_functions.c
  # libs3/src/mingw_s3_functions.c
  )

add_library(s3 STATIC ${LIBS3_SOURCES})
target_link_libraries(s3
  ${LIBXML2_LIBRARIES} ${CURL_LIBRARIES} ${OPENSSL_LIBRARIES})

################################################################################
