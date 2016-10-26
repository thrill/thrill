################################################################################
#
# - Try to find libs3 headers and libraries.
#
# Usage of this module as follows:
#
#     find_package(LibS3)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  LibS3_ROOT_DIR Set this variable to the root installation of
#                 libs3 if the module has problems finding
#                 the proper installation path.
#
# Variables defined by this module:
#
#  LibS3_FOUND             System has libs3 libs/headers
#  LibS3_LIBRARIES         The libs3 library/libraries
#  LibS3_INCLUDE_DIRS      The location of libs3 headers

find_path(LibS3_ROOT_DIR
  NAMES include/libs3.h
  )

find_library(LibS3_LIBRARIES
  NAMES s3
  HINTS ${LibS3_ROOT_DIR}/lib
  )

find_path(LibS3_INCLUDE_DIRS
  NAMES libs3.h
  HINTS ${LibS3_ROOT_DIR}/include
  )

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LibS3 DEFAULT_MSG
  LibS3_LIBRARIES
  LibS3_INCLUDE_DIRS
  )

mark_as_advanced(
  LibS3_ROOT_DIR
  LibS3_LIBRARIES
  LibS3_INCLUDE_DIRS
  )

################################################################################
