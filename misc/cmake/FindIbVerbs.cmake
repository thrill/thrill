# - Try to find IB Verbs
# Once done this will define
#  IbVerbs_FOUND - System has IB Verbs
#  IbVerbs_INCLUDE_DIRS - The IB Verbs include directories
#  IbVerbs_LIBRARIES - The libraries needed to use IB Verbs

find_path(IbVerbs_INCLUDE_DIR verbs.h
  HINTS /usr/local/include/infiniband /usr/include/infiniband)

find_library(IbVerbs_LIBRARY NAMES ibverbs
  PATHS /usr/local/lib /usr/lib)

set(IbVerbs_INCLUDE_DIRS ${IbVerbs_INCLUDE_DIR})
set(IbVerbs_LIBRARIES ${IbVerbs_LIBRARY})

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set IbVerbs_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(
  IbVerbs DEFAULT_MSG IbVerbs_INCLUDE_DIR IbVerbs_LIBRARY)

mark_as_advanced(IbVerbs_INCLUDE_DIR IbVerbs_LIBRARY)

################################################################################
