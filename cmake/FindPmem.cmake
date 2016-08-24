# Find the libpmem library.
# Output variables:
#  PMEM_INCLUDE_DIR : e.g., /usr/include/.
#  PMEM_LIBRARY     : Library path of libpmem
#  PMEM_FOUND       : True if found.
FIND_PATH(PMEM_INCLUDE_DIR NAME libpmem.h
  HINTS $ENV{HOME}/local/include /opt/local/include /usr/local/include /usr/include)

FIND_LIBRARY(PMEM_LIBRARY NAME pmem
  HINTS $ENV{HOME}/local/lib64 $ENV{HOME}/local/lib /usr/local/lib64 /usr/local/lib /opt/local/lib64 /opt/local/lib /usr/lib64 /usr/lib
)

IF (PMEM_INCLUDE_DIR AND PMEM_LIBRARY)
    SET(PMEM_FOUND TRUE)
    MESSAGE(STATUS "Found libpmem: inc=${PMEM_INCLUDE_DIR}, lib=${PMEM_LIBRARY}")
ELSE ()
    SET(PMEM_FOUND FALSE)
    MESSAGE(STATUS "ERROR: libpmem not found.")
    MESSAGE(STATUS "Download and install from http://pmem.io/nvml/")
ENDIF ()
