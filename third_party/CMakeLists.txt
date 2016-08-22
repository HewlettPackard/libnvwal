# these are third party code, we don't care warning.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w -fPIC")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -w")

set(BUILD_TESTING OFF) # Disable testcases in these third party libraries

# We use gflags for command line argument parsing.
# boost::program_options provides equivalent functionalities, but it's not header-only!
# Rather, we statically link to gflags. So far, we are boost-free.
set(BUILD_SHARED_LIBS OFF) # we use gflags as static library only
set(BUILD_STATIC_LIBS ON)
add_subdirectory(gflags-2.1.1)

# For testing, we use Google-testing. It's active, provides more functionalities,
# and has no linking issues.
set(BUILD_SHARED_LIBS ON) # To reduce linking time, we now use shared-library version of gtest,
add_subdirectory(gtest-1.7.0)

add_subdirectory(glog-0.3.4)

# tinyxml2 not used so far, but probably will
# set(BUILD_STATIC_LIBS ON) # this creates tinyxml2static library
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -w") # same as glog
# add_subdirectory(tinyxml2-2.1.0)
