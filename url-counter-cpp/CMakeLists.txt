cmake_minimum_required(VERSION 2.8)
project(UrlCounter)

set(CMAKE_CXX_STANDARD 11)
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
CHECK_CXX_COMPILER_FLAG("-std=c++0x" COMPILER_SUPPORTS_CXX0X)

include_directories(thirdparty/fmt-4.1.0)

add_subdirectory(thirdparty/fmt-4.1.0 EXCLUDE_FROM_ALL)

add_executable(url_counter
        hash/murmurhash.h
        hash/murmurhash.cc
        main.cpp)
target_link_libraries(url_counter fmt-header-only)
