#!/bin/sh
################################################################################
# compile.sh
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

set -ex

git submodule init
git submodule update

CMAKE_OPTS="-DTHRILL_BUILD_EXAMPLES=ON -DTHRILL_BUILD_TESTS=ON"

# try to find a modern C++ compiler
set +x
COMPILERS_LIST=
for MAJOR in `seq 9 -1 4`; do
    COMPILERS_LIST="$COMPILERS_LIST g++-$MAJOR"
    for MINOR in `seq 9 -1 0`; do
        COMPILERS_LIST="$COMPILERS_LIST g++-$MAJOR.$MINOR"
        for PATCH in `seq 9 -1 0`; do
            COMPILERS_LIST="$COMPILERS_LIST g++-$MAJOR.$MINOR.$PATCH"
        done
    done
done
#echo $COMPILERS_LIST

for CMD in $COMPILERS_LIST; do
    if command -v "$CMD" > /dev/null; then
        CMAKE_OPTS="$CMAKE_OPTS -DCMAKE_CXX_COMPILER=$CMD"
        break
    fi
done
set -x

# detect number of cores
if [ -e /proc/cpuinfo ]; then
    CORES=$(grep -c ^processor /proc/cpuinfo)
elif [ "$(uname)" == "Darwin" ]; then
    CORES=$(sysctl -n hw.ncpu)
else
    CORES=4
fi
MAKEOPTS=-j$CORES

if [ ! -d "build" ]; then
    mkdir build
fi
cd build
cmake .. $CMAKE_OPTS $@
make $MAKEOPTS
ctest -V

################################################################################
