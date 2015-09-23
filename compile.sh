#!/bin/sh

set -e

git submodule init
git submodule update

if [ ! -d "build" ]; then
    mkdir build
fi
cd build
cmake .. $@ -DCMAKE_BUILD_TYPE=Release
make $MAKEOPTS -j8
ctest -V
