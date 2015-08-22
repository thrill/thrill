#!/bin/bash

set -e

git submodule init
git submodule update

if [ ! -d "build" ]; then
    mkdir build
fi
cd build
cmake .. $@
make $MAKEOPTS -j 8
ctest -V
