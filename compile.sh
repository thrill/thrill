#!/bin/sh
################################################################################
# compile.sh
#
# Part of Project Thrill - http://project-thrill.org
#
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

set -e

git submodule init
git submodule update

if [ ! -d "build" ]; then
    mkdir build
fi
cd build
cmake .. $@
make $MAKEOPTS -j8
ctest -V

################################################################################
