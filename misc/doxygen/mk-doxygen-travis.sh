#!/bin/bash -x
################################################################################
# misc/mk-doxygen-travis.sh
#
# Script to automatically generate doxyen documentation on Travis and upload it
# to Github Pages.
#
# Based on https://gist.github.com/vidavidorra/548ffbcdae99d752da02
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2017 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

set -e

GH_REPO_REF=github.com/thrill/thrill.github.io
# GH_REPO_TOKEN=<set-by-travis>

# Get the current github doxygen repo
rm -rf thrill.github.io
git clone https://git@${GH_REPO_REF} thrill.github.io

### Clean directory -- works with lots of files
echo 'Generating Doxygen code documentation...'
rm -rf doxygen-html
mkdir doxygen-html
time doxygen 2>&1 | tee doxygen-html/doxygen.log

if [ -f "doxygen-html/index.html" ]; then

    echo 'Uploading documentation to Github Pages...'
    rm -rf thrill.github.io/docs/master
    mkdir -p thrill.github.io/docs
    mv doxygen-html thrill.github.io/docs/master
    find thrill.github.io/docs/master -iname '*.md5' -delete

    pushd thrill.github.io
    git add --all

    git \
        -c user.name="mk-doxygen (Travis CI)" \
        -c user.email="thrill@panthema.net" \
        commit --quiet \
        -m "Deploy code docs to GitHub Pages Travis build: ${TRAVIS_BUILD_NUMBER}" \
        -m "Commit: ${TRAVIS_COMMIT}"

    git \
        -c push.default=simple \
        push --force \
        "https://${GH_REPO_TOKEN}@${GH_REPO_REF}" > /dev/null 2>&1

    popd

else
    echo '' >&2
    echo 'Warning: No documentation (html) files have been found!' >&2
    echo 'Warning: Not going to push the documentation to GitHub!' >&2
    exit 1
fi

################################################################################
