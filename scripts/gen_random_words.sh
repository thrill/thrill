#!/bin/bash
################################################################################
# scripts/gen_random_words.sh
#
# Generates random words, multiple words per line, space separated
#
# Part of Project Thrill.
#
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
################################################################################

MAX_LEN=32 #of word
BYTES=1048576 #1MB
MAX_WORDS_PER_LINE=10
WORDS=1 #is filled later

for (( c=0; c<$BYTES; WORDS=$(((RANDOM % MAX_WORDS_PER_LINE)+1)) ))
do
  for (( d=0; d<$WORDS; d++ ))
  do
    LEN=$(((RANDOM % $MAX_LEN) +1))
    ((c=$c+$LEN+1))
    echo -n "$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w $LEN | head -n 1) "
  done
  ((c=$c+1))
  echo " "
done

################################################################################
