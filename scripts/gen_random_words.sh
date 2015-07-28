# Generates random words, multiple words per line, space separated

#!/bin/bash
MAX_LEN=32 #of word
LINES=10
MAX_WORDS_PER_LINE=10

WORDS=1 #is filled later
for (( c=1; c<$LINES; c++, WORDS=$(((RANDOM % MAX_WORDS_PER_LINE)+1)) ))
do
  for (( d=0; d<$WORDS; d++ ))
  do
    echo -n "$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w $(((RANDOM % $MAX_LEN) +1)) | head -n 1) "
  done
  echo ""
done
