#! /bin/bash

NODES=${1-10}

msub -N test -l nodes=$NODES:ppn=1,walltime=0:00:01 ./invoke.sh
