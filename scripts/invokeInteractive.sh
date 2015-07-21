#! /bin/bash

msub -I -V -N test -l nodes=1:ppn=1,walltime=0:00:10 ./invoke.sh
