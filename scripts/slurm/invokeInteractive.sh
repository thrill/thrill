#!/bin/bash

msub -I -V -N test -l nodes=1:ppn=1,walltime=0:01:00 ./invoke.sh
