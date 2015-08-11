#!/bin/bash

# Simply use (undocumented) expandnodes tool to expand the hostlist. 
expandnodes "$SLURM_JOB_NODELIST"
