This folder contains scripts to manage Thrill execution on the BW HPC or IC2 cluster.

Workflow at the moment is: 

 1. Log into BW HPC cluster login node
 1. Adjust invoke.sh
 2. Call invokeCluster.sh or invokeInteractive.sh

## invoke.sh

Main script. Wraps a Thrill execution on the BW HPC cluster.

This script autmatically pulls the hostlist and the local rank from the SLURM resource manager and starts a Thrill execution. The program to be executed and the port to be used on each worker is defined inside the script, as the `msub` submission tool does not allow parameters.

Please execute this script by using `msub` or one of the two auxillary scripts below.

## invokeCluster.sh

Auxillary script. 

This script, if executed on a BW HPC login node, submits invoke.sh as a task to the cluster. The number of hosts can be given as first argument. 

## invokeInteractive.sh

Auxillary script.

This script, if executed on a BW HPC login node, submits invoke.sh as a task to a single node on the cluster. Standard I/O streams are redirected so that the task is available interactively. 

## Other Scripts

Other scripts contain small components that are used by the scripts mentioned above. 
