This folder contains scripts to run Thrill programs on HPC clusters managed by Slurm.

1. Log into HPC cluster login node.
2. Build the Thrill programs on the login node.
3. Submit batch jobs, which run `slurm/invoke.sh <thrill-program> [args]`.
4. Alternatively, if the job submission system does not pass arguments, adapt `invoke_wrap.sh` to run a Thrill program and submit it.

Examples:

* on ic2 cluster:
`job_submit -t 30 -m 4000 -c d -p 16/2 ~/thrill/run/slurm/invoke.sh ~/thrill/build/examples/page_rank/page_rank_run -g 100000 output.txt 5`

* on UniBWCluster:
`msub invoke_wrap.sh`

## invoke.sh

Main script. Launches a Thrill program on a HPC cluster using the Slurm tool `srun`.

This script autmatically pulls the hostlist and configuration from the SLURM resource environment and starts a Thrill execution by launching one process per host with the appropriate number of worker threads.

## invoke_wrap.sh

Due to the shortcomings of the MOAB submission system, we have to write wrappers, which then call `invoke.sh` with the Thrill application.

## Other Scripts

Other scripts contain small components that are used by the scripts mentioned above. These may have to be adapted to other HPC schedulers.
`map_ib0.awk` maps the hostnames to the appropriate Infiniband IPs which Thrill should use to communicate.
