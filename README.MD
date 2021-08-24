## Help

## Usage
* bashrcpath is optional argument, if not given there is a default bashrc file 
* Scratch directories for QM/poltype bashrc's need to be modified. It is best to copy bashrcs and make modifications as desired.
* First the dameon needs to be started without any arguments (to listen for incoming jobs)
* Next, jobinfofilepath is required argument for passing jobs to daemon
```
python path_to_manager.py --jobinfofilepath=jobinfofilepath
python path_to_manager.py --bashrcpath=bashrcpath --jobinfofilepath=jobinfofilepath

```

## Job Info File Path
* jobinfofilepath is a text file with possible formats below
* scrachdir and scratchspace are optional assignments for QM jobs, scratchspace needs GB unit after arg (--scratchspace=100GB)
* outputlogpath outputs status of job from daemon to outputlog file 
* if jobpath is not specified, default path is outputlogpath directory
* ram is optional input to specify how much ram is needed (in GB,--ram=100GB)
* numproc is optional input to specify number of processors needed
* For CPU job, specify as much info as possilbe (ram,scratch,numproc), the daemon uses this information to assign multiple jobs per node. If none of these are specified, then CPU jobs can only be 1 job per node (like GPU jobs).
* Commands in the job variable must be unique in order for daemon to recognize individually (for example poltype input command is usually "python ..../poltype.py" needs to be fixed with "cd path && python ..../poltype.py"

```
--job=command --outputlogpath=log --scratchdir=scratchdir --scratchspace=disk --ram=ram --numproc=numproc --jobpath=path_to_job
--job=command --outputlogpath=log --scratchdir=scratchdir --scratchspace=disk --jobpath=path_to_job
--job=command --outputlogpath=log --jobpath=path_to_job
--job=command --outputlogpath=log
```

* Example file contents for jobinfofilepath
```
--job=cd /home/bdw2292/PoltypeJobs/NewMethanolTest/methanol_test/qm-torsion && psi4 methanol-opt-1_1-2-254.psi4 methanol-opt-1_1-2-254.log --outputlogpath=/home/bdw2292/PoltypeJobs/NewMethanolTest/methanol_test/methanol-poltype.log --scratchdir=/scratch/bdw2292/Psi4-methanol --scratchspace=65GB
--job=cd /home/bdw2292/PoltypeJobs/NewMethanolTest/methanol_test/qm-torsion && psi4 methanol-opt-1_1-2-284.psi4 methanol-opt-1_1-2-284.log --outputlogpath=/home/bdw2292/PoltypeJobs/NewMethanolTest/methanol_test/methanol-poltype.log --scratchdir=/scratch/bdw2292/Psi4-methanol --scratchspace=65GB
```

* Example with dynamic_omm.x (needs to be in $PATH defined in bashrc files)
```
--job=dynamic_omm.x /home/bdw2292/Sims/Aniline/solvwaterboxmin.xyz -k /home/bdw2292/Sims/Aniline/aniline.key 16666 3 100 2 30 N > /home/bdw2292/Sims/Aniline/Aniline_30_16666.out --outputlogpath=/home/bdw2292/Sims/Aniline/Aniline_30_16666.out
```
* Example with jobpath= (changes directory to jobpath)
```
--job=dynamic_omm.x solvwaterboxproddyn.xyz -k aniline_lambda.key 166666 3 2 2 298 N > /home/bdw2292/Sims/Aniline/SolvSim/SolvSimEle1_Vdw1/SolvSimEle1_Vdw1.out --outputlogpath=/home/bdw2292/Sims/Aniline/proddynamicsjobs.txt --jobpath=/home/bdw2292/Sims/Aniline/SolvSim/SolvSimEle1_Vdw1
--job=dynamic_omm.x solvwaterboxproddyn.xyz -k aniline_lambda.key 166666 3 2 2 298 N > /home/bdw2292/Sims/Aniline/SolvSim/SolvSimEle.5_Vdw1/SolvSimEle.5_Vdw1.out --outputlogpath=/home/bdw2292/Sims/Aniline/proddynamicsjobs.txt --jobpath=/home/bdw2292/Sims/Aniline/SolvSim/SolvSimEle.5_Vdw1
```



## Hostname Node Topology
* Saved in nodes.txt
* include a \# in the line to ignore hostnames
* if CPUONLY in line (after hostname) then used only for cpujobs
* if GPUONLY in line (after hostname) then used only for gpujobs


## How it works
* Make sure to ssh into all hosts manually (so that all hosts are in your rsa keys and reachable by ssh), work_queue_workers are called via ssh on all hosts
* A while loop continuously checks each nodes list of assigned jobs and checks if the subprocess has exited on that node, then resubmits new jobs until the total number of jobs have been finished. 
* Multiple instances of the dameon are not allowed to run simultaneously. If the program is called while an instance is already running, the input jobs are just added to the existing job queue and then the second instance will exit. If the file daemon.pid exists, then the daemon will not start again (except to submit more jobs to queue, or to print which nodes are available).
```
