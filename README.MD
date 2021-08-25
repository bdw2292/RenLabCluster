## Help

## Usage
* Go to NOVA
* run the follwing command to listen and wait for input job files
```
nohup python manager.py &
```
* Then after preparation of input job script is complete (see readme_help), run the following command
```
python manager.py --jobinfofilepath=jobinfo.txt
```
* The port number is set by the controlhost listening script (manager.py)
* output.log, transaction.log for more information
* work_queue_status -M RebLabCluster
* See https://cctools.readthedocs.io/en/stable/work_queue/ for more info
* bashrcpath is optional argument, if not given there is a default bashrc file 
* Scratch directories for QM/poltype bashrc's need to be modified. It is best to copy bashrcs and make modifications as desired.


## Job Info File Path
* scratchspace are optional assignments for QM jobs, scratchspace needs GB unit after arg (--scratchspace=100GB)
* outputlogpath outputs status of job from daemon to outputlog file 
* ram is optional input to specify how much ram is needed (in GB,--ram=100GB)
* numproc is optional input to specify number of processors needed
* For CPU job, specify as much info as possilbe (ram,scratch,numproc), the daemon uses this information to assign multiple jobs per node.
* If no resources are specified, or all resources are specified to be 0, then a single task from the category will consume a whole worker. 
* Commands in the job variable must be unique in order for daemon to recognize individually (for example poltype input command is usually "python ..../poltype.py" needs to be fixed with "cd path && python ..../poltype.py"
* GPU jobs are recognized via having _gpu in executable (like dynamic_gpu and bar_gpu)
* jobinfofilepath is a text file with possible formats below
```
--job=command --scratchspace=disk --ram=ram --numproc=numproc --jobpath=path_to_job
--job=command --scratchspace=disk --jobpath=path_to_job
--job=command --jobpath=path_to_job
--job=command 
```
* nohup.out will list all of the Task IDs associated with command strings
* CPU tasks are tagged with CPU and GPU tasks are tagged with GPU
* To kill tasks by TAG
```
python manager.py --canceltasktag=CPU
```

```
python manager.py --canceltasktag=GPU
```

* To kill task by ID
```
python manager.py --canceltaskid=taskid
```


* Example file contents for jobinfofilepath
```
--job=cd /home/bdw2292/PoltypeJobs/NewMethanolTest/methanol_test/qm-torsion && psi4 methanol-opt-1_1-2-254.psi4 methanol-opt-1_1-2-254.log --outputlogpath=/home/bdw2292/PoltypeJobs/NewMethanolTest/methanol_test/methanol-poltype.log --scratchdir=/scratch/bdw2292/Psi4-methanol --scratchspace=65GB
--job=cd /home/bdw2292/PoltypeJobs/NewMethanolTest/methanol_test/qm-torsion && psi4 methanol-opt-1_1-2-284.psi4 methanol-opt-1_1-2-284.log --outputlogpath=/home/bdw2292/PoltypeJobs/NewMethanolTest/methanol_test/methanol-poltype.log --scratchdir=/scratch/bdw2292/Psi4-methanol --scratchspace=65GB
```

* Example with dynamic_gpu (needs to be in $PATH defined in bashrc files)
```
--job=dynamic_gpu /home/bdw2292/Sims/Aniline/solvwaterboxmin.xyz -k /home/bdw2292/Sims/Aniline/aniline.key 16666 3 100 2 30 N > /home/bdw2292/Sims/Aniline/Aniline_30_16666.out --outputlogpath=/home/bdw2292/Sims/Aniline/Aniline_30_16666.out
```
* Example with jobpath= (changes directory to jobpath)
```
--job=dynamic_gpu solvwaterboxproddyn.xyz -k aniline_lambda.key 166666 3 2 2 298 N > /home/bdw2292/Sims/Aniline/SolvSim/SolvSimEle1_Vdw1/SolvSimEle1_Vdw1.out --outputlogpath=/home/bdw2292/Sims/Aniline/proddynamicsjobs.txt --jobpath=/home/bdw2292/Sims/Aniline/SolvSim/SolvSimEle1_Vdw1
--job=dynamic_gpu solvwaterboxproddyn.xyz -k aniline_lambda.key 166666 3 2 2 298 N > /home/bdw2292/Sims/Aniline/SolvSim/SolvSimEle.5_Vdw1/SolvSimEle.5_Vdw1.out --outputlogpath=/home/bdw2292/Sims/Aniline/proddynamicsjobs.txt --jobpath=/home/bdw2292/Sims/Aniline/SolvSim/SolvSimEle.5_Vdw1
```



## Hostname Node Topology
* Saved in nodes.txt
* include a \# in the line to ignore hostnames


## How it works
* Make sure to ssh into all hosts manually (so that all hosts are in your rsa keys and reachable by ssh), work_queue_workers are called via ssh on all hosts
* A while loop continuously checks new input jobs and constantly submit to queue.
* Multiple instances of the dameon are not allowed to run simultaneously. If the program is called while an instance is already running, the input jobs are just added to the existing job queue and then the second instance will exit. If the file daemon.pid exists, then the daemon will not start again.
```
