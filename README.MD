## Help

## Usage
* Go to NOVA
* Activate conda envioronment for CCTools before calling manager
* Choose unique password that queue and work_queue_workers on cluster share. (NOTE: Currently setting password on queue and work_queue_worker seems to cause tasks to wait in queue and not get submitted).
* Choose unique projectname this instance of queue and work_queue_workers on cluster share

* run the follwing command to listen and wait for input job files
```
nohup python manager.py --projectname=projectname --password=password &
```
* Then after preparation of input job script is complete (see below), run the following command 
```
python manager.py --jobinfofilepath=jobinfo.txt 
```
* The port number is set by the controlhost listening script (manager.py)
* output.log, queuelogger.log for more information
* work_queue_status -M projectname
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
* GPU jobs are recognized via having _gpu in executable (like dynamic_gpu and bar_gpu)
* jobinfofilepath is a text file with possible formats below
* inputfilepaths are needed for CCtools to copy correct files to worker and run command
* outputfiles are needed for CCtools to copy correct files from worker to original job location
* absolutepathtobin is needed to give worker the absolute binary path to binary
* For QM jobs, scratchpath and scrachdir are both needed
* Make sure input and output filenames are unique (only if submitting many jobs to queue), since manager wil move jobs from worker back to manager and then manager will move to path in inputfilepaths
```
--job=command --scratchspace=disk --ram=ram --numproc=numproc --inputfilepaths=file1path,file2path --outputfiles=file1path,file2path --absolutepathtobin=path_to_bin --scratchpath=path_to_scratchdirs --scratchdir=scratchdir
--job=command --inputfilepaths=file1path,file2path --outputfiles=file1,file2 --absolutepathtobin=path_to_bin --scratchpath=path_to_scratchdirs --scratchdir=scratchdir

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
* Example manager initial input (on NOVA)
```
nohup python /home/bdw2292/ClusterDaemon/manager.py --projectname=RenLabCluster --password=secret &
```
* Example calling manager with input jobs
```
python /home/bdw2292/ClusterDaemon/manager.py --jobinfofilepath=/home/bdw2292/ClusterDaemon/jobinfo.txt
```
* Example file contents for jobinfofilepath
```
--job=psi4 water_3D-dma.psi4 water_3D-dma.log --scratchspace=100GB --ram=10GB --numproc=4 --inputfilepaths=/home/bdw2292/PoltypeJobs/SymmetryWater/water_3D-dma.psi4 --outputfiles=water_3D-dma.log --absolutepathtobin=/home/bdw2292/miniconda3/envs/poltype2/bin/psi4 --scratchpath=/scratch/bdw2292 --scratchdir=Psi4-water_3D

```

* Example with dynamic_gpu (needs to be in $PATH defined in bashrc files)



## Hostname Node Topology and Resouce Consumption
* Saved in nodeinfo.txt
* To generate nodeinfo.txt file run the following

```
python pingnodeswriteresources.py
```
* The columns within nodeinfo.txt are as follows       
```
node HASGPU Processors RAM Scratch ConsumptionRatio
```
* The resources listed are not total possible but current available as of the ping, so its good to run this before running manager.py, so has to add more safe restrictions to avoid potentially consuming too much resources
* Consumption ratio is the amount of total resources that a node can use for work_queue_worker, this file is read in when manager.py is called and then the worker_queue_worker on each host is restricted by the current available resource (by time running program to check)*consumptionratio for each resource. Then worker will always report static values to manager for job distribution.

* include a \# in the line to ignore hostnames in front (in nodeinfo.txt)
* If UNK is in column, this indicates program could not ping host for information and this will not be used for restricting the work_queue_worker on that host

## How it works
* Make sure to ssh into all hosts manually (so that all hosts are in your rsa keys and reachable by ssh), work_queue_workers are called via ssh on all hosts
* A while loop continuously checks new input jobs and constantly submit to queue.
* Multiple instances of the dameon are not allowed to run simultaneously. If the program is called while an instance is already running, the input jobs are just added to the existing job queue and then the second instance will exit. If the file daemon.pid exists, then the daemon will not start again.
```
