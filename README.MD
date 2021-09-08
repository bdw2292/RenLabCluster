## Help

## Usage
* Go to NOVA
* Activate conda envioronment for CCTools before calling manager
* Go to directory with manager.py
* Choose unique password that queue and work_queue_workers on cluster share. (NOTE: Currently setting password on queue and work_queue_worker seems to cause tasks to wait in queue and not get submitted). Dont need to set password currently.
* Choose unique projectname this instance of queue and work_queue_workers on cluster share

* run the follwing command to listen and wait for input job files
* Please choose port number for queue default (6123) but if default already in use then will crash
* Default workerdir is /scratch
* Need to specify username to send jobs to /scratch/username
* bashrcpath is optional argument, if not given there is a default bashrc file 
* Scratch directories for QM/poltype bashrc's need to be modified. It is best to copy bashrcs and make modifications as desired.
* Variable --backupmanager can be used to have manager run in backupmode, this will automatically act as manager if first manager dies (if the daemon.pid file disappears) and then read in the current waiting.log as input for queue. 

```
nohup python manager.py --projectname=projectname --password=password --portnumber=portnumber --workerdir=/scratch --username=username --bashrcpath=path_to_bashrc &
```
* Then after preparation of input job script is complete (see below), run the following command 
* poltype will prepare the inputs for you with keyword in inputfile (externalapi=/home/bdw2292/ClusterDaemon/manager.py), see https://github.com/TinkerTools/poltype2
* AMOEBAAnnihilator will also prepare inputs for you with keyword in input file (externalapi=/home/bdw2292/ClusterDaemon/manager.py) https://github.com/bdw2292/AMOEBAAnnihilator
```
python manager.py --jobinfofilepath=jobinfo.txt 
```
* Its best practice to make sure the filename for jobinfofilepath is unique, this file is copied to directory of daemon and then read in and deleted. So if multiple calls to daemon with same filename its possible one gets overwritten.
* The port number is set by the controlhost listening script (manager.py)
* See queuelogger.log for TaskID assignment, completetion and work_queue_worker submission
* See errorlogger.log for failed jobs
* See completed.log for jobs completed
* See waiting.log for jobs still waiting in queue
* See running.log for jobs still running
* See output.log for more details on communication between workers and queue
* See worker.debug under workerdir/username on each node for specific information about worker node
* work_queue_status -M projectname
* See https://cctools.readthedocs.io/en/stable/work_queue/ for more info


## Job Info File Path
* outputlogpath outputs status of job from daemon to outputlog file 
* ram is optional input to specify how much ram is needed (in GB,--ram=100GB)
* numproc is optional input to specify number of processors needed
* For CPU job, specify as much info as possilbe (ram,scratch,numproc), the daemon uses this information to assign multiple jobs per node.
* If no resources are specified, or all resources are specified to be 0, then a single task from the category will consume a whole worker. Specify as much information as possible to be most efficient, numproc, disk, ram 
* GPU jobs are recognized via having _gpu in executable (like dynamic_gpu and bar_gpu)
* jobinfofilepath is a text file with possible formats below
* inputfilepaths are needed for CCtools to copy correct files to worker and run command
* outputfilepaths are needed for CCtools to copy correct files from worker to original job location. In general inputfilepath and outputfilepath is not necesarrily the same (take tinker BAR for example)
* absolutepathtobin is needed only if the executable is not in PATH on the worked (there is a bashrc that is sourced by default) 
* For QM jobs, scratchpath is needed
* Make sure input and output filenames are unique (only if submitting many jobs to queue), since manager wil move jobs from worker back to manager directory (dont want to overwrite filenames that are same) and then manager will move to path in outputfilepath
```
--job=command --ram=ram --numproc=numproc --inputfilepaths=file1path,file2path --outputfilepaths=file1path,file2path --absolutepathtobin=path_to_bin --scratchpath=path_to_scratchdirs --disk=disk 

--job=command --inputfilepaths=file1path,file2path --outputfilepaths=file1,file2 --absolutepathtobin=path_to_bin --scratchpath=path_to_scratchdirs --disk=disk 
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
* Example file contents for jobinfofilepath with Psi4/Gaussian
```
--job=psi4 water_3D-dma.psi4 water_3D-dma.log --scratchpath=/scratch/bdw2292/Psi4-water_3D --numproc=1 --ram=700MB --inputfilepaths=/home/bdw2292/PoltypeJobs/SymmetryWater/water_3D-dma.psi4 --outputfilepaths=/home/bdw2292/PoltypeJobs/SymmetryWater/water_3D-dma.log,/home/bdw2292/PoltypeJobs/SymmetryWater/water_3D-dma.fchk --absolutepathtobin=/home/bdw2292/miniconda3/envs/amoebamdpoltype/bin/psi4  
--job=psi4 water_3D-esp.psi4 water_3D-esp.log --scratchpath=/scratch/bdw2292/Psi4-water_3D --numproc=1 --ram=700MB --inputfilepaths=/home/bdw2292/PoltypeJobs/SymmetryWater/water_3D-esp.psi4,/home/bdw2292/PoltypeJobs/SymmetryWater/grid.dat --outputfilepaths=/home/bdw2292/PoltypeJobs/SymmetryWater/water_3D-esp.log,/home/bdw2292/PoltypeJobs/SymmetryWater/water_3D-esp.fchk,/home/bdw2292/PoltypeJobs/SymmetryWater/grid_esp.dat --absolutepathtobin=/home/bdw2292/miniconda3/envs/amoebamdpoltype/bin/psi4 
--job=GAUSS_SCRDIR=/scratch/bdw2292/Gau-water_3D /opt/g09gh/gaussian/g09/g09 water_3D-opt_1.com --scratchpath=/scratch/bdw2292/Gau-water_3D --numproc=1 --ram=700MB --inputfilepaths=/home/bdw2292/PoltypeJobs/SymmetryWater/water_3D-opt_1.com --outputfilepaths=/home/bdw2292/PoltypeJobs/SymmetryWater/water_3D-opt_1.log --absolutepathtobin=/opt/g09gh/gaussian/g09/g09 
```

* Example poltype files inputs
* Need the cd at the front. Can only do this for nodes physically connected to NOVA, since file system is shared amongst all nodes, this means that not all input and outputfiles need to be specified in order to run a job on a remote worker (as long as filesystem is shared)
```
--job=cd /home/bdw2292/PoltypeJobs/SymmetryWater ; python /home/bdw2292/poltype2/PoltypeModules/poltype.py --ram=10GB --numproc=4  --inputfilepaths=/home/bdw2292/PoltypeJobs/SymmetryWater/poltype.ini
```
* Example AMOEBA Annihilator inputs
```
--job=cd /work/bdw2292/PoltypePaperMolecules/p-Dibromobenzene/p-Dibromobenzene_Simulation && python /home/bdw2292/AMOEBAAnnihhilator/AMOEBAAnnihilatorModules/amoebaannihilator.py --numproc=1 --inputfilepaths=/work/bdw2292/PoltypePaperMolecules/p-Dibromobenzene/p-Dibromobenzene_Simulation/AMOEBA.ini

```

* Example with dynamic_gpu (needs to be in $PATH defined in bashrc files)
```
--job=dynamic_gpu solvwaterboxequil.xyz -k solvfinal_config.key 16665 2 1 2 50 N > TestRESPAGasDefaults_SolvSim_50_16665_NVT.out --numproc=1 --inputfilepaths=/work/bdw2292/TestRESPAGasDefaults/solvwaterboxequil.xyz,/work/bdw2292/TestRESPAGasDefaults/solvfinal_config.key,/work/bdw2292/TestRESPAGasDefaults/amoebabio18.prm --outputfilepaths=/work/bdw2292/TestRESPAGasDefaults/TestRESPAGasDefaults_SolvSim_50_16665_NVT.out,/work/bdw2292/TestRESPAGasDefaults/solvwaterboxequil.arc,/work/bdw2292/TestRESPAGasDefaults/solvwaterboxequil.dyn
--job=dynamic solvwaterboxproddyn.xyz -k solvwaterboxproddyn.key 2000000 0.1 2 2 300  > SolvSimEle1_Vdw1.out --numproc=1 --inputfilepaths=/work/bdw2292/TestSTOCHGasDefaults/SolvSim/SolvSimEle1_Vdw1/solvwaterboxproddyn.xyz,/work/bdw2292/TestSTOCHGasDefaults/SolvSim/SolvSimEle1_Vdw1/solvwaterboxproddyn.key,/work/bdw2292/TestSTOCHGasDefaults/amoebabio18.prm,/work/bdw2292/TestSTOCHGasDefaults/SolvSim/SolvSimEle1_Vdw1/solvwaterboxproddyn.arc,/work/bdw2292/TestSTOCHGasDefaults/SolvSim/SolvSimEle1_Vdw1/solvwaterboxproddyn.dyn --outputfilepaths=/work/bdw2292/TestSTOCHGasDefaults/SolvSim/SolvSimEle1_Vdw1/SolvSimEle1_Vdw1.out,/work/bdw2292/TestSTOCHGasDefaults/SolvSim/SolvSimEle1_Vdw1/solvwaterboxproddyn.arc,/work/bdw2292/TestSTOCHGasDefaults/SolvSim/SolvSimEle1_Vdw1/solvwaterboxproddyn.dyn

```


## Hostname Node Topology and Resouce Consumption
* Saved in nodeinfo.txt
* To generate nodeinfo.txt file run the following

```
nohup python pingnodeswriteresources.py &
```
* This continously runs every 5 minutes, then pings each node for information like resources (including if GPU is present). Need to do this so in case GPU card dies, then manager.py will not send jobs to nodes it thinks has GPU (like previoiusly card working but now its not...)
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

## Todo
* Add option to tell manager how much estimated disk space for MD jobs (arc files get large), then can enforce disk space limits (not important right now)
