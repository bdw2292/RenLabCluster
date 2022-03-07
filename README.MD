## Help

## Usage
* Go to bme-sun
* Activate conda envioronment for CCTools before calling manager
* Go to directory with manager.py
* Each user will get their own queue of workers
* Default --workerdir is /scratch
* Usernames are read in from usernames.txt and jobs are sent to /workerdir/username (default /scratch/username)
* --bashrcpath is optional argument, if not given there is a default bashrc file 
* Its recommended to have conda envioronments installed for Poltype, AMOEBAAnnihilator and ForceBalance in bashrc as well as Tinker GPU for different CUDA versions and CPU Tinker for different OS versions
* Scratch directories for QM/poltype bashrc's need to be modified. It is best to copy bashrcs and make modifications as desired.
* --backupmanager can be used to have manager run in backupmode, this will automatically act as manager if first manager dies (if the daemon.pid file disappears) and then read in the current waiting.log as input for queue.
* --timetokillworkers is optional argument. When a change in user allocated resources is detected, then program will wait timetokillworkers amount of time in minutes until workers on nodes with changed resources are killed and restarted with updated resource allocations (default 15 minutes)
* Run the follwing command to listen and wait for input job files
```
nohup python manager.py &
```
* poltype will prepare the inputs for you with keywords in inputfile (example externalapi=/home/bdw2292/ClusterDaemon/manager.py), see https://github.com/TinkerTools/poltype2
* AMOEBAAnnihilator will also prepare inputs for you with keywords in input file (example externalapi=/home/bdw2292/ClusterDaemon/manager.py) https://github.com/bdw2292/AMOEBAAnnihilator
* Then after preparation of input job script is complete (see below), run the following command 
* If --job is not specified, then job will be rejected.
* If --inputfilepaths is not specified, then job will be rejected.
* If --ram,--disk, or --numproc is not specified, then job will get rejected. These are needed to prevent all resources of certain type being allocated to a task (can happen when dont specify resources).
* If --ram less than 2GB is specified for any CPU job, then it will get rejected. This is to prevent too many jobs from being sent to a single compute node.
* If CPU job and doesnt have at least 1 cpu core specified, job will be rejected. 
* If poltype job doesnt have at least 10GB ram (default from poltype), then job will be rejected
```
python manager.py --jobinfofilepath=jobinfo.txt 
```
* Its best practice to make sure the filename for jobinfofilepath is unique, this file is copied to directory of daemon and then read in and deleted. So if multiple calls to daemon with same filename its possible one gets overwritten.
* The port number is set by the controlhost listening script (manager.py)
* Each user will have their own folder for logging information, the foldername is just the username
* See username_mainqueue_queuelogger.log for TaskID assignment, completetion and work_queue_worker submission
* See username_mainqueue_errorlogger.log for failed jobs
* See username_mainqueue_completed.log for jobs completed
* See username_mainqueue__waiting.log for jobs still waiting in queue
* See username_mainqueue__running.log for jobs still running
* See username_mainqueue__output.log for more details on communication between workers and queue
* See worker.debug under workerdir/username on each node for specific information about worker node
* work_queue_status -M username_mainqueue_projectname
* See https://cctools.readthedocs.io/en/stable/work_queue/ for more info


## Job Info File Path
* --ram specifies how much ram is needed (in GB,--ram=100GB, in MB, --ram=1MB)
* --numproc specifies number of processors needed for job
* --gpujob specfies to request 1 GPU for job (default is no GPU needed)
* Specify as much info as possilbe (ram,scratch,numproc,gpujob), the daemon uses this information to assign multiple jobs per node.
* Specify as much information as possible to be most efficient, numproc, disk, ram even for GPU jobs (which may require little ram or processors). By default, AMOEBAAnnihilator, will request --numproc=1, --disk=1MB and --ram=1MB and --gpujob. Thus taking up less cpu processors (internal count by cctools), if multiple cpu jobs are using processors on a compute node. 
* GPU jobs are recognized via having _gpu in executable (like dynamic_gpu and bar_gpu)
* jobinfofilepath is a text file with possible formats below
* inputfilepaths are needed for CCtools to copy correct files to worker and run command
* outputfilepaths are needed for CCtools to copy correct files from worker to original job location. In general inputfilepath and outputfilepath is not necesarrily the same (take tinker BAR for example)
* absolutepathtobin is needed only if the executable is not in PATH on the worked (there is a bashrc that is sourced by default) 
* For QM jobs, scratchpath is needed
* Make sure input and output filenames are unique (only if submitting many jobs to queue), since manager wil move jobs from worker back to manager directory (dont want to overwrite filenames that are same) and then manager will move to path in outputfilepath
* Poltype and AMOEBAAnnihilator handle this part for you, just need to add a keyword "externalapi", which is absolute path to manager.py 
```
--job=command --ram=ram --numproc=numproc --inputfilepaths=file1path,file2path --outputfilepaths=file1path,file2path --absolutepathtobin=path_to_bin --scratchpath=path_to_scratchdirs --disk=disk 

--job=command --inputfilepaths=file1path,file2path --outputfilepaths=file1,file2 --absolutepathtobin=path_to_bin --scratchpath=path_to_scratchdirs --disk=disk 
```

* CPU tasks are tagged with CPU and GPU tasks are tagged with GPU
* To kill tasks by TAG
```
python manager.py --canceltasktag=CPU --username=username
```

```
python manager.py --canceltasktag=GPU --username=username
```

* To kill task by ID
```
python manager.py --canceltaskid=taskid --username=username
```
* Example manager initial input (on bme-sun)
```
nohup python /home/bdw2292/ClusterDaemon/manager.py &
```
* Example calling manager with input jobs
```
python /home/bdw2292/ClusterDaemon/manager.py --jobinfofilepath=/home/bdw2292/ClusterDaemon/jobinfo.txt
```
* Example file contents for jobinfofilepath with Psi4/Gaussian
```
--job=psi4 water_3D-dma.psi4 water_3D-dma.log --scratchpath=/scratch/bdw2292/Psi4-water_3D --numproc=1 --ram=700MB --inputfilepaths=/home/bdw2292/PoltypeJobs/SymmetryWater/water_3D-dma.psi4 --outputfilepaths=/home/bdw2292/PoltypeJobs/SymmetryWater/water_3D-dma.log,/home/bdw2292/PoltypeJobs/SymmetryWater/water_3D-dma.fchk --absolutepathtobin=/home/bdw2292/miniconda3/envs/amoebamdpoltype/bin/psi4 --disk=100GB 
--job=psi4 water_3D-esp.psi4 water_3D-esp.log --scratchpath=/scratch/bdw2292/Psi4-water_3D --numproc=1 --ram=700MB --inputfilepaths=/home/bdw2292/PoltypeJobs/SymmetryWater/water_3D-esp.psi4,/home/bdw2292/PoltypeJobs/SymmetryWater/grid.dat --outputfilepaths=/home/bdw2292/PoltypeJobs/SymmetryWater/water_3D-esp.log,/home/bdw2292/PoltypeJobs/SymmetryWater/water_3D-esp.fchk,/home/bdw2292/PoltypeJobs/SymmetryWater/grid_esp.dat --absolutepathtobin=/home/bdw2292/miniconda3/envs/amoebamdpoltype/bin/psi4 --disk=100GB  
--job=GAUSS_SCRDIR=/scratch/bdw2292/Gau-water_3D /opt/g09gh/gaussian/g09/g09 water_3D-opt_1.com --scratchpath=/scratch/bdw2292/Gau-water_3D --numproc=1 --ram=700MB --inputfilepaths=/home/bdw2292/PoltypeJobs/SymmetryWater/water_3D-opt_1.com --outputfilepaths=/home/bdw2292/PoltypeJobs/SymmetryWater/water_3D-opt_1.log --absolutepathtobin=/opt/g09gh/gaussian/g09/g09 --disk=100GB  

```

* Example poltype files inputs
* Need the cd at the front. Can only do this for nodes physically connected to bme-sun, since file system is shared amongst all nodes, this means that not all input and outputfiles need to be specified in order to run a job on a remote worker (as long as filesystem is shared)
```
--job=cd /home/bdw2292/PoltypeJobs/SymmetryWater ; python /home/bdw2292/poltype2/PoltypeModules/poltype.py --ram=10GB --numproc=1 --disk=0GB --inputfilepaths=/home/bdw2292/PoltypeJobs/SymmetryWater/poltype.ini
```
* Example AMOEBA Annihilator inputs
```
--job=cd /work/bdw2292/PoltypePaperMolecules/p-Dibromobenzene/p-Dibromobenzene_Simulation && python /home/bdw2292/AMOEBAAnnihhilator/AMOEBAAnnihilatorModules/amoebaannihilator.py --numproc=1 --ram=10GB --disk=0GB --inputfilepaths=/work/bdw2292/PoltypePaperMolecules/p-Dibromobenzene/p-Dibromobenzene_Simulation/AMOEBA.ini

```

* Example with dynamic_gpu (needs to be in $PATH defined in bashrc files)
```
--job=dynamic_gpu solvwaterboxequil.xyz -k solvfinal_config.key 16665 2 1 2 50 N > TestRESPAGasDefaults_SolvSim_50_16665_NVT.out --numproc=0 --gpujob --ram=1MB --disk=1MB --inputfilepaths=/work/bdw2292/TestRESPAGasDefaults/solvwaterboxequil.xyz,/work/bdw2292/TestRESPAGasDefaults/solvfinal_config.key,/work/bdw2292/TestRESPAGasDefaults/amoebabio18.prm --outputfilepaths=/work/bdw2292/TestRESPAGasDefaults/TestRESPAGasDefaults_SolvSim_50_16665_NVT.out,/work/bdw2292/TestRESPAGasDefaults/solvwaterboxequil.arc,/work/bdw2292/TestRESPAGasDefaults/solvwaterboxequil.dyn
--job=dynamic solvwaterboxproddyn.xyz -k solvwaterboxproddyn.key 2000000 0.1 2 2 300  > SolvSimEle1_Vdw1.out --numproc=1 --ram=3GB --disk=0GB --inputfilepaths=/work/bdw2292/TestSTOCHGasDefaults/SolvSim/SolvSimEle1_Vdw1/solvwaterboxproddyn.xyz,/work/bdw2292/TestSTOCHGasDefaults/SolvSim/SolvSimEle1_Vdw1/solvwaterboxproddyn.key,/work/bdw2292/TestSTOCHGasDefaults/amoebabio18.prm,/work/bdw2292/TestSTOCHGasDefaults/SolvSim/SolvSimEle1_Vdw1/solvwaterboxproddyn.arc,/work/bdw2292/TestSTOCHGasDefaults/SolvSim/SolvSimEle1_Vdw1/solvwaterboxproddyn.dyn --outputfilepaths=/work/bdw2292/TestSTOCHGasDefaults/SolvSim/SolvSimEle1_Vdw1/SolvSimEle1_Vdw1.out,/work/bdw2292/TestSTOCHGasDefaults/SolvSim/SolvSimEle1_Vdw1/solvwaterboxproddyn.arc,/work/bdw2292/TestSTOCHGasDefaults/SolvSim/SolvSimEle1_Vdw1/solvwaterboxproddyn.dyn

```

## Hostname Node Topology and Resouce Consumption
* Saved in nodeinfo.txt
* To generate nodeinfo.txt file run the following

```
python pingnodes.py --bashrcpath=path_to_bashrc --coreconsumptionratio=.7 --ramconsumptionratio=.7 --diskconsumptionratio=.7 --mincardtype=1000 
```
* If node has less than 15GB memory, then consumption ratio for cores and ram is 20%
* If GPU node, then consumption ratio for cores and ram is 50% and disk is 60%, otherwise 70%
* analyze_gpu is run on each node with bashrcpath to test if envioronment for that node is correct
* The columns within nodeinfo.txt are as follows       
```
node HASGPU CARDTYPE Processors RAM Scratch CPUConsumptionRatio RAMConsumptionRatio UseForceBalance CPUUsername GPUUsername
```
* Put NOFB under UseForceBalance column if you dont want the GPU cards on that node to use ForceBalance.
* If you do want to use ForceBalance, put a job name under the UseForceBalance column for all nodes allocated to that ForceBalance job. Such as FBJob1, FBJob2 etc...
* The resources listed are total possible,
* To be safe  its good to run this before running manager.py, GPU cards could die or changes to cluster hardware made
* Consumption ratio is the amount of total resources that a node can use for work_queue_worker, this file is read in when manager.py is called and then the worker_queue_worker on each host is restricted by the total available resource *consumptionratio for each resource. Then worker will always report static values to manager for job distribution.

* include a \# in the line to ignore hostnames in front (in nodeinfo.txt)
* If UNK is in column, this indicates program could not ping host for information and this will not be used for restricting the work_queue_worker on that host
* Every few minutes manager.py will read Google spreadsheet and update nodeinfo.txt with usernames assigned to nodes/GPU cards. Then manager.py will read nodeinfo.txt, then block workers that are not allowed in user-specific queue. If previoiusly user was not allowed to use a node but then now is, then manager.py will unblock that host for user specific queue. https://docs.google.com/spreadsheets/d/1EOlUwFpdNU2uBZ5XrHSvZnRYCUCOw5tCSkFisTm3big/edit#gid=979716892
* Emails will be sent from renlabclusterreport@gmail.com to users informing them they have 15 minutes to make changes to spreadsheet when any change in resource allocation is detected. Usernames must have queues enabled (they must be using the RenLabCluster daemon) in order for emails to be sent. By default all users in userstoemail.txt are enabled. 
* The spreadsheet is accessed via credentials.json and the title called 'Ren lab cluster usage'. Do NOT change any of these or column order.
* If the same username is in all cards for a node under CPUUsername column, then only that user will be alloewd to use CPU resources on that node. If a single value is left blank (on any of the cards for that node,even if there is no GPU), then anyuser will be allowed access to CPU resources on that node.
* Each username is counted for each card for each node under the GPUUsername column, the total cards for that node and username are set on the work_queue_worker for that user to use. 
* Every few minutes if the allowed cpu resources or total gpu cards allowed for a username changes (from changes on spreadsheet), then the work_queue_worker on that node will be killed and a new one will be started with the updated criteria. 
* Usernames that are allowed queues are saved in usernametoemail.txt
* Make sure to ssh into all hosts manually (so that all hosts are in your rsa keys and reachable by ssh), work_queue_workers are called via ssh on all hosts
* A while loop continuously checks new input jobs and constantly submit to queue.
* Multiple instances of the dameon are not allowed to run simultaneously. If the program is called while an instance is already running, the input jobs are just added to the existing job queue and then the second instance will exit. If the file daemon.pid exists, then the daemon will not start again.

## Creating Google Service Account
* https://www.youtube.com/watch?v=4ssigWmExak
1. Go to console.developers.google.com
2. Sign in
3. Start a new project
4. Activate google sheet APIs
5. Go to service accounts and create service account
6. Create/Enter email address for reading the spreadsheet
7. Give editor permissions to project
8. Go to spreadsheet and click on share
9. Enter email address that was created
10. Click on the service account created
11. Scroll down and add key, click create new key
12. Select json
13. Rename file to credentials.json and put in the NodeTopolgy directory




## Test Daemon
* Run test AMOEBAAnnihilator job under TestMinAnnihilatorExample

## Monitor Resource Usage
* Run the command bellow
* Every two days send email for users that are not using their CPU/GPU node reservations on spreadsheet
* Assumes nodeinfo.txt already exists and usernames are being written to file in real time
```
nohup python pingnodes.py --monitorresourceusage &
```

## Updating manager without loss of service
* If the current manager has a backup mode manager running, first kill the main manager (via PID in daemon.pid), then remove the daemon.pid file. This will cause the backup manager to become the new manager. Then you will want to run the updated mangaer.py in backupmode. After that, again kill the main manager (which still running under old manager.py) and delete the daemon.pid. This will cause the updated manager.py that was running in backupmode to become the primary manager. Then you can submit another manager in backup mode.

* If the current manager does not have a backup manager running, submit the updated manager.py in backup mode. Then kill the primary manager (PID in daemon.pid), and remove the daemon.pid file. This will cause the updated manager in backup mode to become the primary manager. Then add another manager in backup mode.

## Todo
* Seperate the CPU disk and GPU disk resources, otherwise everytime CPU resource changes, since they share the same disk, then daemon detects change in GPU disk resource and kill CPU and GPU worker
* Parse /opt/quota.sun and /opt/quota.nova.home, then report if quota is getting close to filling (send emails and log information)
