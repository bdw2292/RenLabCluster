## Setup for Cluster Submission

```
conda create --name clusterenv --yes
conda activate clusterenv
conda install -c conda-forge ndcctools --yes
conda install -c anaconda dill --yes
```
* Also install the two above python libaries in whichever bashrc is being used for the daemon (default /home/bdw2292/.allpurpose.bashrc)
* Go to NOVA
* run the follwing command to listen and wait for input job files
```
nohup python manager.py &
```
* Then after preparation of input job script is complete (see readme_help), run the following command
```

```

* The port number is set by the controlhost listening script (manager.py)
* work_queue_status for information
* See https://cctools.readthedocs.io/en/stable/work_queue/ for more info
