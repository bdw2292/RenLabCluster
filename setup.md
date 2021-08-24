## Setup for Cluster Submission

```
conda create --name clusterenv --yes
conda activate clusterenv
conda install -c conda-forge ndcctools --yes
conda install -c anaconda dill --yes
```
* Also install the two above python libaries in whichever bashrc is being used for the daemon (default /home/bdw2292/.allpurpose.bashrc)

