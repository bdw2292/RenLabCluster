## Setup for Cluster Submission
* Make sure you can ssh into all nodes, so rsa keys exist for each node
* Install miniconda or anaconda
* Clone repo 
```
git clone https://github.com/bdw2292/RenLabCluster.git
```
* Create conda from yml file or manually line by line
```
conda env create -f clusterenv.yml
```
* Manual install
```
conda create --name clusterenv --yes
conda activate clusterenv
conda install -c conda-forge ndcctools=7.3.5 --yes
conda install -c anaconda dill=0.3.2 --yes
conda install -c conda-forge gspread=4.0.1 --yes
conda install -c conda-forge tqdm=4.62.3 --yes
```


## Setup for bashrc
* Install from yml file
```
conda env create -f amoebamdpoltype.yml
```

* Manual install
```
conda deactivate
conda create -n amoebamdpoltype python=3.7 --yes
conda activate amoebamdpoltype
conda install -c psi4 psi4 --yes
conda install scipy --yes
conda install matplotlib --yes
conda install -c conda-forge openbabel=2.4.1 --yes
conda install -c conda-forge forcebalance --yes
conda install git pip --yes
conda install -c conda-forge rdkit --yes
conda install -c conda-forge mdanalysis --yes
conda install -c conda-forge svgutils --yes
conda install -c conda-forge cairosvg --yes
conda install -c conda-forge pymbar --yes
conda install -c conda-forge mdtraj --yes
conda install -c anaconda packaging --yes
conda install -c conda-forge ndcctools --yes
conda install -c anaconda dill --yes
conda install -c conda-forge gspread --yes
conda install -c anaconda psutil --yes
```

* Bashrc example
* Dont forget to replace username in scratch with your username
```
OSVERSION=`cat /etc/system-release`
place="${OSVERSION//[!0-9]/}"
OSVERSION=${place::1}
VAL=`nvidia-smi &> /dev/null; echo $?`

if [ $VAL != 0 ]; then
  echo -e "\e[101mCUDA utility not installed on `hostname`\e[0m"
  declare -i VALUE=0
else

  for dir in /usr/local/*;
  do
      SUB='cuda'
      if [[ "$dir" == *"$SUB"* ]]; then
        VALUE=`echo $dir | tr -dc '0-9'`
      fi
  done

  if [ $VALUE == 102 ] ; then
       export TINKERGPUDIR=/home/liuchw/Softwares/tinkers/Tinker9-latest/build_cuda10.2/
       function dynamic_gpu () {
           local TINKER9=/home/liuchw/Softwares/tinkers/Tinker9-latest/build_cuda10.2
       
           $TINKER9/dynamic9.sh $@ 
       }
       export -f dynamic_gpu
       
       function bar_gpu () {
           local TINKER9=/home/liuchw/Softwares/tinkers/Tinker9-latest/build_cuda10.2
       
           $TINKER9/bar9.sh $@  
       }
       export -f bar_gpu
       
       function analyze_gpu () {
           local TINKER9=/home/liuchw/Softwares/tinkers/Tinker9-latest/build_cuda10.2
       
           $TINKER9/analyze9.sh $@
       }
       export -f analyze_gpu
       export GPUDYNAMICS=True # need this for ForceBalance to let program know to use dynamic_gpu
  elif [ $VALUE == 112 ] ; then
       export TINKERGPUDIR=/home/liuchw/Softwares/tinkers/Tinker9-latest/build_cuda11.2
       function dynamic_gpu () {
           local TINKER9=/home/liuchw/Softwares/tinkers/Tinker9-latest/build_cuda11.2
       
           $TINKER9/dynamic9.sh $@
       }
       export -f dynamic_gpu
       
       function bar_gpu () {
           local TINKER9=/home/liuchw/Softwares/tinkers/Tinker9-latest/build_cuda11.2
       
           $TINKER9/bar9.sh $@
       }
       export -f bar_gpu
       
       function analyze_gpu () {
           local TINKER9=/home/liuchw/Softwares/tinkers/Tinker9-latest/build_cuda11.2
       
           $TINKER9/analyze9.sh $@
       }
       export -f analyze_gpu
       
       export GPUDYNAMICS=True
  fi

 


fi
if [ $OSVERSION == 8 ] ; then
    export PATH=/home/bdw2292/NewestTinkerOS8/bin/:$PATH
    export TINKERPATH=/home/bdw2292/NewestTinkerOS8/bin/ # this is for ForceBalance to recognize tinker path in different nodes
     
else
    export PATH=/home/bdw2292/NewestTinker/bin/:$PATH
    export TINKERPATH=/home/bdw2292/NewestTinker/bin/

fi


export myusername=`whoami`
conda activate amoebamdpoltype
export g09root=/opt/g09gh/gaussian
source $g09root/g09/bsd/g09.profile
export GAUSS_SCRDIR=/scratch/$myusername/
export GDMADIR=/opt/gdma/gdma-2.3.3/bin/
export PATH=/opt/gdma/gdma-2.3.3/bin/:$PATH
export PSI_SCRATCH=/scratch/$myusername/

```

