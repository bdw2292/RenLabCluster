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
conda create -n amoebamdpoltype python=3.7 psi4=1.4.1+cd00f19 -c psi4 --yes
conda activate amoebamdpoltype
conda install scipy=1.7.1 --yes
conda install matplotlib=3.4.3 --yes
conda install -c conda-forge openbabel=2.4.1 --yes
conda install -c conda-forge forcebalance --yes
conda install git pip=21.3.1 --yes
conda install -c conda-forge rdkit=2021.09.2 --yes
conda install -c conda-forge mdanalysis=2.0.0 --yes
conda install -c conda-forge svgutils=0.3.4 --yes
conda install -c conda-forge cairosvg=2.5.2 --yes
conda install -c conda-forge pymbar=3.0.5 --yes
conda install -c conda-forge mdtraj=1.9.6 --yes
conda install -c anaconda packaging=20.4 --yes
conda install -c conda-forge ndcctools=7.3.5 --yes
conda install -c anaconda dill=0.3.2 --yes
conda install -c conda-forge gspread=4.0.1 --yes
conda install -c conda-forge tqdm=4.62.3 --yes
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
else
  CARD=`nvidia-smi -q | grep 'Product Name' | tr -dc '0-9'`
  if [ $CARD > 1000 ]; then
     firstchar=${CARD:0:1}
     if [ $firstchar == 1 ] || [ $firstchar == 2 ]; then
       declare -i VALUE=102

     elif [ $firstchar == 3 ]; then
       declare -i VALUE=112

     else
       declare -i VALUE=0
       echo -e "\e[101mTinker9 not supported for CUDA $CARD\e[0m"
     fi
  elif [ $CARD == 100 ]; then
     declare -i VALUE=102

  else
     declare -i VALUE=0

     echo -e "\e[101mTinker9 not supported for CUDA $CARD\e[0m"
  fi
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
       export GPUDYNAMICS=True # need for forcebalance to know to use dynamic_gpu (modified forcebalance)
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

