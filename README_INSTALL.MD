## Setup for Cluster Submission
```
git clone https://github.com/bdw2292/RenLabCluster.git
conda create --name clusterenv --yes
conda activate clusterenv
conda install -c conda-forge ndcctools --yes
conda install -c anaconda dill --yes
conda install -c conda-forge gspread --yes
conda install -c conda-forge tqdm --yes
```

## Setup for bashrc
```
conda create -n amoebamdpoltype python=3.6 psi4=1.3.2+ecbda83 -c psi4 --yes
conda activate poltype
conda install scipy --yes
conda install matplotlib --yes
conda install -c conda-forge openbabel=2.4.1 --yes
conda install git pip --yes
conda install -c conda-forge rdkit --yes
conda install -c conda-forge mdanalysis --yes
conda install -c conda-forge svgutils --yes
conda install -c conda-forge cairosvg --yes
conda install -c conda-forge pymbar --yes
conda install matplotlib --yes
conda install -c conda-forge openbabel=2.4.1 --yes
conda install -c conda-forge mdtraj --yes
conda install tqdm --yes
conda install -c conda-forge rdkit --yes
conda install -c anaconda packaging
```

* Bashrc example
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
       export GPUDYNAMICS=True
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
else
    export PATH=/home/bdw2292/NewestTinker/bin/:$PATH
fi

conda activate amoebamdpoltype
export g09root=/opt/g09gh/gaussian
source $g09root/g09/bsd/g09.profile
export GAUSS_SCRDIR=/scratch/bdw2292/
export GDMADIR=/opt/gdma/gdma-2.3.3/bin/
export PATH=/opt/gdma/gdma-2.3.3/bin/:$PATH
export PSI_SCRATCH=/scratch/bdw2292/
```

