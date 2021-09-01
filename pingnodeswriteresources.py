import os
import sys
import subprocess
from tqdm import tqdm
import re

def ReadNodeList(nodelistfilepath):
    nodelist=[]
    if os.path.isfile(nodelistfilepath):
        temp=open(nodelistfilepath,'r')
        results=temp.readlines()
        for line in results:
            linesplit=line.split()
            if len(linesplit)<1:
                continue
            newline=line.replace('\n','')
            linesplit=newline.split()
            node=linesplit[0]
            node=node.replace('#','')
            nodelist.append(node)

        temp.close()
    return nodelist


def PingNodesAndDetermineNodeInfo(nodelist):
    gpunodes=[] 
    nodetototalram={}
    nodetototalcpu={}
    nodetototalscratch={}
    nodetocardcount={}
    for nodeidx in tqdm(range(len(nodelist)),desc='Pinging nodes'):
        node=nodelist[nodeidx]
        cudaversion,cardcount=CheckGPUStats(node)
        nproc=CheckTotalCPU(node)
        currentproc=CheckCurrentCPUUsage(node)
        if type(nproc)==int and type(currentproc)==int:
            nproc=nproc-currentproc

        if nproc==0:
            nproc='UNK'
        ram=CheckRAM(node)
        if ram==False:
            ram='UNK'
        scratch=CheckScratchSpace(node)
        if scratch==False:
            scratch='UNK'
        nodetototalram[node]=ram
        nodetototalcpu[node]=nproc
        nodetototalscratch[node]=scratch
        if cudaversion!=None:
            gpunodes.append(node)
            nodetocardcount[node]=cardcount
        else:
            nodetocardcount[node]=0

    return gpunodes,nodetototalram,nodetototalcpu,nodetototalscratch,nodetocardcount

def CheckGPUStats(node):
    cmdstr='nvidia-smi'
    job='ssh %s "%s"'%(node,cmdstr)
    p = subprocess.Popen(job, stdout=subprocess.PIPE,shell=True)
    nodedead,output=CheckForDeadNode(p,node)
    cudaversion=None
    cardcount=0
    nodedead=False
    if nodedead==False:
        lines=output.split('\n')
        for line in lines:
            linesplit=line.split()
            if 'CUDA' in line:
                cudaversion=linesplit[8]
            if len(linesplit)==15:
                cardcount+=1
    return cudaversion,cardcount

def CheckForDeadNode(process,node):
    nodedead=False
    output, err = process.communicate()
    output=ConvertOutput(output)    
    if process.returncode != 0:
        if err!=None:
            err=ConvertOutput(err)
            nodedead=True
    return nodedead,output

def ConvertOutput(output):
    if output!=None:
        output=output.rstrip()
        if type(output)!=str:
            output=output.decode("utf-8")
    return output


def CheckTotalCPU(node):
    totalproc=False
    cmdstr='nproc'
    output=CheckOutputFromExternalNode(node,cmdstr)
    if output!=False:
        lines=output.split('\n')
        firstline=lines[0]
        firstlinesplit=firstline.split()
        totalproc=int(firstlinesplit[0])
    return totalproc

def CheckOutputFromExternalNode(node,cmdstr):
    nodetimeout=10
    output=True
    job='ssh %s "%s"'%(node,cmdstr)
    try: # if it has output that means this process is running
        output=subprocess.check_output(job,stderr=subprocess.STDOUT,shell=True,timeout=nodetimeout)
        output=ConvertOutput(output)
         
    except: #if it fails, that means no process with the program is running or node is dead/unreachable
         output=False
    return output 

def CheckRAM(node):
    ram=False
    total=False
    cmdstr='free -g'
    output=CheckOutputFromExternalNode(node,cmdstr)
    if output!=False:
        lines=output.split('\n')
        for line in lines:
            linesplit=line.split()
            if 'Mem' in line:
                ram=float(linesplit[3])
                total=float(linesplit[1])
                break
            elif 'buffers/cache' in line:
                ram=float(linesplit[3])
                used=float(linesplit[2])
                total=ram+used
                break
        ram=str(ram)+'GB'
        ram=ConvertMemoryToMBValue(ram)

    return ram

def ConvertMemoryToMBValue(scratch):
    availspace,availunit=SplitScratch(scratch)
    if availunit=='M' or availunit=='MB':
        availspace=float(availspace)
    elif availunit=='T' or availunit=='TB':
        availspace=float(availspace)*1000000
    elif availunit=='G' or availunit=='GB':
        availspace=float(availspace)*1000
    return int(availspace)
 

def CheckScratchSpace(node):
    cmdstr='df -h'
    scratchavail=False
    scratchtotal=False
    output=CheckOutputFromExternalNode(node,cmdstr)
    if output!=False:
        lines=output.split('\n')[1:-1]
        d={}
        for line in lines:
            linesplit=line.split()
            if len(linesplit)==5 or len(linesplit)==6:
                avail = re.split('\s+', line)[3]
                mount = re.split('\s+', line)[5]
                d[mount] = avail
        if '/scratch' in d.keys(): 
            scratchavail=d['/scratch']
        else:
            scratchavail='0G'
        if scratchavail==None:
            scratchavail='0G'
        else:
            try:
                scratchavail=ConvertMemoryToMBValue(scratchavail)
            except:
                scratchavail=0

        


    return int(scratchavail)

def SplitScratch(string):
    for eidx in range(len(string)):
        e=string[eidx]
        if not e.isdigit() and e!='.':
            index=eidx
            break
    space=string[:index]
    diskunit=string[index]
    return space,diskunit

def CheckCurrentCPUUsage(node):
    currentproc=False
    filepath=os.path.join(os.getcwd(),'topoutput.txt')
    cmdstr='top -b -n 1 > '+filepath   
    job='ssh %s "%s"'%(node,cmdstr)
    p = subprocess.Popen(job, stdout=subprocess.PIPE,shell=True)
    nodedead,output=CheckForDeadNode(p,node)
    if nodedead==False:
        if os.path.isfile(filepath):
            temp=open(filepath,'r')
            results=temp.readlines()
            temp.close()
            procsum=0
            for line in results:
                linesplit=line.split()
                if len(linesplit)==12:
                   proc=linesplit[8]
                   if proc.isnumeric():
                       proc=float(proc)/100
                       procsum+=proc
            currentproc=int(procsum)
            if os.path.isfile(filepath):
                os.remove(filepath)
    return currentproc



def WriteOutNodeInfo(filename,gpunodes,nodetototalram,nodetototalcpu,nodetototalscratch,nodelist,consumptionratio,nodetocardcount):
    temp=open(filename,'w')
    columns='#node'+' '+'HASGPU'+' '+'GPUCARDS'+' '+'Processors'+' '+'RAM(MB)'+' '+'Scratch(MB)'+' '+'ConsumptionRatio'+'\n'
    temp.write(columns)
    for node in nodelist:
        hasgpu=False
        if node in gpunodes:
            hasgpu=True
        if hasgpu==True:
            gpustring='GPU'
        else:
            gpustring='NOGPU'
        ram=str(nodetototalram[node])
        nproc=str(nodetototalcpu[node])
        scratch=str(nodetototalscratch[node])
        cardcount=str(nodetocardcount[node])
        string=node+' '+gpustring+' '+cardcount+' '+nproc+' '+ram+' '+scratch+' '+consumptionratio+'\n'
        temp.write(string)
    temp.close()

nodelistfilepath='nodes.txt'
consumptionratio='.8'
nodelist=ReadNodeList(nodelistfilepath)
gpunodes,nodetototalram,nodetototalcpu,nodetototalscratch,nodetocardcount=PingNodesAndDetermineNodeInfo(nodelist)
filename='nodeinfo.txt'
WriteOutNodeInfo(filename,gpunodes,nodetototalram,nodetototalcpu,nodetototalscratch,nodelist,consumptionratio,nodetocardcount)
