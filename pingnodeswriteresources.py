import os
import sys
import subprocess
from tqdm import tqdm
import re
import time

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
            if '#' not in line:
                nodelist.append(node)

        temp.close()
    return nodelist


def PingNodesAndDetermineNodeInfo(nodelist):
    gpunodes=[] 
    nodetototalram={}
    nodetototalcpu={}
    nodetototalscratch={}
    nodetocardcount={}
    nodetocardtype={}
    for nodeidx in tqdm(range(len(nodelist)),desc='Pinging nodes'):
        node=nodelist[nodeidx]
        cudaversion,cardcount,cardtype=CheckGPUStats(node)
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
            nodetocardtype[node]=cardtype
        else:
            nodetocardcount[node]=0
            nodetocardtype[node]='UNK'

    return gpunodes,nodetototalram,nodetototalcpu,nodetototalscratch,nodetocardcount,nodetocardtype

def CheckGPUStats(node):
    cmdstr='nvidia-smi'
    job='ssh %s "%s"'%(node,cmdstr)
    try:
        output = subprocess.check_output(job, stderr=subprocess.STDOUT,shell=True, timeout=10)
        output=ConvertOutput(output)
        nodedead=False
    except:
        nodedead=True
    cudaversion=None
    cardcount=0
    cardtype=None
    if nodedead==False:
        lines=output.split('\n')
        for line in lines:
            linesplit=line.split()
            if 'CUDA' in line:
                cudaversion=linesplit[8]
            if len(linesplit)==15:
                cardcount+=1

    cmdstr='nvidia-smi -q'
    job='ssh %s "%s"'%(node,cmdstr)
    try:
        output = subprocess.check_output(job,stderr=subprocess.STDOUT, shell=True,timeout=10)
        output=ConvertOutput(output)
    except:
        nodedead=True
    if nodedead==False:
        lines=output.split('\n')
        for line in lines:
            if "Product Name" in line:
                linesplit=line.split()
                for e in linesplit:
                    if e.isdigit():
                        cardtype=e

    return cudaversion,cardcount,cardtype


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
    output=True
    job='ssh %s "%s"'%(node,cmdstr)
    try: # if it has output that means this process is running
        output=subprocess.check_output(job,stderr=subprocess.STDOUT,shell=True,timeout=2.5)
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
    try:
        output = subprocess.check_output(job,stderr=subprocess.STDOUT,shell=True, timeout=10)
        output=ConvertOutput(output)
        nodedead=False
    except:
        nodedead=True
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



def WriteOutNodeInfo(filename,gpunodes,nodetototalram,nodetototalcpu,nodetototalscratch,nodelist,consumptionratio,nodetocardcount,nodetocardtype,mincardtype):
    temp=open(filename,'w')
    columns='#node'+' '+'HASGPU'+' '+'CARDTYPE'+' '+'GPUCARDS'+' '+'Processors'+' '+'RAM(MB)'+' '+'Scratch(MB)'+' '+'ConsumptionRatio'+'\n'
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
        cardtype=nodetocardtype[node]
        if cardtype!='UNK' and cardtype!=None:
            value=float(cardtype)
            if value<mincardtype:
                cardcount='0'
                node='#'+node # temp until --gpus 0 fixed
        if cardtype==None:
            cardtype='UNK'
        string=node+' '+gpustring+' '+cardtype+' '+cardcount+' '+nproc+' '+ram+' '+scratch+' '+consumptionratio+'\n'
        temp.write(string)
    temp.close()

while True:
    mincardtype=1000
    nodelistfilepath='nodes.txt'
    consumptionratio='.8'
    nodelist=ReadNodeList(nodelistfilepath)
    gpunodes,nodetototalram,nodetototalcpu,nodetototalscratch,nodetocardcount,nodetocardtype=PingNodesAndDetermineNodeInfo(nodelist)
    filename='nodeinfo.txt'
    WriteOutNodeInfo(filename,gpunodes,nodetototalram,nodetototalcpu,nodetototalscratch,nodelist,consumptionratio,nodetocardcount,nodetocardtype,mincardtype)
    time.sleep(60*5)
