import os
import sys
import time
import getopt
import subprocess
import shutil
import logging

nodelistfilepath='nodeinfo.txt'
envpath='/home/bdw2292/.allpurpose.bashrc'
jobinfofilepath=None
loggerfile='submitallnode.log'

opts, xargs = getopt.getopt(sys.argv[1:],'',["bashrcpath=","jobinfofilepath="])
for o, a in opts:
    if o in ("--bashrcpath"):
        envpath=a
    elif o in ("--jobinfofilepath"):
        jobinfofilepath=a


def ReadNodeList(nodelistfilepath):
    nodelist=[]
    nodetousableram={}
    nodetohasgpu={}
    nodetousabledisk={}
    nodetousableproc={}
    if os.path.isfile(nodelistfilepath):
        temp=open(nodelistfilepath,'r')
        results=temp.readlines()
        for line in results:
            linesplit=line.split()
            if len(linesplit)<1:
                continue
            newline=line.replace('\n','')
            if '#' not in line:
                linesplit=newline.split()
                node=linesplit[0]
                nodelist.append(node)
                hasgpu=linesplit[1]
                proc=linesplit[2]
                ram=linesplit[3]
                scratch=linesplit[4]
                consumratio=float(linesplit[5])
                if hasgpu=='GPU':
                    nodetohasgpu[node]=True
                else:
                    nodetohasgpu[node]=False
                if proc!='UNK':
                    proc=str(int(int(proc)*consumratio))
                if ram!='UNK':
                    ram=str(int(int(ram)*consumratio))
                if scratch!='UNK':
                    scratch=str(int(int(scratch)*consumratio))
                nodetousableram[node]=ram
                nodetousabledisk[node]=scratch
                nodetousableproc[node]=proc              

        temp.close()
    if len(nodelist)==0:
        raise ValueError('Node list has no nodes to read from')
    return nodelist,nodetohasgpu,nodetousableproc,nodetousableram,nodetousabledisk


def WriteToLogFile(string):
    now = time.strftime("%c",time.localtime())
    string=now+' '+string+'\n'
    logging.info(string)


def WaitForInputJobs():
    jobinfo={}
    foundinputjobs=False
    WriteToLogFile('Waiting for input jobs')
    while foundinputjobs==False:
        jobinfo,foundinputjobs=CheckForInputJobs(jobinfo)   
        time.sleep(5)
    return jobinfo

def CheckForInputJobs(jobinfo):
    files=os.listdir()
    array=[]
    foundinputjobs=False
    for f in files:
        if 'submittoall' in f:
            foundinputjobs=True
            jobinfo=ReadJobInfoFromFile(jobinfo,f)
            array.append(f)
    for f in array:
        os.remove(f)
    return jobinfo,foundinputjobs


def ReadJobInfoFromFile(jobinfo,filename):
    if os.path.isfile(filename):
        temp=open(filename,'r')
        results=temp.readlines()
        temp.close()
        for line in results:
            split=line.split()
            if len(split)==0:
                continue
            cmdstr,jobfilepath=ParseJobInfo(line)
            array=['jobfilepath']
            for key in array:
                if key not in jobinfo.keys():
                    jobinfo[key]={}
            job=tuple([cmdstr,tuple(jobfilepath)])
            jobinfo['jobfilepath'][job]=jobfilepath

    return jobinfo


def ParseJobInfo(line):
    linesplit=line.split('--')[1:]
    linesplit=[e.rstrip() for e in linesplit]
    job=None
    jobfilepath=None
    for line in linesplit:
        if "job=" in line:
            job=line.replace('job=','')
        if "jobfilepath" in line:
            jobfilepath=line.replace('jobfilepath=','')

    return job,jobfilepath

def CallJob(node,envpath,cmdstr,processes,jobpath,pidtojobpath):
    cmdstr = 'ssh %s "source %s ; cd %s ; %s"' %(str(node),envpath,jobpath,cmdstr)
    WriteToLogFile('Calling: '+cmdstr)
    process = subprocess.Popen(cmdstr, stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
    pid=process.pid
    pidtojobpath[pid]=jobpath
    WriteToLogFile('PID '+str(pid)+' is assigned to '+cmdstr)
    processes.append(process)
    return processes,pidtojobpath

def CallJobs(nodetojobs,envpath,processes,jobinfo,pidtojobpath):
    for node,jobs in nodetojobs.items():
        for job in jobs:
            jobpath=jobinfo['jobfilepath'][job]
            cmdstr=job[0]
            processes,pidtojobpath=CallJob(node,envpath,cmdstr,processes,jobpath,pidtojobpath)
    return processes,pidtojobpath


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def DistributeEvenly(nodelist,jobinfo):
    nodetojobs={}
    jobtojobfilepath=jobinfo['jobfilepath']
    jobs=list(jobtojobfilepath.keys())
    for node in nodelist:
        nodetojobs[node]=[]
    if len(jobs)<len(nodelist):
        for i in range(len(jobs)):
            job=jobs[i]
            node=nodelist[i]
            nodetojobs[node].append(job)
    else:
       ratio=int(len(jobs)/len(nodelist))
       even=list(chunks(jobs,ratio))
       for i in range(len(nodelist)):
           node=nodelist[i]
           jobs=even[i]
           nodetojobs[node]=jobs
    return nodetojobs 
        
def Monitor(processes,nodelist,envpath,pidtojobpath):
    delarray=[]
    for p in processes:
        poll=p.poll()
        pid=p.pid
        if poll is None:
            pass
        else:
            returnstatus=p.returncode
            delarray.append(p)
            if returnstatus!=0:
                jobpath=pidtojobpath[pid]
                WriteToLogFile('Process failed '+str(pid)+' '+jobpath)
    for p in delarray:
        processes.remove(p)
    WriteToLogFile('Processes active '+str(len(processes)))
    jobinfo={}
    jobinfo,foundinputjobs=CheckForInputJobs(jobinfo)   
    if foundinputjobs==True:
        nodetojobs=DistributeEvenly(nodelist,jobinfo)
        processes,pidtojobpath=CallJobs(nodetojobs,envpath,processes,jobinfo,pidtojobpath)
        Monitor(processes,nodelist,envpath,pidtojobpath)
    else:
        time.sleep(30)
        Monitor(processes,nodelist,envpath,pidtojobpath)


thedir= os.path.dirname(os.path.realpath(__file__))+r'/'
if jobinfofilepath==None:
    processes=[]
    pidtojobpath={}
    logging.basicConfig(filename=loggerfile, filemode='w', format='%(name)s - %(levelname)s - %(message)s',level=logging.INFO)
    nodelist,nodetohasgpu,nodetousableproc,nodetousableram,nodetousabledisk=ReadNodeList(nodelistfilepath)
    jobinfo=WaitForInputJobs()
    nodetojobs=DistributeEvenly(nodelist,jobinfo)
    processes,pidtojobpath=CallJobs(nodetojobs,envpath,processes,jobinfo,pidtojobpath)
    Monitor(processes,nodelist,envpath,pidtojobpath)
else:
    head,tail=os.path.split(jobinfofilepath)
    split=tail.split('.')
    first=split[0]
    newfirst=first+'_submittoall'
    split[0]=newfirst
    newname='.'.join(split)
    newpath=os.path.join(thedir,newname)
    shutil.copy(jobinfofilepath,newpath)
    sys.exit()

