import work_queue as wq
import os
import sys
import subprocess
import time
import shutil
import getopt


portnumber=9123
nodelistfilepath='nodes.txt'
masterhost='nova'
envpath='/home/bdw2292/.allpurpose.bashrc'
jobinfofilepath=None
pidfile='daemon.pid'
canceltaskid=None
canceltasktag=None
opts, xargs = getopt.getopt(sys.argv[1:],'',["bashrcpath=","jobinfofilepath=","canceltaskid=","canceltasktag="])
for o, a in opts:
    if o in ("--bashrcpath"):
        envpath=a
    elif o in ("--jobinfofilepath"):
        jobinfofilepath=a
    elif o in ("--canceltaskid"):
        canceltaskid=a
    elif o in ("--canceltasktag"):
        canceltasktag=a


def ReadNodeList(nodelistfilepath):
    nodelist=[]
    gpunodesonlylist=[]
    cpunodesonlylist=[]
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
                if 'CPUONLY' in line:
                    cpunodesonlylist.append(node)
                elif 'GPUONLY' in line:
                    gpunodesonlylist.append(node)

        temp.close()
    if len(nodelist)==0:
        raise ValueError('Node list has no nodes to read from')
    return nodelist,cpunodesonlylist,gpunodesonlylist


def CallWorker(node,envpath,masterhost,portnumber):
    if masterhost[-1].isdigit() and '-' in masterhost:
        cardvalue=masterhost[-1]
        masterhost=masterhost[:-2]
    cmdstr='work_queue_worker '+str(masterhost)+' '+str(portnumber)
    if node[-1].isdigit() and '-' in node:
        cardvalue=node[-1]
        node=node[:-2]
        cmdstr=SpecifyGPUCard(cardvalue,cmdstr)
    cmdstr = 'ssh %s "source %s ;%s"' %(str(node),envpath,cmdstr)
    print('Calling: '+cmdstr,flush=True)
    process = subprocess.Popen(cmdstr, stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)

def CallWorkers(nodelist,envpath,masterhost,portnumber):
    for node in nodelist:
        CallWorker(node,envpath,masterhost,portnumber)       

def CallJob(cmdstr,path):
    import subprocess
    cmdstr = 'cd %s ;%s' %(path,cmdstr)
    process = subprocess.Popen(cmdstr, stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)


def ReadJobInfoFromFile(jobinfo,filename):
    if os.path.isfile(filename):
        temp=open(filename,'r')
        results=temp.readlines()
        temp.close()
        for line in results:
            job,scratchspace,jobpath,ram,numproc=ParseJobInfo(line)
            array=['scratchspace','jobpath','ram','numproc']
            for key in array:
                if key not in jobinfo.keys():
                    jobinfo[key]={}
            jobinfo['scratchspace'][job]=scratchspace
            jobinfo['jobpath'][job]=jobpath
            jobinfo['ram'][job]=ram
            jobinfo['numproc'][job]=numproc

    return jobinfo

def ReadJobInfoFromDic(jobinfo):
    jobtoscratchspace=jobinfo['scratchspace']
    jobtojobpath=jobinfo['jobpath']
    jobtoram=jobinfo['ram']
    jobtonumproc=jobinfo['numproc']
    return jobtoscratch,jobtojobpath,jobtoram,jobtonumproc 

def ConvertMemoryToMBValue(scratch):
    availspace,availunit=SplitScratch(scratch)
    if availunit=='M' or availunit=='MB':
        availspace=float(availspace)
    elif availunit=='T' or availunit=='TB':
        availspace=float(availspace)*1000000
    elif availunit=='G' or availunit=='GB':
        availspace=float(availspace)*1000
    return int(availspace)
     

def SplitScratch(string):
    for eidx in range(len(string)):
        e=string[eidx]
        if not e.isdigit() and e!='.':
            index=eidx
            break
    space=string[:index]
    diskunit=string[index]
    return space,diskunit


def SubmitToQueue(jobinfo,queue,taskidtojob):
    print("Submitting tasks...",flush=True)
    jobtoscratch,jobtojobpath,jobtoram,jobtonumproc=ReadJobInfoFromDic(jobinfo)
    for job,jobpath in jobtojobpath.items():
        if job!=None:
            scratch=jobtoscratch[job]
            ram=jobtoram[job]
            numproc=jobtonumproc[job]
            print('Calling: '+str(job),flush=True)
            task = wq.PythonTask(CallJob, job, jobpath)
            if numproc!=None: 
                numproc=int(numproc)
                task.specify_cores(numproc)      
            if ram!=None:
                ram=ConvertMemoryToMBValue(ram)           
                task.specify_memory(ram)              
            if scratch!=None:
                scratch=ConvertMemoryToMBValue(scratch)         
                task.specify_disk(scratch)      
            if '_gpu' in job:
                task.specify_gpus(1)          
                task.specify_tag("GPU")
            else:
                task.specify_max_retries(2) # let QM do retry for now (or poltype)
                task.specify_tag("CPU")
            taskid=str(queue.submit(task))
            taskidtojob[taskid]=job
            print('Task ID of '+taskid+' is assigned to job '+job,flush=True)
    return queue,taskidtojob

def Monitor(q,taskidtojob):
    jobinfo={}
    while not q.empty():
        t = q.wait(5)
        q=CheckForTaskCancellations(q,taskidtojob)
        if t:
            print("Task used %s cores, %s MB memory, %s MB disk" % (t.resources_measured.cores,t.resources_measured.memory,t.resources_measured.disk,flush=True))
            print("Task was allocated %s cores, %s MB memory, %s MB disk" % (t.resources_requested.cores,t.resources_requested.memory,t.resources_requested.disk,flush=True))
            if t.limits_exceeded and t.limits_exceeded.cores > -1:
                print("Task exceeded its cores allocation.",flush=True)
            x = t.output
            if isinstance(x,Exception):
                print("Exception: {}".format(x),flush=True)
            else:
                print("Result: {}".format(x),flush=True)
        else:
            jobinfo,foundinputjobs=CheckForInputJobs(jobinfo)
            if foundinputjobs==True:
                q,taskidtojob=SubmitToQueue(jobinfo,q,taskidtojob)
                Monitor(q,taskidtojob)
       
    
    jobinfo=WaitForInputJobs()
    q,taskidtojob=SubmitToQueue(jobinfo,q,taskidtojob)
    Monitor(q,taskidtojob)


def WaitForInputJobs():
    print('Waiting for input jobs',flush=True)
    jobinfo={}
    foundinputjobs=False
    while foundinputjobs==False:
        jobinfo,foundinputjobs=CheckForInputJobs(jobinfo)   
        time.sleep(5)
    return jobinfo

def CheckForInputJobs(jobinfo):
    files=os.listdir()
    array=[]
    foundinputjobs=False
    for f in files:
        if 'submit' in f:
            foundinputjobs=True
            jobinfo=ReadJobInfoFromFile(jobinfo,f)
            array.append(f)
    for f in array:
        os.remove(f)
    return jobinfo,foundinputjobs
    
def WritePIDFile(pidfile):
    pid=str(os.getpid())
    temp=open(pidfile, 'w',buffering=1)
    temp.write(pid+'\n')
    temp.flush()
    os.fsync(temp.fileno())
    temp.close()

def ParseJobInfo(line):
    linesplit=line.split('--')[1:]
    linesplit=[e.rstrip() for e in linesplit]
    job=None
    scratch=None
    scratchspace=None
    jobpath=None
    ram=None
    numproc=None
    for line in linesplit:
        if "job=" in line:
            job=line.replace('job=','')
        if "scratchspace=" in line:
            scratchspace=line.replace('scratchspace=','')
        if "jobpath=" in line:
            jobpath=line.replace('jobpath=','')
        if "ram=" in line:
            ram=line.replace('ram=','')
        if "numproc=" in line:
            numproc=line.replace('numproc=','')

    return job,scratchspace,jobpath,ram,numproc

def CheckForTaskCancellations(q,taskidtojob):
    thedir= os.path.dirname(os.path.realpath(__file__))+r'/'
    os.chdir(thedir)
    files=os.listdir()
    for f in files:
        if '_cancel.txt' in f:
            temp=open(f,'r')
            results=temp.readlines()
            temp.close()
            result=results[0]
            resultsplit=result.split()
            final=resultsplit[0]
            if final in taskidtojob.keys():
                t = q.cancel_by_taskid(final) 
            else:
                t = q.cancel_by_tasktag(final) 
            os.remove(f)
    return q

thedir= os.path.dirname(os.path.realpath(__file__))+r'/'

if jobinfofilepath==None:
    if os.path.isfile(pidfile):
        raise ValueError('Daemon instance is already running')
    WritePIDFile(pidfile)
    try:
        taskidtojob={}
        nodelist,cpunodesonlylist,gpunodesonlylist=ReadNodeList(nodelistfilepath)
        jobinfo=WaitForInputJobs()
        queue = wq.WorkQueue(portnumber,debug_log = "output.log",stats_log = "stats.log",transactions_log = "transactions.log")
        queue.enable_monitoring('resourcesummary',watchdog=False)
        print("listening on port {}".format(queue.port),flush=True)
        CallWorkers(nodelist,envpath,masterhost,portnumber)
        # Submit several tasks for execution:
        queue,taskidtojob=SubmitToQueue(jobinfo,queue,taskidtojob)
        Monitor(queue,taskidtojob)
    finally:
        if os.path.isfile(pidfile): # delete pid file
            os.remove(pidfile)
    
else:
    if canceltaskid==None and canceltasktag=None:
        head,tail=os.path.split(jobinfofilepath)
        split=tail.split('.')
        first=split[0]
        newfirst=first+'_submit'
        split[0]=newfirst
        newname='.'.join(split)
        newpath=os.path.join(thedir,newname)
        shutil.copy(jobinfofilepath,newpath)
        sys.exit()
    else:
        os.chdir(thedir)
        if canceltaskid!=None:
            with open(canceltaskid+'_cancel.txt', 'w') as fp:
                fp.write(canceltaskid+'\n')
        if canceltasktag!=None:
            with open(canceltasktag+'_cancel.txt', 'w') as fp:
                fp.write(canceltasktag+'\n')
        sys.exit()

