import work_queue as wq
import os
import sys
import subprocess
import time
import shutil
import getopt
import traceback
import logging

waittime=15
portnumber=9123
nodelistfilepath='nodeinfo.txt'
masterhost='nova'
envpath='/home/bdw2292/.allpurpose.bashrc'
jobinfofilepath=None
pidfile='daemon.pid'
canceltaskid=None
canceltasktag=None
thedir= os.path.dirname(os.path.realpath(__file__))+r'/'
projectname=None
password=None
queueloggerfile='queuelogger.log'
errorloggerfile='errorlogger.log'
workerdir='/scratch'
username=None

opts, xargs = getopt.getopt(sys.argv[1:],'',["bashrcpath=","jobinfofilepath=","canceltaskid=","canceltasktag=",'projectname=','password=','portnumber=','username=','workerdir='])
for o, a in opts:
    if o in ("--bashrcpath"):
        envpath=a
    elif o in ("--jobinfofilepath"):
        jobinfofilepath=a
    elif o in ("--canceltaskid"):
        canceltaskid=a
    elif o in ("--canceltasktag"):
        canceltasktag=a
    elif o in ("--password"):
        password=a
    elif o in ("--projectname"):
        projectname=a
    elif o in ("--portnumber"):
        portnumber=int(a)
    elif o in ("--username"):
        username=a
    elif o in ("--workerdir"):
        workerdir=a


def ReadNodeList(nodelistfilepath):
    nodelist=[]
    nodetousableram={}
    nodetohasgpu={}
    nodetousabledisk={}
    nodetousableproc={}
    nodetocardcount={}
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
                cardcount=linesplit[3]
                proc=linesplit[4]
                ram=linesplit[5]
                scratch=linesplit[6]
                consumratio=float(linesplit[7])
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
                nodetocardcount[node]=cardcount          

        temp.close()
    if len(nodelist)==0:
        raise ValueError('Node list has no nodes to read from')
    return nodelist,nodetohasgpu,nodetousableproc,nodetousableram,nodetousabledisk,nodetocardcount


def CallWorker(node,envpath,masterhost,portnumber,hasgpu,proc,ram,disk,projectname,password,cardcount,queuelogger,workerdir,username):
    fullworkdir=os.path.join(workerdir,username)
    idletimeout=100000000
    cmdstr='work_queue_worker '+str(masterhost)+' '+str(portnumber) 
    cmdstr+=' --workdir '+fullworkdir
    cmdstr+=' -d all -o '+os.path.join(fullworkdir,'worker.debug')
    if proc!='UNK':
        cmdstr+=' '+'--cores '+proc
    if ram!='UNK':
        cmdstr+=' '+'--memory '+ram
    cmdstr+=' '+'--gpus '+str(cardcount)
    cmdstr+=' '+'-t '+str(idletimeout)
    cmdstr+=' '+'-M '+projectname
    #cmdstr+=' '+'--password '+password CCtools has issues when this is specified
    cmdstr+=' '+'--single-shot'
    mkdirstring='mkdir '+fullworkdir+' ; '
    cmdstr=mkdirstring+cmdstr
    thedir= os.path.dirname(os.path.realpath(__file__))+r'/'
    cmdstr = 'ssh %s "source %s ;%s"' %(str(node),envpath,cmdstr)
    WriteToLogFile(queuelogger,'Calling: '+cmdstr)
    process = subprocess.Popen(cmdstr, stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)

def CallWorkers(nodelist,envpath,masterhost,portnumber,nodetohasgpu,nodetousableproc,nodetousableram,nodetousabledisk,projectname,password,nodetocardcount,queuelogger,workerdir,username):
    for node in nodelist:
        hasgpu=nodetohasgpu[node]
        proc=nodetousableproc[node]
        ram=nodetousableram[node]
        disk=nodetousabledisk[node]
        cardcount=nodetocardcount[node]
        CallWorker(node,envpath,masterhost,portnumber,hasgpu,proc,ram,disk,projectname,password,cardcount,queuelogger,workerdir,username)       



def ReadJobInfoFromFile(jobinfo,filename,queuelogger):
    if os.path.isfile(filename):
        temp=open(filename,'r')
        results=temp.readlines()
        temp.close()
        for line in results:
            split=line.split()
            if len(split)==0:
                continue
            cmdstr,ram,numproc,inputfilepaths,outputfilepaths,binpath,scratchpath,cache,inputline=ParseJobInfo(line)

            if inputfilepaths==None:
                WriteToLogFile(queuelogger,'WARNING inputfilepaths is not specified, will ignore input')
                continue
            array=['ram','numproc','inputfilepaths','outputfilepaths','binpath','scratchpath','cache','inputline']
            for key in array:
                if key not in jobinfo.keys():
                    jobinfo[key]={}
            job=tuple([cmdstr,tuple(inputfilepaths)])
            jobinfo['ram'][job]=ram
            jobinfo['numproc'][job]=numproc
            jobinfo['inputfilepaths'][job]=inputfilepaths
            jobinfo['outputfilepaths'][job]=outputfilepaths
            jobinfo['binpath'][job]=binpath
            jobinfo['scratchpath'][job]=scratchpath
            jobinfo['cache'][job]=cache
            jobinfo['inputline'][job]=inputline

    return jobinfo

def ReadJobInfoFromDic(jobinfo):
    jobtoram=jobinfo['ram']
    jobtonumproc=jobinfo['numproc']
    jobtoinputfilepaths=jobinfo['inputfilepaths']
    jobtobinpath=jobinfo['binpath']
    jobtoscratchpath=jobinfo['scratchpath']
    jobtooutputfilepaths=jobinfo['outputfilepaths']
    jobtocache=jobinfo['cache']
    jobtoinputline=jobinfo['inputline']
    return jobtoram,jobtonumproc,jobtoinputfilepaths,jobtobinpath,jobtoscratchpath,jobtooutputfilepaths,jobtocache,jobtoinputline

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



def SubmitToQueue(jobinfo,queue,taskidtojob,cattomaxresourcedic,taskidtooutputfilepaths,taskidtoinputline,queuelogger,errorlogger):
    WriteToLogFile(queuelogger,"Submitting tasks...")
    jobtoram,jobtonumproc,jobtoinputfilepaths,jobtobinpath,jobtoscratchpath,jobtooutputfilepaths,jobtocache,jobtoinputline=ReadJobInfoFromDic(jobinfo)
    for job,ram in jobtoram.items():
        if job!=None:
            numproc=jobtonumproc[job]
            inputfilepaths=jobtoinputfilepaths[job]
            outputfilepaths=jobtooutputfilepaths[job]
            binpath=jobtobinpath[job]
            scratchpath=jobtoscratchpath[job]
            cacheval=jobtocache[job]
            cmdstr=job[0]
            inputline=jobtoinputline[job]
            if scratchpath!=None:
                head,tail=os.path.split(scratchpath)
                string1='mkdir '+head+' ; '
                string2='mkdir '+scratchpath+' ; '
                cmdstr=string1+string2+cmdstr
            temp={}
            task = wq.Task(cmdstr)
            if binpath!=None:
                if os.path.isfile(binpath):
                    head,tail=os.path.split(binpath)
                    task.specify_file(binpath, tail, wq.WORK_QUEUE_INPUT, cache=cacheval)
            if inputfilepaths!=None:
                for inputfile in inputfilepaths:
                    if os.path.isfile(inputfile):
                        head,tail=os.path.split(inputfile)
                        task.specify_file(inputfile, tail, wq.WORK_QUEUE_INPUT, cache=cacheval)
            if outputfilepaths!=None:
                for outputfilepath in outputfilepaths:
                    head,outputfile=os.path.split(outputfilepath)
                    task.specify_file(outputfile, outputfile, wq.WORK_QUEUE_OUTPUT, cache=cacheval)
            if numproc!=None: 
                numproc=int(numproc)
                task.specify_cores(numproc)     
                temp['cores']=numproc 
            if ram!=None:
                ram=ConvertMemoryToMBValue(ram)           
                task.specify_memory(ram)    
                temp['memory']=ram      
            if '_gpu' in cmdstr:
                task.specify_gpus(1)        
                task.specify_cores(0)     
                task.specify_tag("GPU")
                temp['gpus']=1
                temp['cores']=0
            else:
                task.specify_tag("CPU")
                temp['gpus']=1
            task.specify_max_retries(2) # if some issue on node, retry on another node
            foundcat=False
            largestcat=0
            for cat,resourcedic in cattomaxresourcedic.items():
                if temp==resoucedic:
                    foundcat=True
                    break
                catnum=cat.replace('Cat','')
                catnum=int(catnum)
                if catnum>largestcat:
                    largestcat=catnum
            if foundcat==True:
                pass
            else:
                largestcat=largestcat+1
                cat="Cat"+str(largestcat)
                #queue.specify_category_max_resources(cat, temp)
            #task.specify_category(cat)
            taskid=str(queue.submit(task))
            taskidtooutputfilepaths[taskid]=outputfilepaths
            taskidtoinputline[taskid]=inputline
            taskidtojob[taskid]=job
            WriteToLogFile(queuelogger,'Task ID of '+taskid+' is assigned to job '+cmdstr)
    return queue,taskidtojob,cattomaxresourcedic,taskidtooutputfilepaths,taskidtoinputline

def Monitor(q,taskidtojob,cattomaxresourcedic,taskidtooutputfilepaths,waittime,taskidtoinputline,queuelogger,errorlogger):
    jobinfo={}
    while not q.empty():
        t = q.wait(waittime)
        q=CheckForTaskCancellations(q,taskidtojob)
        if t:
            taskid=str(t.id)
            inputline=taskidtoinputline[taskid]
            newoutputfilepaths=[] # sort by largest file size and move largest first (like move arc first so dont submit next job before arc and dyn are returned
            if taskid in taskidtooutputfilepaths.keys():
                outputfilepaths=taskidtooutputfilepaths[taskid]
                if outputfilepaths!=None:
                    for outputfilepath in outputfilepaths:
                        head,tail=os.path.split(outputfilepath)
                        if os.path.isdir(head):
                            if os.path.isfile(tail):
                               newoutputfilepaths.append(outputfilepath) 
            newfiles=[os.path.split(i)[1] for i in newoutputfilepaths]
            newfilestofilepaths=dict(zip(newfiles,newoutputfilepaths))
            filesizes=[os.stat(thefile).st_size for thefile in newfiles]
            newfilestofilesizes=dict(zip(newfiles,filesizes))
            sorteddic={k: v for k, v in sorted(newfilestofilesizes.items(), key=lambda item: item[1],reverse=True)}  
            for filename in sorteddic.keys():
                filepath=newfilestofilepaths[filename]
                WriteToLogFile(queuelogger,'Moving file '+filename)
                shutil.move(os.path.join(os.getcwd(),filename),filepath)
                
            exectime = t.cmd_execution_time/1000000
            returnstatus=t.return_status
            WriteToLogFile(queuelogger,'A job has finished Task %s!\n' % (str(taskid)))
            if returnstatus!=0:
                WriteToLogFile(queuelogger,'Error: Job did not terminate normally '+inputline)
                WriteToLogFile(errorlogger,'Error: Job did not terminate normally '+inputline)

            try:
                WriteToLogFile(queuelogger,'Job name = ' + str(t.tag) + 'command = ' + str(t.command) + '\n')
                WriteToLogFile(queuelogger,"Host = " + str(t.hostname) + '\n')
                WriteToLogFile(queuelogger,"Execution time = " + str(exectime))
                WriteToLogFile(queuelogger,"Task used %s cores, %s MB memory, %s MB disk" % (t.resources_measured.cores,t.resources_measured.memory,t.resources_measured.disk))
                WriteToLogFile(queuelogger,"Task was allocated %s cores, %s MB memory, %s MB disk" % (t.resources_requested.cores,t.resources_requested.memory,t.resources_requested.disk))
                if t.limits_exceeded and t.limits_exceeded.cores > -1:
                    WriteToLogFile(queuelogger,"Task exceeded its cores allocation.")
            except:
                pass # sometimes task returns as None?? not very often though
        else:
            WriteToLogFile(queuelogger,"Workers: %i init, %i idle, %i busy, %i total joined, %i total removed\n" \
                % (q.stats.workers_init, q.stats.workers_idle, q.stats.workers_busy, q.stats.workers_joined, q.stats.workers_removed))
            WriteToLogFile(queuelogger,"Tasks: %i running, %i waiting, %i dispatched, %i submitted, %i total complete\n" \
                % (q.stats.tasks_running, q.stats.tasks_waiting, q.stats.tasks_dispatched, q.stats.tasks_submitted, q.stats.tasks_done))

            jobinfo,foundinputjobs=CheckForInputJobs(jobinfo,queuelogger)
            if foundinputjobs==True:
                q,taskidtojob,cattomaxresourcedic,taskidtooutputfilepaths,taskidtoinputline=SubmitToQueue(jobinfo,q,taskidtojob,cattomaxresourcedic,taskidtooutputfilepaths,taskidtoinputline,queuelogger,errorlogger)
                Monitor(q,taskidtojob,cattomaxresourcedic,taskidtooutputfilepaths,waittime,taskidtoinputline,queuelogger,errorlogger)
       
    
    jobinfo=WaitForInputJobs(queuelogger)
    q,taskidtojob,cattomaxresourcedic,taskidtooutputfilepaths,taskidtoinputline=SubmitToQueue(jobinfo,q,taskidtojob,cattomaxresourcedic,taskidtooutputfilepaths,taskidtoinputline,queuelogger,errorlogger)
    Monitor(q,taskidtojob,cattomaxresourcedic,taskidtooutputfilepaths,waittime,taskidtoinputline,queuelogger,errorlogger)


def WaitForInputJobs(queuelogger):
    WriteToLogFile(queuelogger,'Waiting for input jobs')
    jobinfo={}
    foundinputjobs=False
    while foundinputjobs==False:
        jobinfo,foundinputjobs=CheckForInputJobs(jobinfo,queuelogger)   
        time.sleep(5)
    return jobinfo

def CheckForInputJobs(jobinfo,queuelogger):
    files=os.listdir()
    array=[]
    foundinputjobs=False
    for f in files:
        if 'submit.' in f:
            foundinputjobs=True
            jobinfo=ReadJobInfoFromFile(jobinfo,f,queuelogger)
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
    ram=None
    numproc=None
    inputfilepaths=None
    outputfilepaths=None
    binpath=None
    scratchpath=None
    cache=False
    inputline=line
    for line in linesplit:
        if "job=" in line:
            job=line.replace('job=','')
        if "scratchpath=" in line:
            scratchpath=line.replace('scratchpath=','')
        if "ram=" in line:
            ram=line.replace('ram=','')
        if "numproc=" in line:
            numproc=line.replace('numproc=','')
        if "inputfilepaths=" in line:
            inputfilepaths=line.replace('inputfilepaths=','')
            inputfilepaths=inputfilepaths.split(',')
        if "outputfilepaths=" in line:
            outputfilepaths=line.replace('outputfilepaths=','')
            outputfilepaths=outputfilepaths.split(',')
        if "absolutepathtobin" in line:
            binpath=line.replace('absolutepathtobin=','')
        if "cache" in line:
            cache=True

    return job,ram,numproc,inputfilepaths,outputfilepaths,binpath,scratchpath,cache,inputline

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

def WriteToLogFile(logger,string):
    logger.info(string)

def SetupLogger(name, log_file,formatter, level=logging.INFO):
    handler = logging.FileHandler(log_file)        
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger

if jobinfofilepath==None:

    #if password==None:
    #    raise ValueError('Please set password for manager, needed for security purposes. Otherwise cluster data visibile to public.')
    if projectname==None:
        raise ValueError('Please set projectname for manager, needed for multiple instances of managers and work_queue_workers running on cluster via different users.')
    if username==None:
        raise ValueError('Please specify username for working directory location')
    try:
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        queuelogger=SetupLogger('queuelogger',queueloggerfile,formatter)
        errorlogger=SetupLogger('errorlogger',errorloggerfile,formatter)
        
        #logging.basicConfig(filename=loggerfile, filemode='w', format='%(name)s - %(levelname)s - %(message)s',level=logging.INFO)
        if os.path.isfile(pidfile):
            raise ValueError('Daemon instance is already running')
        WritePIDFile(pidfile)
        taskidtojob={}
        cattomaxresourcedic={}
        taskidtooutputfilepaths={}
        taskidtoinputline={}
        nodelist,nodetohasgpu,nodetousableproc,nodetousableram,nodetousabledisk,nodetocardcount=ReadNodeList(nodelistfilepath)
        jobinfo=WaitForInputJobs(queuelogger)
        queue = wq.WorkQueue(portnumber,name=projectname,debug_log = "output.log",stats_log = "stats.log",transactions_log = "transactions.log")
        queue.enable_monitoring('resourcesummary',watchdog=False)
        #queue.specify_password(password) CCTools has issues when this is specified
        WriteToLogFile(queuelogger,"listening on port {}".format(queue.port))
        CallWorkers(nodelist,envpath,masterhost,portnumber,nodetohasgpu,nodetousableproc,nodetousableram,nodetousabledisk,projectname,password,nodetocardcount,queuelogger,workerdir,username)
        # Submit several tasks for execution:
        queue,taskidtojob,cattomaxresourcedic,taskidtooutputfilepaths,taskidtoinputline=SubmitToQueue(jobinfo,queue,taskidtojob,cattomaxresourcedic,taskidtooutputfilepaths,taskidtoinputline,queuelogger,errorlogger)
        Monitor(queue,taskidtojob,cattomaxresourcedic,taskidtooutputfilepaths,waittime,taskidtoinputline,queuelogger,errorlogger)

    except:
        traceback.print_exc(file=sys.stdout)
        text = str(traceback.format_exc())
        WriteToLogFile(queuelogger,str(text))
        raise ValueError('Program Crash')

    finally:
        if os.path.isfile(pidfile): # delete pid file
            os.remove(pidfile)
    
else:
    if canceltaskid==None and canceltasktag==None:
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

