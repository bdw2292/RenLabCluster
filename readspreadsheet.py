import os
import sys
import gspread
import traceback
import time

def ReadSheets():
    gc = gspread.service_account(filename='credentials.json')
    sh=gc.open('Ren lab cluster usage')
    worksheet=sh.sheet1
    noderes=worksheet.col_values(1)
    usernameres=worksheet.col_values(7)
    gpunodetousername=dict(zip(noderes,usernameres))
    usernameres=worksheet.col_values(6)
    cpunodetousername=dict(zip(noderes,usernameres))
    return gpunodetousername,cpunodetousername


def ReadSheetsUpdateFile(usernames,nodetopology):
    while True:
        try:
            gpunodetousername,cpunodetousername=ReadSheets()
            WriteUsernameToNodeTopologyFile(nodetopology,gpunodetousername,cpunodetousername,usernames) 
        except:
            traceback.print_exc(file=sys.stdout)
            text = str(traceback.format_exc())
            print(text,flush=True)

        time.sleep(60) 

def WriteUsernameToNodeTopologyFile(nodetopology,gpunodetousername,cpunodetousername,usernames):
    temp=open(nodetopology,'r')
    results=temp.readlines()
    temp.close()
    temp=open(nodetopology,'w')
    for line in results:
        if '#' not in line:
            linesplit=line.split()
            card=linesplit[0]
            linesplit=linesplit[:8+1]
            line=' '.join(linesplit)
            anyuser=False
            if card in gpunodetousername.keys():
                gpuusername=gpunodetousername[card]
                if gpuusername in usernames:
                    pass
                else:
                    anyuser=True
            if anyuser==True:
                gpuusername='ANYUSER'
            anyuser=False
            if card in cpunodetousername.keys():
                cpuusername=cpunodetousername[card]
                if cpuusername in usernames:
                    pass
                else:
                    anyuser=True
            if anyuser==True:
                cpuusername='ANYUSER'
            line=line.replace('\n','')+' '+cpuusername+' '+gpuusername+'\n'
        temp.write(line)
    temp.close()

def ReadUsernameList(usernamelist):
    usernames=[]
    usernametoemail={}
    temp=open(usernamelist,'r')
    results=temp.readlines()
    temp.close()
    for line in results:
        linesplit=line.split()
        if len(linesplit)!=0:
            username=linesplit[0]
            email=linesplit[1]
            usernames.append(username)
            usernametoemail[username]=email

    return usernames,usernametoemail


usernametoemaillist='usernamestoemail.txt'
nodelistfilepath=os.path.join('NodeTopology','nodeinfo.txt')
usernames,usernametoemail=ReadUsernameList(usernametoemaillist)
ReadSheetsUpdateFile(usernames,nodelistfilepath)
