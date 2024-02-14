import ast, json
import os
import sys
import glob

def gatherfilesandgroups(dirroot):
    filelist = sorted(glob.glob(dirroot))
    scriptlist = []
    for fname in filelist:
        morefiles = sorted(glob.glob(fname+"/*"))
        #print(morefiles)
        for mfname in morefiles:
            if(mfname.endswith("groupid.txt")):
                print(mfname)
                with open(mfname,'r') as f:
                    gid = f.readline()
                    scriptlist.append({'filename':mfname,'groupid':gid})
    return scriptlist

def gathercyberfilenames(dirroot):
    filelist = sorted(glob.glob(dirroot))
    scriptlist = []
    for fname in filelist:
        morefiles = sorted(glob.glob(fname+"/*"))
        #print(morefiles)
        dirlist = []
        for mfname in morefiles:
            if(".record.0" in mfname):
                #print(mfname)
                dirlist.append(mfname)
        scriptlist.append({"root":fname,"dir":dirlist,"size":len(dirlist)})
    return scriptlist

root = "/home/jay/s3bucket/Deployment_2_SEOhio/Blue Route/OU Pacifica/*"
datafiles = gathercyberfilenames(root)
for df in datafiles:
    print(f"{df['root']} -> {df['size']}")

sys.exit(0)
scriptlist1 = gatherfilesandgroups(root)
# root = "/home/jay/s3bucket/Deployment_2_SEOhio/RedRoute/OU Pacifica/*"
# scriptlist2 = gatherfilesandgroups(root)
# root = "/home/jay/s3bucket/Deployment_2_SEOhio/GreenRoute/OU Pacifica/*"
# scriptlist3 = gatherfilesandgroups(root)
# scriptlist1.append(scriptlist2)
# scriptlist1.append(scriptlist3)
for item in scriptlist1:
    print(item['groupid'])