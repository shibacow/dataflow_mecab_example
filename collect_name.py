#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from collections import Counter
from glob import glob
import os,re
import subprocess
import argparse
import logging
import pandas as pd
logging.basicConfig(level=logging.INFO)
parser = argparse.ArgumentParser()
parser.add_argument("--name", help="specify name")
parser.add_argument("--limit",default=250,help="specify limit",type=int)
args = parser.parse_args()

#src='results_1611373606'
#src='results_1611383629'
src='results_1611386343'
if not os.path.isdir(src):
    cmd='gsutil -m mv -r gs://xxxxxxxxxxxxxxxxxxxxxx/{} .'.format(src)
    subprocess.run(cmd,shell=True)

def readfiles():
    for f in os.listdir(src):
        srcf=src+os.sep+f
        with open(srcf,'rt') as fin:
            for l in fin.readlines():
                l=l.strip()
                yield l
class SplitData(object):
    def __init__(self,l):
        sp=l.split('_')
        if len(sp)!=2:
            first=sp[0]
            last=u'_'.join(sp[1:])
            sp=[first,last]
        self.year=int(sp[0])
        last=sp[1].split(':')
        if len(last)!=2:
            first=u':'.join(last[:-1])
            lst=last[-1]
            last=[first,lst]
        self.body=last[0]
        self.cnt=int(last[-1])
    def __str__(self):
        return u"year={} body={} cnt={}".format(self.year,self.body,self.cnt)
def gen_dataframe(dlist):
    df = pd.DataFrame(dlist)
    df = pd.pivot_table(df,columns='year',index='rank',values='body',aggfunc=lambda x:" ".join(x))
    df.to_csv('result.csv')
def main():
    dkt={}
    for l in readfiles():
        sp=SplitData(l)
        dkt.setdefault(sp.year,[])
        dkt[sp.year].append(sp)
    dlist=[]
    for k in sorted(dkt.keys()):
        v=dkt[k]
        vv=sorted(v,key=lambda x:x.cnt,reverse=True)
        vv=vv[:args.limit]
        logging.info('='*50)
        for rank,v in enumerate(vv):
            if args.name:
                if re.search(args.name,v.body):
                    logging.info("y={0} rank={1:03d} data={2}".format(k,rank+1,v))
            else:
                    logging.info("y={0} rank={1:03d} data={2}".format(k,rank+1,v))
                    dlist.append(dict(year=k,rank=rank+1,body=v.body))
    if not args.name:
        gen_dataframe(dlist)
if __name__=='__main__':main()
