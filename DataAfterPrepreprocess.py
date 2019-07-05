# coding:utf-8
import numpy as np
import pandas as pd
import datetime as dt
import csv
import time

# yoochoose dataset preprocess
# data = pd.read_csv('data/yoochoose-clicks.dat', sep=',', header=None, usecols=[0,1,2], dtype={0:np.int32, 1:str, 2:np.int64})
# data.columns = ['SessionId', 'TimeStamp', 'ItemId']
# data.to_csv('data/yoochoose-clicks.csv', index=False)

# lastfm dataset preprocess
items_freq={}
with open('data/userid-timestamp-artid-artname-traid-traname.tsv', 'rb') as f:
    lines = f.readlines()
    user_dict = {}
    for line in lines:
        linelist = line.strip('\n').strip('\r').split('\t')
        if len(linelist) < 5:
            continue
        curuser = linelist[0]
        itemid = linelist[2]
        if len(itemid) < 2:
            continue
        newline = [linelist[0], linelist[1], linelist[2]]
        if user_dict.has_key(curuser):
            user_dict[curuser] += [newline]
        else:
            user_dict[curuser] = [newline]
        if itemid not in items_freq:
            items_freq[itemid] = 1
        else:
            items_freq[itemid] += 1

if len(items_freq) > 40000:
    items_freq = dict(sorted(items_freq.items(), key=lambda d: d[1], reverse=True)[:40000])

csvfile = open('data/lastfm_info.csv','w')
writer = csv.writer(csvfile)
writer.writerow(["SessionId","TimeStamp","ItemId"])
sessid = 0
for user in user_dict.keys():
    user_dict[user] = sorted(user_dict[user], key=lambda x: x[1])
    curdate = None
    sessid += 1
    for record in user_dict[user]:
        itemid = record[2]
        if len(itemid) < 2 or itemid not in items_freq:
            continue
        sessdate = time.mktime(time.strptime(record[1], '%Y-%m-%dT%H:%M:%SZ'))
        if curdate and sessdate-curdate > 28800:
            sessid += 1
        curdate = sessdate
        newrecord = [sessid, record[1], record[2]]
        writer.writerow(newrecord)
