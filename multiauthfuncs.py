import tweepy
from requests_oauthlib import OAuth1
import os
from tqdm import trange
from tqdm import tqdm
import requests
from time import time
import numpy as np
import json
import pickle
import multiprocessing
from time import sleep
from multiprocessing import Process

#change to your location of csv with different auths
#mylocation at ~/ on asterix
with open("./tweet_auth_tokens.csv") as fp:
    at=[i.split(",") for i in fp.read().split("\n")]
auths=[OAuth1(at[i][0],at[i][1],at[i][2],at[i][3]) for i in range(len(at))]


def get_users(x,auth,output_folder):
    while(True):
        URL=f"https://api.twitter.com/1.1/users/lookup.json?user_id={','.join([str(i) for i in x])}"
        r = requests.get(url = URL, auth=auth)
        if(r.status_code != 200):
            print("sleeping")
            url="https://api.twitter.com/1.1/application/rate_limit_status.json?resources=help,users,search,statuses"
            while(True):
                sleep(30)
                try:
                    l=requests.get(url = url, auth=auth).json()
                    if(l["resources"]["statuses"]["/statuses/user_timeline"]["remaining"]!=0):
                        break;
                except:
                    pass;
            continue;
        else:
            l = r.json()
            with open(f"{output_folder}/{x[0]}.p","wb") as f:
                pickle.dump(l,f)
            break;
def get_users_mp_aux(x,auth,output_folder):
    n=100
    for i in range(0,len(x),n):
        get_users(x[i:i+n],auth,output_folder)
        
        





def get_users_mp(x,auths,output_folder):
    if(os.path.isdir()):
        print(f"Not a directory: {output_folder}")
        return(None)
    Process_jobs = []
    k=len(auths)
    n=(1+len(x)//k)
    index=0
    for i in range(0,len(x),n):
        auth=auths[index]
        index+=1
        p = multiprocessing.Process(target = get_users_mp_aux, args = (x[i:i+n],auth,output_folder))
        Process_jobs.append(p)
        p.start()
    for p in Process_jobs:
        p.join()
        
#get_users_mp(x,auths,output_folder="/home/souvic/HULK/asterixdata/translation/data/")
#x is the list of twitter ids to retrive user objects with last status for





#get timeline data, last 3200 tweets of specified user_ids, code can be easily changed to screen names

def get_timeline(auth,user_id=None,screen_name=None,count=200,trim_user=True,exclude_replies=False,include_rts=True,max_id=None):
    l=[1]
    ans=[]
    while(len(l)!=0):
        if(user_id is not None):
            url=f"https://api.twitter.com/1.1/statuses/user_timeline.json?user_id={user_id}&count={count}&trim_user={trim_user}&exclude_replies={exclude_replies}&include_rts={include_rts}"
        else:
            url=f"https://api.twitter.com/1.1/statuses/user_timeline.json?screen_name={screen_name}&count={count}&trim_user={trim_user}&exclude_replies={exclude_replies}&include_rts={include_rts}"
        url+="&tweet_mode=extended"
        if(max_id is not None):
            #print(max_id,"here")
            url+=f"&max_id={max_id}"
        r = requests.get(url = url, auth=auth)
        #print(url)
        if(r.status_code == 401):
            break;
        if(r.status_code != 200):
            print("sleeping")
            url="https://api.twitter.com/1.1/application/rate_limit_status.json?resources=help,users,search,statuses"
            while(True):
                sleep(30)
                
                try:
                    l=requests.get(url = url, auth=auth).json()
                    if(l["resources"]["statuses"]["/statuses/user_timeline"]["remaining"]!=0):
                        break;
                except Exception as e:
                    print(e)
                    
                    pass;
            continue;
        else:
            l = r.json()
            ans.extend(l)
            if(len(l)==0 or max_id==l[-1]["id_str"]):
                break;
            else:
                max_id=l[-1]["id_str"]
    return(ans)
            
            



def get_timeline_mp_aux(index,auths,users,output_folder):
    auth=auths[index]
    with open(f'{output_folder}/{index}.jsonl', 'w') as outfile:
        for user_id in users:
            json1=get_timeline(auth=auth,user_id=user_id)
            json.dump(json1, outfile)
            outfile.write('\n')
        
        
def get_timeline_mp(auths,users,output_folder):
    if(not os.path.isdir(output_folder)):
        print(f"Not a directory: {output_folder}")
        return(None)
    Process_jobs = []
    k=len(auths)
    n=(1+len(users)//k)
    index=-1
    for i in range(0,len(users),n):
        index+=1
        p = multiprocessing.Process(target = get_timeline_mp_aux, args = (index,auths,users[i:i+n],output_folder))
        Process_jobs.append(p)
        p.start()
    for p in Process_jobs:
        p.join()
#output_folder="/home/souvic/HULK/asterixdata/translation/tweet_timelines"

#get_timeline_mp(auths,xx,output_folder)
