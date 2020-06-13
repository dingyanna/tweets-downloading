import pymongo
import glob
import os
import time
import sys
import datetime
import calendar
from twarc import Twarc
import pandas as pd
from multiprocessing import Pool
import pandas

# connect to database
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
mydb = myclient['covid-stigma']
mycol = mydb['tweets'] 

# get the available api keys
with open('api_keys.txt') as f:
    df = pd.read_csv(f, sep=",")
api_keys = df.values.tolist()

# set up the array of inputs to the concurrent processes
# use 12 keys and distribute tweets in 4 months evenly to these keys.
# example: 
#    args = [[configured_twarc_object, month, start_day, end_day]]
args = []
for i in range(12):
    month = i / 3
    # adjust to the correct month, skipping February
    if month == 0:
        month += 1
    else:
        month += 2
    last_day = calendar.monthrange(2020, month)[1]
    start = 1
    end = 10
    if i % 3 == 1:
        start = 11
        end = 20
    elif i % 3 == 2:
        start = 21
        end = last_day
    t = Twarc(api_keys[i][0], api_keys[i][1], api_keys[i][2], api_keys[i][3],
            app_auth=True)
    args.append([t, month, start, end])

def get_and_save_data(id_col, t):
    """
    Use configured Twarc t to get full tweets given tweet ids id_col 
    and save tweets in database.
    """
    for tweet in t.hydrate(id_col):
        x = None
        try:
            x = mycol.insert_one(tweet)
        except:
            print sys.exc_info()[1]

def process_path(path, t):
    """
    Iterate through and process the data files specified by path.
    """
    for file in glob.glob(path):
        try:
            with open(file) as f:
                get_and_save_data(f, t)
        except:
            print sys.exc_info()[1]

def get_date_str(num):
    """
    Standardize the month and day string to 2 characters.
    """
    if num < 10:
        return '0' + str(num)
    else:
        return str(num)

def hydrate_ids(args):
    """
    Process three datasets of tweet ids and download the corresponding full
    tweets into database.

    Parameter:
        args - an array of [configured_twarc_object, month, start_day, end_day]
    """
    # process dataset 1
    for day in range(args[2], args[3] + 1):
        base_dir_A = "/home/zarif/projects/COVID-19-TweetIDs/"
        mon_str = get_date_str(args[1])
        day_str = get_date_str(day)
        path = base_dir_A + \
            "2020-%s/coronavirus-tweet-id-2020-%s-%s-*.txt" \
            % (mon_str, mon_str, day_str)
        process_path(path, args[0])
        
        # process dataset 2 for months after February
        if args[1] > 2:
            base_dir_B = "/home/zarif/projects/covid19_twitter/"
            path = base_dir_B + \
            "dailies/2020-%s-%s/2020-%s-%s-dataset.tsv.gz" \
            % (mon_str, day_str, mon_str, day_str)
            if os.path.exists(path):
                df = pd.read_csv(path, sep='\t')
                try:
                    get_and_save_data(df['tweet_id'].tolist(), args[0])
                except:
                    print path + '\n' + sys.exc_info()[1]
            
    # process dataset 3 for months after February
    mydict = {1: '0', 10: '2', 
                11: '3', 20: '5', 
                21: '6', 30: '9', 31: '9'}
    base_dir = "/home/zarif/projects/dataverse_files/"
    if args[1] > 2:
        path = base_dir + "coronavirus-through-27-May-2020-%s[%s-%s].txt" \
                % (str(args[1] - 3), mydict[args[2]], mydict[args[3]])
        process_path(path, args[0])

if __name__ == '__main__':
    p = Pool(12)
    p.map(hydrate_ids, args)
    p.close()
    p.join()