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

# set up the inputs to the concurrent processes
# example: 
#    args = [[key, secret key, token, secret token, month, start_day, end_day]]
#args = [api_keys[i].append(i + 1, ) if i != 2 else continue \
        for i in range(len(api_keys))]

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
            print 'An exception occurred: ' + sys.exc_info()[1]
        print(x)

def process_path(path, t):
    """
    Iterate through and process the data files specified by path.
    """
    for file in glob.glob(path):
        try:
            with open(file) as f:
                get_and_save_data(f, t)
        except:
            print 'An exception occurred', sys.exc_info()[1]

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
    
    """
    t = Twarc(args[0], args[1], args[2], args[3])
    last_day = calendar.monthrange(2020, mon)[1]

    # process dataset 1
    for day in range(1, last_day + 1):
        base_dir_A = "/home/zarif/projects/COVID-19-TweetIDs/"
        mon_str = get_date_str(mon)
        day_str = get_date_str(day)
        path = base_dir_A + \
            "2020-%s/coronavirus-tweet-id-2020-%s-%s-*.txt" \
            % (mon_str, mon_str, day_str)
        process_path(path, t)
        
        if mon > 2:
            # process dataset 2 for months after February
            base_dir_B = "/home/zarif/projects/covid19_twitter/"
            path = base_dir_B + \
            "dailies/2020-%s-%s/2020-%s-%s-dataset.tsv.gz" \
            % (mon_str, day_str, mon_str, day_str)
            if os.path.exists(path):
                df = pd.read_csv(path, sep='\t')
                try:
                    get_and_save_data(df['tweet_id'].tolist(), t)
                except:
                    print 'An exception occurred', sys.exc_info()[1]
            
    # process dataset 3 for months after February
    if mon > 2:
        base_dir = "/home/zarif/projects/dataverse_files/"
        path = base_dir + "coronavirus-through-27-May-2020-*.txt"
        process_path(path, t)
    

if __name__ == '__main__':
    p = Pool(10)
    p.map(hydrate_ids, )
    p.close()
    p.join()