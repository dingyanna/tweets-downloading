import pymongo
import glob
import os
import time
import sys
import datetime
import calendar
from twarc import Twarc
import pandas as pd
import multiprocessing

# connect to database
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
mydb = myclient['covid-stigma']
mycol1 = mydb['tweets'] 

# twarc configuration for twitter API keys
consumer_key= ''
consumer_secret= ''
access_token= ''
access_token_secret= ''
t = Twarc(consumer_key, consumer_secret, access_token, access_token_secret)

# use hydrator/twarc to get full tweets for list of Tweet Ids
# and store them in db
def get_and_save_data(id_col):
    for tweet in t.hydrate(id_col):
        x = None
        try:
            x = mycol1.insert_one(tweet)
        except:
            print '[' + os.getpid() + ']' +\
                'An exception occurred: ' + sys.exc_info()[1]
        print(x)

def process_path(path):
    for file in glob.glob(path):
        try:
            with open(file) as f:
                get_and_save_data(f)
        except:
            print 'An exception occurred', sys.exc_info()[1]

def get_date_str(num):
    if num < 10:
        return '0' + str(num)
    else:
        return str(num)

def hydrate_ids(keys, mon):
    last_day = calendar.monthrange(2020, mon)[1]
    
    first = datetime.datetime(2020, mon, 1)
    last = datetime.datetime(2020, mon, last_day)
    for day in range(1, last_day + 1):
        base_dir_A = "/home/zarif/projects/COVID-19-TweetIDs/"
        mon_str = get_date_str(mon)
        day_str = get_date_str(day)
        path = base_dir_A + \
            "2020-%s/coronavirus-tweet-id-2020-%s-%s-*.txt" \
            % (mon_str, mon_str, day_str)
        process_path(path)

        # process dataset 2 for months after February
        if mon > 2:
            base_dir_B = "/home/zarif/projects/covid19_twitter/"
            path = base_dir_B + \
            "dailies/2020-%s-%s/2020-%s-%s-dataset.tsv.gz" \
            % (mon_str, day_str, mon_str, day_str)
            if os.path.exists(path):
                df = pd.read_csv(path, sep='\t')
                try:
                    get_and_save_data(df['tweet_id'].tolist())
                except:
                    print 'An exception occurred', sys.exc_info()[1]
    
    # process dataset 3 for May
    if mon == 5:
        base_dir = "/home/zarif/projects/dataverse_files/"
        path = base_dir + "coronavirus-through-27-May-2020-*.txt"
        process_path(path)


if __name__ == '__main__':


    print('Download complete...')