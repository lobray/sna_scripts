import tweepy
import csv
import pandas as pd
import numpy as np
import json
import time
import datetime

import twitter_credentials
from common_functions import *

# HOME = "/mnt/sdb1/leslie_results/data/tweets/"
# FILE = "/mnt/sdb1/leslie_results/data/all_tweets.csv"

# my computer
HOME = "/home/leslie/Desktop/SNA/tweets/"
FILE = "/home/leslie/Desktop/SNA/tweets/all_tweets_27.csv"
CRAWLED_FOR_TWEETS_FILE = HOME + "crawled_for_tweets.csv"
ALL_TWEETS = HOME + "all_tweets.csv"
# "/mnt/sdb1/leslie_results/data/crawled_for_tweets.csv"



def create_files():

    header = ["user_id", "tweet_id_str", "created_at", "text", "hashtags", "entities", "retweet_count", "favorite_count", "place", "coordinates"]
    
    for i in range(100):
        if i < 10:
            identifier = '0' + str(i)
        else: 
            identifier = str(i)
# 
        with open(HOME + "all_tweets_" + identifier + ".csv", 'w') as g:
            writer = csv.writer(g)
            writer.writerow(header)


def partition_tweets():

    all_ids = []

    with open(ALL_TWEETS) as f:
        for line in f:
            
            user_id = line.split(",", 1)[0]
            identifier = user_id[-2:]

            if user_id == "user_id":
                continue
            else:
                with open(HOME + "all_tweets_" + identifier + ".csv", 'ab+') as g:
                    g.write(line)

                all_ids.append(user_id)

    unique_ids = list(set(all_ids))

    with open(CRAWLED_FOR_TWEETS_FILE, "a+") as g:
        for i in unique_ids:
            g.write(i)
            g.write('\n')


        # needs to get the last two digits of the user id, then assign the whole line to the 
        # also needs to add the id to the crawled_for_tweets.csv file, if its not aleady in it.

create_files()
partition_tweets()
