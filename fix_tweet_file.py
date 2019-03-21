import tweepy
import csv
import pandas as pd
import numpy as np
import json
import time
import datetime

# import twitter_credentials
# from common_functions import *

HOME = "/mnt/sdb1/leslie_results/data/tweets/"
FILE = "/mnt/sdb1/leslie_results/data/all_tweets.csv"
CRAWLED_FOR_TWEETS_FILE = "/mnt/sdb1/leslie_results/data/crawled_for_tweets.csv"

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
    iteration = 0 
    possible_values = list(np.arange(10,100))
    zero_to_nine = ["00", "01", "02", "03", "04", "05", "06", "07", "08", "09"]

    for i in range(len(possible_values)):
        possible_values[i] = str(possible_values[i])

    possible_values = zero_to_nine + possible_values
    
    

    with open(FILE) as f:
        for line in f:
            
            # print(iteration)
            iteration += 1

            try:

                user_id = line.split(",", 1)[0]
            
                identifier = user_id[-2:]
                print(iteration, identifier)

                if user_id == "user_id":
                    continue
                
                if identifier in possible_values:
                    print(user_id)

                    with open(HOME + "all_tweets_" + identifier + ".csv", 'a') as g:
                        g.write(line)

                    all_ids.append(user_id)
            except:
                print("some error")

    unique_ids = list(set(all_ids))

    with open(CRAWLED_FOR_TWEETS_FILE, "a") as g:
        for i in unique_ids:
            g.write(i)
            g.write('\n')


        # needs to get the last two digits of the user id, then assign the whole line to the 
        # also needs to add the id to the crawled_for_tweets.csv file, if its not aleady in it.

create_files()
# partition_tweets()

def get_all_unique_users():
    possible_values = list(np.arange(10,100))
    zero_to_nine = ["00", "01", "02", "03", "04", "05", "06", "07", "08", "09"]

    for i in range(len(possible_values)):
        possible_values[i] = str(possible_values[i])

    possible_values = zero_to_nine + possible_values

    for i in range(100):
        all_ids = []

        print(i)
        
        with open(HOME + "all_tweets_" + possible_values[i] + ".csv", 'rU') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                all_ids.append(row["user_id"])

        all_ids = list(set(all_ids))
        with open('/mnt/sdb1/leslie_results/data/crawled_for_tweets.csv', 'a') as g:
            writer = csv.writer(g)
            writer.writerows(all_ids)
        


        
# get_all_unique_users()
