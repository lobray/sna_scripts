import tweepy
import csv
import pandas as pd
import numpy as np
import json
import time
import datetime

import twitter_credentials
from common_functions import *

# specify whether you are looking for the tweet of a specific user, or a tweet of all the downloaded users
SPECIFIC_ID = False
ALL_IDS = True
FILE = "/mnt/sdb1/leslie_results/data/user.csv" # LESLIE MAYBE CHANGE TO DOWNLOAD ONLY CONNECTIONS? 
PREVIOUSLY_GOT_TWEETS = list(set(pd.read_csv(   "/mnt/sdb1/leslie_results/data/crawled_for_tweets.csv").iloc[:,0])) # this line is the problem
USER_INFO = pd.read_csv(FILE)
INPUT_IDS = USER_INFO.iloc[:,1]


def get_all_tweets(user_id):
    '''
    Function to download the most recent 3200 tweets of a given user, 200 at a time (twitter limit). 

    Args: user_id
    Returns: N/A. Creates a csv with all tweets from the given user
    '''
    
    auth = tweepy.OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
    auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)
    
    try:

        #initialize a list to hold all the tweepy Tweets
        alltweets = []
        #make initial request for most recent tweets (200 is the maximum allowed count)
        new_tweets = api.user_timeline(user_id = user_id, count=200, tweet_mode = "extended")
        
        #save most recent tweets
        alltweets.extend(new_tweets)
        
        #save the id of the oldest tweet less one
        print("oldest", alltweets[-1].id)
        oldest = alltweets[-1].id - 1

        #keep grabbing tweets until there are no tweets left to grab
        while len(new_tweets) > 0:
            print("getting tweets before %s" % (oldest))
            
            #all subsiquent requests use the max_id param to prevent duplicates
            new_tweets = api.user_timeline(user_id = user_id, count=200, max_id=oldest, tweet_mode="extended")
            
            #save most recent tweets
            alltweets.extend(new_tweets)
            
            #update the id of the oldest tweet less one
            oldest = alltweets[-1].id - 1
            
            print("...%s tweets downloaded so far" % (len(alltweets)))


        
        full_tweets = [[str(user_id), tweet.id_str, tweet.created_at, tweet.full_text.encode("utf-8"), "", tweet.entities, tweet.retweet_count, tweet.favorite_count, tweet.place, tweet.coordinates] for tweet in alltweets]

        # extract hashtag and add into list
        for i in range(len(full_tweets)):
            if len(full_tweets[i][5]["hashtags"]) > 0:
                number_hashtags = len(full_tweets[i][5]["hashtags"])
                hashtag_list = []
                for j in range(number_hashtags):
                    new_hashtag = full_tweets[i][5]["hashtags"][j]["text"].encode("utf-8")
                    hashtag_list.append(new_hashtag)

                full_tweets[i][4] = hashtag_list
                
        user_id_string = str(user_id)
        last_two_digits = user_id_string[-2:]

        #write the tweet file csv    
        with open('/mnt/sdb1/leslie_results/data/all_tweets_' + last_two_digits + '.csv', 'a+') as g:
            writer = csv.writer(g)
            writer.writerows(full_tweets)

        with open('/mnt/sdb1/leslie_results/data/crawled_for_tweets.csv', 'a+') as g:
            writer = csv.writer(g)
            writer.writerows(user_id_string)
    
    except:
        pass



if __name__ == '__main__':
    
    
    if SPECIFIC_ID == True:
        #pass in the id of the account you want to download
        get_all_tweets(897160329364373505)
        # get_all_tweets(214032204)
    
    if ALL_IDS == True:
        # get tweets for all downloaded users who have posted at least 1 tweet

        ids_to_crawl = return_swiss_ids(input_ids=INPUT_IDS, old_ids=PREVIOUSLY_GOT_TWEETS, user_df = USER_INFO)
        
        for i in range(len(ids_to_crawl)):
            current_user = np.int64(ids_to_crawl[i])
            print(i, len(ids_to_crawl), current_user)
            get_all_tweets(current_user)
            time.sleep(3)
    print(datetime.datetime.now())        
    
