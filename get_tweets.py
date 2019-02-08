import tweepy
import csv
import pandas as pd
import numpy as np
import json

import twitter_credentials

# specify whether you are looking for the tweet of a specific user, or a tweet of all the downloaded users
SPECIFIC_ID = False
ALL_IDS = True
FILE = "/mnt/sdb1/leslie_results/data/user.csv"

def get_all_tweets(user_id):
    '''
    Function to download the most recent 3200 tweets of a given user, 200 at a time (twitter limit). 

    Args: user_id
    Returns: N/A. Creates a csv with all tweets from the given user
    '''
    
    auth = tweepy.OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
    auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)
    
    #initialize a list to hold all the tweepy Tweets
    alltweets = []    
    
    #make initial request for most recent tweets (200 is the maximum allowed count)
    new_tweets = api.user_timeline(user_id = user_id,count=200)
    
    #save most recent tweets
    alltweets.extend(new_tweets)
    
    #save the id of the oldest tweet less one
    oldest = alltweets[-1].id - 1
    
    #keep grabbing tweets until there are no tweets left to grab
    while len(new_tweets) > 0:
        print "getting tweets before %s" % (oldest)
        
        #all subsiquent requests use the max_id param to prevent duplicates
        new_tweets = api.user_timeline(user_id = user_id, count=200, max_id=oldest)
        
        #save most recent tweets
        alltweets.extend(new_tweets)
        
        #update the id of the oldest tweet less one
        oldest = alltweets[-1].id - 1
        
        print "...%s tweets downloaded so far" % (len(alltweets))

    outtweets = [[tweet.id_str, tweet.created_at, tweet.text.encode("utf-8"), tweet.entities, tweet.retweet_count, tweet.favorite_count, tweet.place, tweet.coordinates] for tweet in alltweets]

    #write the csv    
    with open('%s_tweets.csv' % user_id, 'wb') as f:
        writer = csv.writer(f)
        writer.writerow(["id_str", "created_at", "text", "entities", "retweet_count", "favorite_count", "place", "coordinates"])
        writer.writerows(outtweets)
    
    pass


if __name__ == '__main__':
    
    
    if SPECIFIC_ID == True:
        #pass in the id of the account you want to download
        get_all_tweets(15786941)
    
    if ALL_IDS == True:
        # get tweets for all downloaded users who have posted at least 1 tweet
        all_users = np.array(pd.read_csv(FILE))[:,1]
        status_count = np.array(pd.read_csv(FILE))[:,15]
        for i in range(len(all_users)):
            current_user = int(all_users[i])
            print(current_user, type(current_user))
            if status_count[i] > 0:
                get_all_tweets(current_user)
    
