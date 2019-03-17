from tweepy import API 
from tweepy import Cursor
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import tweepy
import time
import numpy as np
import pandas as pd
import datetime

import twitter_credentials
from common_functions import *


INPUT_IDS = np.array([214032204, 27655533, 492113869, 3197731816, 15786941])
USER_INFO = pd.read_csv("/mnt/sdb1/leslie_results/data/user.csv")
CRAWLED_ID_FILE = "/mnt/sdb1/leslie_results/data/crawled_for_followers.csv"
USER_FOLLOWER_RELATIONSHIPS = "/mnt/sdb1/leslie_results/data/user-follower.csv"
FAILED_IDS = "/mnt/sdb1/leslie_results/data/failed_ids.csv"
CRAWLED_IDS =  np.array(pd.read_csv(CRAWLED_ID_FILE, dtype=np.int64))[:,0]



#### TWITTER CLIENT ####
# (class methods from https://github.com/vprusso/youtube_tutorials/tree/master/twitter_python)
class TwitterClient():
    def __init__(self, twitter_user=None):
        self.auth = TwitterAuthenticator().authenticate_twitter_app()
        self.twitter_client = API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        self.twitter_user = twitter_user

    def get_twitter_client_api(self):
        return self.twitter_client

    def get_follower_ids(self, num_followers):
        '''
        Returns a list of people who are following the requested user. 
        Uses pagination in order to retrieve all followers within Twitter API rate limits (5000 per request).
        Sleeps after each request for 1 minute so as not to exceed the twitter rate limits (1 per minute).

        Args: number of follower ids you want to return per request (maximum is 5000)
        Returns: list of follower ids of a given user
        '''
        store_follower_ids = []
        for page in tweepy.Cursor(api.followers_ids, id=self.twitter_user).pages():
            store_follower_ids.extend(page)
            print("page", page)
            print("Length of stored follower ids for this user:", len(store_follower_ids))
            time.sleep(60)

        for i in range(len(store_follower_ids)):
            store_follower_ids[i] = str(store_follower_ids[i])

        return(store_follower_ids)


#### TWITTER AUTHENTICATER ####
class TwitterAuthenticator():

    def authenticate_twitter_app(self):
        auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
        auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
        return(auth) 


def crawl_followers(user):
    '''
    Appends the user:followers relationships of a given id to the csv containing all user:follower relationships to date.
    Appends the given user to the csv containing the list of all ids that have been crawled for followers

    Args: user ID to crawl
    Returns: list of ids that follow the input user 
    '''
    followers = twitter_client.get_follower_ids(5000)

    results = pd.DataFrame({0: [str(user)] * len(followers), 1: followers})
    
    # Add this user id to the list of ids that have been crawled
    with open(CRAWLED_ID_FILE, 'a+') as f:
        user_id = pd.DataFrame(data=[user])
        user_id.to_csv(f, header=False, index=False)

    # Add follow relationships to the relationships csv for that language
    with open(USER_FOLLOWER_RELATIONSHIPS, 'a+') as f:
        results.to_csv(f, header=False, index=False)


if __name__ == '__main__':
    

    # otherwise get new ids
    ids_to_crawl = return_swiss_ids(input_ids=INPUT_IDS, old_ids=CRAWLED_IDS, user_df = USER_INFO)

    j = 0 
    num_failed = 0

    for i in range(len(ids_to_crawl)):
        id = ids_to_crawl[i]
        print(id)
        print(datetime.datetime.now())
        
        try:
            twitter_client = TwitterClient(id)
            api = twitter_client.get_twitter_client_api()
            crawl_response = crawl_followers(id)
            
            print("Success Number:", j, "id:", id)
            j += 1

        except tweepy.TweepError as e:
            num_failed += 1
            print("Number of failed IDs:", num_failed)
            print ("Error code:", e.api_code)

            # Append the failed id to the Failed Ids csv, then wait one minute before trying the next ID
            with open(FAILED_IDS, 'a+') as f:
                failed = pd.DataFrame(data=[str(id)])
                failed.to_csv(f, header=False, index=False)
            time.sleep(60)