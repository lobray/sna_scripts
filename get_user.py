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

# SNA Computer
OUTGOING_LINKS = np.array(pd.read_csv("/mnt/sdb1/leslie_results/data/user-follower.csv", dtype=np.int64))[:,0]
INCOMING_LINKS = np.array(pd.read_csv("/mnt/sdb1/leslie_results/data/user-follower.csv", dtype=np.int64))[:,1]
INPUT_IDS = set(np.concatenate((OUTGOING_LINKS, INCOMING_LINKS)))
OLD_USER_INFO_IDS =  set(np.array(pd.read_csv("/mnt/sdb1/leslie_results/data/user.csv"))[:,1])


def lookup_user_info(followers_id):
    '''
    Function to get user objects for a list of user ids. Looks up in batchs of size 100 (Twitter limit). 
    Pauses for 1 second after each request to comply with Twitter limits.

    Args: list of ids you want to lookup user objects for
    Returns: list of Twitter user objects
    '''

    auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
    auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)

    # full_users = []
    users_count = len(followers_id)
    while True:
         
        for i in range((users_count / 100) + 1):    
            full_users = []
            try:
                 
                full_users.extend(api.lookup_users(user_ids=followers_id[i*100:min((i+1)*100, users_count)]))
                print('getting users batch:', i)
                print("=========================================")
                 
                simplified_df = construct_simplified_data_frame(full_users)
                with open("/mnt/sdb1/leslie_results/data/user.csv", 'a+') as f:
                    simplified_df.to_csv(f, header=False, index=False, encoding='utf-8')

                time.sleep(1)

            except tweepy.TweepError as e:
                print('Something went wrong, quitting...', i)
                print("time of error:", datetime.datetime.now())  
                time.sleep(15 * 60)
                pass
         
        return(full_users)


def construct_simplified_data_frame(lists_of_user_objects):
    '''
    Constructs a data frame of the relevant attributes from the Twitter user objects.

    Args: a list of Twitter User objects
    Returns: Pandas DataFrame of the desired attributes from all user objects 
    '''
    df = pd.DataFrame(data=[user.id for user in lists_of_user_objects], columns=['id'])
    df['id_str'] = np.array([user.id_str.encode('utf-8', 'ignore').decode('utf-8') for user in lists_of_user_objects])
    df['location'] = np.array([user.location.encode('utf-8', 'ignore').decode('utf-8') for user in lists_of_user_objects])
    df['lang'] = np.array([user.lang for user in lists_of_user_objects])
    df['screen_name'] = np.array([user.screen_name for user in lists_of_user_objects])
    df['verified'] = np.array([user.verified for user in lists_of_user_objects])
    df['followers_count'] = np.array([user.followers_count for user in lists_of_user_objects])
    df['friends_count'] = np.array([user.friends_count for user in lists_of_user_objects])  
    df['created_at'] = np.array([user.created_at for user in lists_of_user_objects])
    df['favourites_count'] = np.array([user.favourites_count for user in lists_of_user_objects])
    df['geo_enabled'] = np.array([user.geo_enabled for user in lists_of_user_objects])
    df['listed_count'] = np.array([user.listed_count for user in lists_of_user_objects])
    df['name'] = np.array([user.name.encode('utf-8', 'ignore').decode('utf-8') for user in lists_of_user_objects])
    df['protected'] = np.array([user.protected for user in lists_of_user_objects])
    df['statuses_count'] = np.array([user.statuses_count for user in lists_of_user_objects])    

    return(df)


if __name__ == '__main__':
    
    # if seed user, input the ids as a list into the variable new_users. 
    # seed_users = [15786941, 214032204, 27655533, 492113869, 3197731816]
    # new_users = seed_users
    
    new_users = dedupe_input_ids(new_ids=INPUT_IDS, old_ids=OLD_USER_INFO_IDS)
    new_users_objects = lookup_user_info(new_users)

    print(datetime.datetime.now())    

   

    













