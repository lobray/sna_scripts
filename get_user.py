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

# SNA Computer
OUTGOING_LINKS = np.array(pd.read_csv("/mnt/sdb1/leslie_results/data/user-follower.csv", dtype=np.int64))[:,0]
INCOMING_LINKS = np.array(pd.read_csv("/mnt/sdb1/leslie_results/data/user-follower.csv", dtype=np.int64))[:,1]
INPUT_IDS = np.concatenate((OUTGOING_LINKS, INCOMING_LINKS))
OLD_USER_INFO_IDS =  np.array(pd.read_csv("/mnt/sdb1/leslie_results/data/user.csv"))[:,0]

# Leslie's Computer
# OUTGOING_LINKS = np.array(pd.read_csv("/home/leslie/Desktop/SNA/user-follower.csv", dtype=np.int64))[:,0]
# INCOMING_LINKS = np.array(pd.read_csv("/home/leslie/Desktop/SNA/user-follower.csv", dtype=np.int64))[:,1]
# INPUT_IDS = np.concatenate((OUTGOING_LINKS, INCOMING_LINKS))
# OLD_USER_INFO_IDS =  np.array(pd.read_csv("/home/leslie/Desktop/SNA/user.csv"))[:,0]


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

    full_users = []
    users_count = len(followers_id)
    # while True:
         
    for i in range((users_count / 100) + 1):    
        try:
             
            full_users.extend(api.lookup_users(user_ids=followers_id[i*100:min((i+1)*100, users_count)]))
            print('getting users batch:', i)
            print("=========================================")
            time.sleep(1)

        except tweepy.TweepError as e:
            print('Something went wrong, quitting...', i)
            print("time of error:", datetime.datetime.now())  
            time.sleep(15 * 60)
         
    return(full_users)


def construct_data_frame(lists_of_user_objects):
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
    df['description'] = np.array([user.description.encode('utf-8', 'ignore').decode('utf-8') for user in lists_of_user_objects])  

    # Fix encoding issues (due to crazy characters in description)
    # for i in range(len(df)):
    #     location_entry = df.loc[i,"location"]
    #     name_entry = df.loc[i,"name"]
    #     description_entry = df.loc[i,"description"]
    #     df[i,"location"] = location_entry.encode('utf-8', 'ignore').decode('utf-8')
    #     df[i, "name"] = name_entry.encode('utf-8', 'ignore').decode('utf-8')
    #     df[i, "description"] = description_entry.encode('utf-8', 'ignore').decode('utf-8')

    return(df)

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

    # Fix encoding issues (due to crazy characters in description)
    # for i in range(len(df)):
    #     print(df.loc[i,:])
    #     location_entry = df.loc[i,"location"]
    #     name_entry = df.loc[i,"name"]
    #     df[i,"location"] = location_entry.encode('utf-8', 'ignore').decode('utf-8')
    #     df[i, "name"] = name_entry.encode('utf-8', 'ignore').decode('utf-8')


    return(df)


def dedupe_input_ids(new_ids, old_ids):
    '''
    Takes input ids and checks against the list of previously crawled ids to prevent crawling the same node twice.

    Args: new ids to consider and old ids already crawled
    Returns: list of IDs to crawl
    '''
    new_input_ids = set(list(new_ids))
    previously_crawled_ids = set(list(old_ids))

    ids_to_crawl = list(new_input_ids - previously_crawled_ids)

    # convert to integer
    for i in range(len(ids_to_crawl)):
        ids_to_crawl[i] = int(ids_to_crawl[i])
    return(ids_to_crawl)


if __name__ == '__main__':
    
    # if seed user, input the ids as a list into the variable new_users. 
    # seed_users = [15786941, 214032204, 27655533, 492113869, 3197731816]
    # new_users = seed_users
    
    new_users = dedupe_input_ids(new_ids=INPUT_IDS, old_ids=OLD_USER_INFO_IDS)
    new_users_objects = lookup_user_info(new_users)
    df = construct_data_frame(new_users_objects)   
    simplified_df = construct_simplified_data_frame(new_users_objects)
    print(datetime.datetime.now())    

    with open("/mnt/sdb1/leslie_results/data/user.csv", 'a+') as f:
        simplified_df.to_csv(f, header=False, index=False, encoding='utf-8')

    with open("/mnt/sdb1/leslie_results/data/full_user.csv", 'a+') as f:
        df.to_csv(f, header=False, index=False, encoding='utf-8')

    print(datetime.datetime.now())    
   

    













