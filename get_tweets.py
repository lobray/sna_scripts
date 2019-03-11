import tweepy
import csv
import pandas as pd
import numpy as np
import json
import time
import datetime

import twitter_credentials

# specify whether you are looking for the tweet of a specific user, or a tweet of all the downloaded users
SPECIFIC_ID = False
ALL_IDS = True
FILE = "/mnt/sdb1/leslie_results/data/user.csv"
PREVIOUSLY_GOT_TWEETS = list(set(pd.read_csv("/mnt/sdb1/leslie_results/data/all_tweets.csv").iloc[:,0]))
USER_INFO = pd.read_csv(FILE)
INPUT_IDS = USER_INFO.iloc[:,1]


def dedupe_input_ids(new_ids, old_ids):
    '''
    Takes input ids and checks against the list of previously crawled ids to prevent crawling the same node twice.

    Args: new ids to consider and old ids already crawled
    Returns: list of IDs to crawl
    '''
    new_input_ids = set(new_ids)
    previously_crawled_ids = set(old_ids)
    ids_to_crawl = list(new_input_ids - previously_crawled_ids)

    # convert to integer
    for i in range(len(ids_to_crawl)):
        print(ids_to_crawl[i])
        try:
            ids_to_crawl[i] = int(ids_to_crawl[i])
        except:
            pass

    return(ids_to_crawl)

def return_swiss_ids(input_ids, old_ids, user_df):
    '''
    Function to find which users are located in Switzerland, AND have posted at least one status.

    Args: input_ids should be the output from deduped list
    Returns: list of ids that are in Switzerland and have not been crawled 
    '''
    ids_to_crawl = set(dedupe_input_ids(new_ids=input_ids, old_ids=old_ids))
    
    swiss_places = ["svizzera", "switzerland", "schweiz", "suisse", "ch", "zurich", "bern", "geneva", "lausanne", "winterthur", "luzern", "st. gallen", "lugano", "basel", "biel", "bienne"]
    
    swiss_users_list = []

    lang_df = user_df[user_df["lang"].isin(["en", "de", "fr", "it"])]
    non_null_df = lang_df[-lang_df["location"].isna()]
    
    for user in non_null_df.index:
        print(user)
        loc = non_null_df["location"][user].lower()
        tmp_loc = []
        for i in range(len(swiss_places)):
            tmp_loc.append(swiss_places[i] in loc)
        if np.sum(np.array(tmp_loc)) > 0:
            # add in extra condition that status count needs to be greater than zero
            if non_null_df["statuses_count"][user] > 1:
                # add in condition that tweets are not protected
                if non_null_df["protected"][user] == False:
                    swiss_users_list.append(non_null_df["id_str"][user])

    swiss_ids = set(swiss_users_list)
    
    print(swiss_ids)

    new_swiss_ids = list(ids_to_crawl.intersection(swiss_ids))
    for i in range(len(new_swiss_ids)):
        new_swiss_ids[i] = int(new_swiss_ids[i])

    print("number of new swiss ids:", new_swiss_ids)

    return(new_swiss_ids)


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


        # outtweets = [[tweet.id_str, tweet.created_at, tweet.full_text.encode("utf-8"), tweet.entities, tweet.retweet_count, tweet.favorite_count, tweet.place, tweet.coordinates] for tweet in alltweets]
        full_tweets = [[str(user_id), tweet.id_str, tweet.created_at, tweet.full_text.encode("utf-8"), "", tweet.entities, tweet.retweet_count, tweet.favorite_count, tweet.place, tweet.coordinates] for tweet in alltweets]

        # outtweets = [[tweet.id_str, tweet.created_at, tweet.fulltext.encode("utf-8"), tweet.entities, tweet.retweet_count, tweet.favorite_count, tweet.place, tweet.coordinates] for tweet in alltweets]
        # full_tweets = [[user_id, tweet.id_str, tweet.created_at, tweet.fulltext.encode("utf-8"), "", tweet.entities, tweet.retweet_count, tweet.favorite_count, tweet.place, tweet.coordinates] for tweet in alltweets]

        # extract hashtag and add into list
        for i in range(len(full_tweets)):
            if len(full_tweets[i][5]["hashtags"]) > 0:
                number_hashtags = len(full_tweets[i][5]["hashtags"])
                hashtag_list = []
                for j in range(number_hashtags):
                    new_hashtag = full_tweets[i][5]["hashtags"][j]["text"].encode("utf-8")
                    hashtag_list.append(new_hashtag)

                full_tweets[i][4] = hashtag_list
                # print(full_tweets[i][5]["hashtags"])


        
        # write the csv per user    
        # with open('/mnt/sdb1/leslie_results/data/tweets/%s_tweets.csv' % user_id, 'wb') as f:
        #     writer = csv.writer(f)
        #     writer.writerow(["id_str", "created_at", "text", "entities", "retweet_count", "favorite_count", "place", "coordinates"])
        #     writer.writerows(outtweets)

        #write the tweet file csv    
        with open('/mnt/sdb1/leslie_results/data/all_tweets.csv', 'a+') as g:
            writer = csv.writer(g)
            writer.writerows(full_tweets)
    
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
    
