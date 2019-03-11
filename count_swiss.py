import tweepy
import csv
import pandas as pd
import numpy as np
import json
import time
import datetime

import twitter_credentials

FILE = "/mnt/sdb1/leslie_results/data/user.csv"
USER_INFO = pd.read_csv(FILE)
INPUT_IDS = USER_INFO.iloc[:,1]


def count_swiss_ids(input_ids, user_df):
    '''
    Function to find which users are located in Switzerland.

    Args: input_ids from user.csv file
    Returns: count of ids that are in Switzerland  
    '''
    ids_to_crawl = input_ids
    
    swiss_places = ["svizzera", "switzerland", "schweiz", "suisse", "ch", "zurich", "bern", "geneva", "lausanne", "winterthur", "luzern", "st. gallen", "lugano", "basel", "biel", "bienne"]
    
    swiss_users_list = []

    lang_df = user_df[user_df["lang"].isin(["en", "de", "fr", "it"])]
    non_null_df = lang_df[-lang_df["location"].isna()]
    non_null_df_all_lang = user_df[-user_df["location"].isna()]
    
    too_many_tweets = 0 
    for user in non_null_df.index:
        print(user)
        loc = non_null_df["location"][user].lower()
        tmp_loc = []
        for i in range(len(swiss_places)):
            tmp_loc.append(swiss_places[i] in loc)
        if np.sum(np.array(tmp_loc)) > 0:
            # add in extra condition that status count needs to be greater than zero
            if non_null_df["statuses_count"][user] > 0:
                # add in condition that tweets are not protected
                if non_null_df["protected"][user] == False:

                    swiss_users_list.append(non_null_df["id_str"][user])
                    
                    if non_null_df["statuses_count"][user] == 1:
                        too_many_tweets += 1

    swiss_ids = set(swiss_users_list)
    
    # print summary statistics
    print("number of new swiss ids:", len(swiss_ids))
    print("nonnull location, all languages", len(non_null_df_all_lang))
    print("nonnull location, specified languages", len(non_null_df))
    print("number of statuses greater than 3200", too_many_tweets)

    return(swiss_ids)

count_swiss_ids(input_ids=INPUT_IDS, user_df=USER_INFO)
