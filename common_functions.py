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


def dedupe_input_ids(new_ids, old_ids):
    '''
    Takes input ids and checks against the list of previously crawled ids to prevent crawling the same node twice.

    Args: new ids to consider and old ids already crawled
    Returns: list of IDs to crawl
    '''
    new_input_ids = list(new_ids)
    for i in range(len(new_input_ids)):
        new_input_ids[i] = str(new_input_ids[i])
    new_input_ids = set(new_input_ids)

    previously_crawled_ids = list(old_ids)
    for i in range(len(previously_crawled_ids)):
        previously_crawled_ids[i] = str(previously_crawled_ids[i])
    previously_crawled_ids = set(previously_crawled_ids)

    set_to_crawl = list(new_input_ids - previously_crawled_ids)
    
    ids_to_crawl = []
    
    # fix any errors
    for i in range(len(set_to_crawl)):
        try:
            id_int = np.int64(set_to_crawl[i])
            ids_to_crawl.append(id_int)
        except:
            continue

    print("number of new ids (swiss+non swiss):", len(ids_to_crawl))
    return(ids_to_crawl)


def return_swiss_ids(input_ids, old_ids, user_df):
    '''
    Function to find which users are located in Switzerland. Make sure account isn't protected.

    Args: input_ids should be the output from deduped list
    Returns: list of ids that are in Switzerland and have not been crawled 
    '''
    user_df.drop_duplicates(subset="id_str", keep="first", inplace=True)

    ids_to_crawl = set(dedupe_input_ids(new_ids=input_ids, old_ids=old_ids))
    swiss_places = ["svizzera", "switzerland", "schweiz", "suisse", "swiss", ", ch", "zurich", "zuerich", "bern", "geneva", "geneve", "lausanne", "winterthur", "luzern", "st. gallen", "st.gallen", "lugano", "basel", "biel", "bienne", "zug", "aarau"]
    
    swiss_users_list = []

    lang_df = user_df[user_df["lang"].isin(["en", "de", "fr", "it"])]
    non_null_df = lang_df[-lang_df["location"].isna()]

    for user in non_null_df.index:
        # print(user)
        loc = non_null_df["location"][user].lower()
        tmp_loc = []
        for i in range(len(swiss_places)):
            tmp_loc.append(swiss_places[i] in loc)
        if np.sum(np.array(tmp_loc)) > 0:
            if non_null_df["protected"][user] == False:
                if "china" in loc:
                    continue
                if "chicago" in loc:
                    continue
                else:
                    swiss_users_list.append(non_null_df["id_str"][user])
        

    swiss_ids = set(swiss_users_list)

    new_swiss_ids = list(ids_to_crawl.intersection(swiss_ids))

    for i in range(len(new_swiss_ids)):
    	new_swiss_ids[i] = np.int64(new_swiss_ids[i])

    print("new input swiss ids:", len(new_swiss_ids))

    return(new_swiss_ids)