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

FILE = "/mnt/sdb1/leslie_results/data/user.csv"
USER_INFO = pd.read_csv(FILE)



def return_unaccounted_for_locations(user_df):
    '''
    Function to list locations that have not been included in our search of twitter users. 

    Args: dataframe of user info
    Returns: saves the result to a csv
    '''

    # places = USER_INFO.loc[-USER_INFO["location"].isna()]["location"]

    # now remove the onces that we've already account for
    swiss_places = ["svizzera", "switzerland", "schweiz", "suisse", ", ch", "zurich", "bern", "geneva", "lausanne", "winterthur", "luzern", "st. gallen", "lugano", "basel", "biel", "bienne"]
    swiss_users_list = []
    swiss_ids = []

    number_swiss_users = 0

    print("all users size, null location size", user_df.shape, user_df[user_df["location"].isna()].shape)

    lang_df = user_df[user_df["lang"].isin(["en", "de", "fr", "it"])]
    non_null_df = lang_df[-lang_df["location"].isna()]

    print("lang df size then not null size", lang_df.shape, non_null_df.shape)
       
    for user in non_null_df.index:
        # print(user)
        loc = non_null_df["location"][user].lower()
        tmp_loc = []
        for i in range(len(swiss_places)):
            tmp_loc.append(swiss_places[i] in loc)
        if np.sum(np.array(tmp_loc)) == 0:
            swiss_users_list.append(non_null_df["location"][user])
        if np.sum(np.array(tmp_loc)) > 0:
            if "china" in loc:
                pass
            if "chicago" in loc:
                pass
            else:
                number_swiss_users += 1
            # print(swiss_places[tmp_loc.index(1)]) # print locations as a gut check
                swiss_ids.append(non_null_df["id_str"][user])

    print("number swiss users looked up:", number_swiss_users)
    print("number unique swiss ids:", len(set(swiss_ids)))
    print("number unique locations not included", len(set(swiss_users_list)))

    unaccounted_for_locations = pd.DataFrame(data=swiss_users_list, columns = ["locations"])
    unaccounted_for_locations = unaccounted_for_locations.groupby("locations").size()
    unaccounted_for_locations.to_csv("/mnt/sdb1/leslie_results/data/locations.csv")    


return_unaccounted_for_locations(user_df = USER_INFO)
    
