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
	    if np.sum(np.array(tmp_loc)) == 0:
	        swiss_users_list.append(non_null_df["location"][user])

	unaccounted_for_locations = pd.DataFrame(data=set(swiss_users_list))
	unaccounted_for_locations.to_csv("/mnt/sdb1/leslie_results/data/locations.csv")    


return_unaccounted_for_locations(user_df = USER_INFO)
    
