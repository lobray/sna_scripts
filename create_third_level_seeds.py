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

FRIENDS_OF_SECOND_LEVEL_SEED_SET = list(set(pd.read_csv("/mnt/sdb1/leslie_results/data/user-follower.csv")["user"]))
for i in range(len(FRIENDS_OF_SECOND_LEVEL_SEED_SET)):
	FRIENDS_OF_SECOND_LEVEL_SEED_SET[i] = str(int(FRIENDS_OF_SECOND_LEVEL_SEED_SET[i]))


ALREADY_CRAWLED = list(pd.read_csv("/mnt/sdb1/leslie_results/data/crawled_for_friends.csv"))
for i in range(1, len(ALREADY_CRAWLED)):
	ALREADY_CRAWLED[i] = str(int(ALREADY_CRAWLED[i]))

USER = pd.read_csv("/mnt/sdb1/leslie_results/data/user.csv")

third_level_set = return_swiss_ids(input_ids=FRIENDS_OF_SECOND_LEVEL_SEED_SET, old_ids=ALREADY_CRAWLED, user_df=USER)

for i in range(len(third_level_set)):
	third_level_set[i] = str(third_level_set[i])

third_level_set = pd.DataFrame(data=third_level_set)
print(len(third_level_set))
third_level_set.to_csv("/mnt/sdb1/leslie_results/data/third_level_seeds_switzerland.csv")
