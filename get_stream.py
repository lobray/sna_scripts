from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import tweepy
import pandas as pd
import csv
import time
import datetime
import sys
reload(sys)

import twitter_credentials

sys.setdefaultencoding('utf8')


auth = tweepy.OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)

for tweet in tweepy.Cursor(api.search,
                       q="#frauentag",
                       count=500,
                       result_type="recent",
                       include_entities=True,
                       tweet_mode="extended").items():
    tweet_info = pd.DataFrame(data=[tweet.id_str], columns=["tweet_id_str"])
    tweet_info["user_id_str"] = tweet.user.id_str
    tweet_info["created_at"] = tweet.created_at
    tweet_info["text"] = tweet.full_text
    tweet_info["hashtags"] = ""
    tweet_info["retweet_count"] = tweet.retweet_count
    tweet_info["favorite_count"] = tweet.favorite_count
    tweet_info["place"] = tweet.place
    tweet_info["coordinates"] = tweet.coordinates
    tweet_info["geo"] = tweet.geo
    tweet_info["lang"] = tweet.lang

    # [tweet.id_str, tweet.user.id_str, tweet.created_at, tweet.full_text.encode("utf-8"), "", tweet.retweet_count, tweet.favorite_count, tweet.place, tweet.coordinates, tweet.geo, tweet.lang]
    if len(tweet.entities["hashtags"]) > 0:
    	hashtag_list = []
    	number_hashtags = len(tweet.entities["hashtags"])

    	for i in range(number_hashtags):
    		print(tweet.entities["hashtags"][i]["text"])
    		print(type(tweet.entities["hashtags"][i]["text"]))
    		hashtag_list.append(tweet.entities["hashtags"][i]["text"])

    	print(hashtag_list)

    	tweet_info["hashtags"] = [hashtag_list]

        # tweet_info["hashtags"] = tweet.entities["hashtags"][0]["text"].encode("utf-8")

    # print(tweet_info)
    with open("/mnt/sdb1/leslie_results/data/searched_tweets.csv", 'a+') as tf:
        tweet_info.to_csv(tf, index=False, header=False)
    time.sleep(3)
    print(datetime.datetime.now())


# # # # TWITTER STREAMER # # # #
# class TwitterStreamer():
#     """
#     Class for streaming and processing live tweets.
#     """
#     def __init__(self):
#         pass

#     def stream_tweets(self, fetched_tweets_filename, hash_tag_list):
#         # This handles Twitter authetification and the connection to Twitter Streaming API
#         listener = StdOutListener(fetched_tweets_filename)
#         auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
#         auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
#         stream = Stream(auth, listener)

#         # This line filter Twitter Streams to capture data by the keywords: 
#         stream.filter(track=hash_tag_list)


# # # # # TWITTER STREAM LISTENER # # # #
# class StdOutListener(StreamListener):
#     """
#     This is a basic listener that just prints received tweets to stdout.
#     """
#     def __init__(self, fetched_tweets_filename):
#         self.fetched_tweets_filename = fetched_tweets_filename

#     def on_data(self, data):
#         try:
#             print(type(data))
#             print(len(data))

#             # tweet_data = [[tweet.user[0], tweet.id_str, tweet.created_at, tweet.full_text.encode("utf-8"), "", tweet.entities, tweet.user[1], tweet.retweet_count, tweet.favorite_count, tweet.place, tweet.coordinates] for tweet in data]

#             # id_str = data.id_str
#             # created = data.created_at
#             # text = data.text
#             # fav = data.favorite_count
#             # name = data.user.screen_name
#             # description = data.user.description
#             # loc = data.user.location
#             # user_created = data.user.created_at
#             # followers = data.user.followers_count

#              # extract hashtag and add into list
#             # for i in range(len(tweet_data)):
# 	           #  if len(tweet_data[i][5]["hashtags"]) > 0:
# 	           #      number_hashtags = len(tweet_data[i][5]["hashtags"])
# 	           #      hashtag_list = []
# 	           #      for j in range(number_hashtags):
# 	           #          new_hashtag = tweet_data[i][5]["hashtags"][j]["text"].encode("utf-8")
# 	           #          hashtag_list.append(new_hashtag)

# 	           #      tweet_data[i][4] = hashtag_list

#             # with open(self.fetched_tweets_filename, 'a+') as tf:
#             #     # tf.write(data)
#             #     writer = csv.writer(tf)
#       	    	# writer.writerows(tweet_data)
#             return(True)
#         except BaseException as e:
#             print("Error on_data %s" % str(e))
#         return(True)
          

#     def on_error(self, status):
#         print(status)

 
# if __name__ == '__main__':
 
#     # Authenticate using config.py and connect to Twitter Streaming API.
#     hash_tag_list = ["frauentag", "tagderfrau"]
#     fetched_tweets_filename = "/mnt/sdb1/leslie_results/data/searched_tweets.csv"

#     twitter_streamer = TwitterStreamer()
#     twitter_streamer.stream_tweets(fetched_tweets_filename, hash_tag_list)
    