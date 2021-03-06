import pandas as pandas
import numpy as np
from searchtweets import ResultStream, gen_rule_payload, load_credentials
from searchtweets import collect_results
import csv

import twitter_credentials
from common_functions import *

premium_search_args = load_credentials("~/.twitter_keys.yaml",
                                       account_type="premium")

rule = gen_rule_payload("selbstbestimmungsinitiative",
                        from_date="2018-09-01", #UTC 2017-09-01 00:00
                        to_date="2018-10-30",#UTC 2017-10-30 00:00
                        results_per_call=100) #100 for sandbox, 500 when paid. can also add "count bucket"

print(rule)

rs = ResultStream(rule_payload=rule,
                  max_results=100, #100 for sandbox, 500 for paid
                  max_pages=1,
                  **premium_search_args)

print(rs)

tweets = list(rs.stream())
  
# using unidecode to prevent emoji/accents printing
full_tweets = [[tweet.user_id, tweet.id, tweet.all_text, tweet.created_at_datetime, tweet.created_at_string, tweet.hashtags, tweet.tweet_type, tweet.lang, tweet.screen_name, tweet.profile_location, tweet.retweet_count, tweet.favorite_count, tweet.text] for tweet in tweets]

with open('/mnt/sdb1/leslie_results/data/sbi.csv', 'a+') as g:
  writer = csv.writer(g)
  writer.writerows(full_tweets)


