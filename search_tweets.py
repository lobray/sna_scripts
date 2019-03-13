import pandas as pandas
import numpy as np
from searchtweets import ResultStream, gen_rule_payload, load_credentials
from searchtweets import collect_results
import csv

import twitter_credentials

premium_search_args = load_credentials("~/.twitter_keys.yaml",
                                       account_type="premium")


rule = gen_rule_payload("selbstbestimmungsinitiative",
                        from_date="2018-09-01", #UTC 2017-09-01 00:00
                        to_date="2018-10-30",#UTC 2017-10-30 00:00
                        results_per_call=100) #for sandbox. can also add "count bucket"

print(rule)

# # HOW DOES THIS WORK WTIH PAGINATION?

rs = ResultStream(rule_payload=rule,
                  max_results=100, # for sandbox
                  max_pages=1,
                  # endpoint = "https://api.twitter.com/1.1/tweets/search/fullarchive/dev.json",
                  **premium_search_args)

print(rs)

tweets = list(rs.stream())
  


# using unidecode to prevent emoji/accents printing
full_tweets = [[tweet.all_text, tweet.created_at_datetime, tweet.created_at_string, tweet.hashtags, tweet.id, tweet.lang, tweet.screen_name, tweet.profile_location, tweet.retweet_count, tweet.favorite_count, tweet.text, tweet.tweet_type, tweet.user_id] for tweet in tweets]

with open('/mnt/sdb1/leslie_results/data/sbi.csv', 'a+') as g:
  writer = csv.writer(g)
  writer.writerows(full_tweets)


