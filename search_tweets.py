import pandas as pandas
import numpy as np
from searchtweets import ResultStream, gen_rule_payload, load_credentials
from searchtweets import collect_results


import twitter_credentials


premium_search_args = load_credentials("~/.twitter_keys.yaml",
                                       yaml_key="search_tweets_premium",
                                       env_overwrite=False)


rule = gen_rule_payload("from:jack",
                        from_date="2017-09-01", #UTC 2017-09-01 00:00
                        to_date="2017-10-30",#UTC 2017-10-30 00:00
                        results_per_call=100) #for sandbox. can also add "count bucket"
print(rule)

print(rule)

tweets = collect_results(rule,
                         max_results=100,
                         result_stream_args=premium_search_args) # change this if you need to - is this only for counts

[print(tweet.created_at_datetime) for tweet in tweets[0:10]]
[print(tweet.generator.get("name")) for tweet in tweets[0:10]]
# HOW DOES THIS WORK WTIH PAGINATION?

rs = ResultStream(rule_payload=rule,
                  max_results=100, # for sandbox
                  max_pages=1,
                  **premium_search_args)

print(rs)

tweets = list(rs.stream())

# using unidecode to prevent emoji/accents printing
[print(tweet.all_text) for tweet in tweets[0:10]];



# {'username': '<MY_USERNAME>',
#  'password': '<MY_PASSWORD>',
#  'endpoint': '<MY_ENDPOINT>'}


# {'bearer_token': '<A_VERY_LONG_MAGIC_STRING>',
#  'endpoint': 'https://api.twitter.com/1.1/tweets/search/30day/dev.json',
#  'extra_headers_dict': None}