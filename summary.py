import pandas as pd 
import numpy as np 
from datetime import datetime
import csv
import matplotlib.pyplot as plt


# IMPORT THE DATA
# TWEETS = pd.read_csv("/mnt/sdb1/leslie_results/data/all_tweets.csv")
# column_names_for_tweets = ["user_id_str", "tweet_id_str", "created_at", "text", "hashtags", "entities", "retweet_count", "favorite_count", "place", "coordinates"]
# TWEETS.columns = column_names_for_tweets


# column_names_for_tweets = ["user_id_str", "tweet_id_str", "created_at", "text", "hashtags", "entities", "retweet_count", "favorite_count", "place", "coordinates"]
# TWEETS.columns = column_names_for_tweets

USER = pd.read_csv("/mnt/sdb1/leslie_results/data/user.csv")
# SIMPLE_USER = pd.read_csv("/mnt/sdb1/leslie_results/data/deduped_user_for_summary.csv")
# print(SIMPLE_USER[SIMPLE_USER["statuses_count"]>1000000]["screen_name"])

# aaaaaaaaaaaaaaaaaaaaa


# old_tweets = pd.read_csv("/mnt/sdb1/leslie_results/data/swiss_over_tweeted_oldest_tweets.csv")








def identify_swiss_users(user_df):
    """
    Function to take a user dataframe and return subset to only include users in switzerland and speak the relevant languages. Also dedupes entries.
    For now will save it to a csv so i dont need to rerun this every single time.

    Args: user data frame
    Returns: pandas dataframe of only swiss users
    """
    
    # remove duplicates 
    user_df.drop_duplicates(subset="id_str", keep="first", inplace=True)
    
    # consider only languages in CH and non null values.
    lang_df = user_df[user_df["lang"].isin(["en", "de", "fr", "it"])]
    non_null_df = lang_df[-lang_df["location"].isna()]

    swiss_places = ["svizzera", "switzerland", "schweiz", "suisse", ", ch", "zurich", "bern", "geneva", "lausanne", "winterthur", "luzern", "st. gallen", "lugano", "basel", "biel", "bienne"]
    swiss_ids = []

    for user in non_null_df.index:
       
        print(user)
        loc = non_null_df["location"][user].lower()
        tmp_loc = []
        for i in range(len(swiss_places)):
            tmp_loc.append(swiss_places[i] in loc)
        if np.sum(np.array(tmp_loc)) > 0:
        	swiss_ids.append(non_null_df["id_str"][user])
    
    print("finished")     
    swiss_df = non_null_df[non_null_df["id_str"].isin(swiss_ids)]
    print(len(swiss_df))
    print(swiss_df[swiss_df["statuses_count"] > 1000000]["screen_name"])
    summary = pd.DataFrame({"id_str": swiss_df["id_str"], "location": swiss_df["location"]})
    summary = summary.groupby(["location"]).size().sort_values()
    print(summary)

    # save to a csv
    swiss_df.to_csv("/mnt/sdb1/leslie_results/data/deduped_user_for_summary_fixed.csv", index=False)
    summary.to_csv("/mnt/sdb1/leslie_results/data/locations_included_fixed.csv")

    return(swiss_df)
identify_swiss_users(user_df=USER)

aaaaaaaaaaaaaa

def oldest_tweets_histogram(tweet_df, user_df):
    """
    Function to show a histogram of the oldest tweet published by swiss users, when there 
    are > 3000 tweets (although it returns 3200 tweets from the API, I'm using 
    3000 to account for the fact that there may be future tweets).

    Args: user data frame, tweets data frame
    Returns: histogram
    """

    # subset user and tweet dataframe to only include columns we care about, for which we have downloaded tweet data
    
    user_simplified = pd.DataFrame({"id_str": user_df["id_str"], "statuses_count": user_df["statuses_count"]})

    over_tweeted = user_simplified[user_simplified["statuses_count"] >= 3200]
    print(over_tweeted.shape)
    no_tweets = user_simplified[user_simplified["statuses_count"] <= 1]
    print("no or one tweet", len(no_tweets))
    
    oldest_tweet = pd.DataFrame({"id_str": tweet_df["user_id_str"], "created_at": tweet_df["created_at"]}).groupby(["id_str"]).min()
    print("number of users tweets looked up", len(oldest_tweet))
    
    result = pd.merge(oldest_tweet, over_tweeted, left_on = "id_str", right_on="id_str")

    result.to_csv("/mnt/sdb1/leslie_results/data/swiss_over_tweeted_oldest_tweets.csv")
    print(result.shape)
    print(result.head)

    print("number of users with 0/1 tweets", "percent of all tweets")
    print("number of users with > 3000 tweets", "percent of all tweets")

    return(result)


def quantile_information(user_df, column_name):
	"""
	Function to return the quantile information of a given attribute.

	Inputs: user data frame, column name to collect data on given as a string
	Output: quantile information
	"""

	variable_of_interest = user_df[column_name]
	quantiles = np.quantile(variable_of_interest, q=[0.1,0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9])
	mean = np.mean(variable_of_interest)

	# print([format(quantiles[i], "0.1f") for i in range(len(quantiles))])
	print(column_name, np.min(variable_of_interest), np.max(variable_of_interest))

quantile_information(SIMPLE_USER, "friends_count")
quantile_information(SIMPLE_USER, "followers_count")
quantile_information(SIMPLE_USER, "statuses_count")
quantile_information(SIMPLE_USER[SIMPLE_USER["statuses_count"]>0], "statuses_count")
# quantile_information(SIMPLE_USER[SIMPLE_USER["statuses_count"]>3200], "created_at")


# print(SIMPLE_USER.shape)
# oldest_tweets_histogram(tweet_df= TWEETS, user_df= SIMPLE_USER)
# oldest = pd.read_csv("/mnt/sdb1/leslie_results/data/swiss_over_tweeted_oldest_tweets.csv").iloc[:,2:]

# shortened_date = []
# for i in range(len(oldest)):
#     shortened_date.append(oldest["created_at"][i][0:7])


# oldest_by_month = pd.DataFrame({"created_at": shortened_date, "statuses_count": oldest["statuses_count"]})

# # tweet_hist = oldest_by_month.groupby(["created_at"]).size().to_frame().sort_values(by="created_at", ascending=True).plot(kind="hist")
# plt.hist(oldest["statuses_count"], bins = [3000, 10000, 15000, 20000, 25000, 30000, 35000, 40000, 45000, 50000, 55000, 60000])
# plt.show()
# aaaaaaaaaaaaaaaa


# quantiles of 3200th tweets
# simple_old = old_tweets[old_tweets["statuses_count"]>3200]
# for i in range(len(simple_old)):
# 	simple_old.iloc[i,0] = simple_old.iloc[i,0][0:7]
# indices = np.round(5592 * np.array([0.1,0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]))
# ordered = np.sort(simple_old["created_at"])
# print(ordered.shape)
# print(ordered)
# for i in range(len(indices)):
# 	print(ordered[int(indices[i])])


