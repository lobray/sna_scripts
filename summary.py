import pandas as pd 
import numpy as np 
from datetime import datetime
import csv
import matplotlib.pyplot as plt


# IMPORT THE DATA
# TWEETS = pd.read_csv("/mnt/sdb1/leslie_results/data/all_tweets.csv")
# column_names_for_tweets = ["user_id_str", "tweet_id_str", "created_at", "text", "hashtags", "entities", "retweet_count", "favorite_count", "place", "coordinates"]
# TWEETS.columns = column_names_for_tweets

# USER = pd.read_csv("/mnt/sdb1/leslie_results/data/user.csv")
# SIMPLE_USER = pd.read_csv("/mnt/sdb1/leslie_results/data/deduped_user_for_summary.csv")

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

    swiss_places = ["svizzera", "switzerland", "schweiz", "suisse", "ch", "zurich", "bern", "geneva", "lausanne", "winterthur", "luzern", "st. gallen", "lugano", "basel", "biel", "bienne"]
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

    # save to a csv
    swiss_df.to_csv("/mnt/sdb1/leslie_results/data/deduped_user_for_summary.csv", index=False)

    return(swiss_df)


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




# print(SIMPLE_USER.shape)
# oldest_tweets_histogram(tweet_df= TWEETS, user_df= SIMPLE_USER)
oldest = pd.read_csv("/mnt/sdb1/leslie_results/data/swiss_over_tweeted_oldest_tweets.csv").iloc[:,2:]

shortened_date = []
for i in range(len(oldest)):
    shortened_date.append(oldest["created_at"][i][0:7])


oldest_by_month = pd.DataFrame({"created_at": shortened_date, "statuses_count": oldest["statuses_count"]})

# tweet_hist = oldest_by_month.groupby(["created_at"]).size().to_frame().sort_values(by="created_at", ascending=True).plot(kind="hist")
plt.hist(oldest["statuses_count"], bins = [3000, 10000, 15000, 20000, 25000, 30000, 35000, 40000, 45000, 50000, 55000, 60000])
plt.show()
# aaaaaaaaaaaaaaaa

def visualize(df, column_name='start_date', color='#494949', title=''):
    """
    Visualize a dataframe with a date column.

    Parameters
    ----------
    df : Pandas dataframe
    column_name : str
        Column to visualize
    color : str
    title : str
    """
    plt.figure(figsize=(20, 10))
    ax = (df[column_name].groupby(df[column_name].dt.month)
                         .count()).plot(kind="bar", color=color)
    ax.set_facecolor('#eeeeee')
    ax.set_xlabel("hour of the day")
    ax.set_ylabel("count")
    ax.set_title(title)
    plt.show()

# visualize(oldest, column_name="created_at", color="#494949", title="test")
# oldest["created_at"] = oldest["created_at"].astype("datetime64")
# oldest.groupby(oldest["created_at"].dt.month).count().plot(kind="bar")
# plt.hist(oldest, bins='auto')  # arguments are passed to np.histogram
# plt.title("Histogram with 'auto' bins")
# plt.show()