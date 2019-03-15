
import time
import numpy as np
import pandas as pd
import datetime

import twitter_credentials
from common_functions import *

# USER = pd.read_csv("/mnt/sdb1/leslie_results/data/user.csv")
# seeds = np.array(pd.read_csv("/mnt/sdb1/leslie_results/data/second_level_seeds.csv", dtype=np.int64))[:,0]
# old = np.array([0])
# swiss = return_swiss_ids(input_ids = seeds, old_ids = old, user_df=USER)
# swiss_seeds = pd.DataFrame({"user": swiss})



# swiss_seeds = list(pd.read_csv("/mnt/sdb1/leslie_results/data/second_level_seeds_switzerland.csv", dtype="str").iloc[:,0])
# print(swiss_seeds)

# uf = pd.read_csv("/mnt/sdb1/leslie_results/data/user-follower.csv")
# print("all uf", len(uf))

# swiss_uf = uf[uf["follower"].isin(swiss_seeds)]
# print(swiss_uf)
# print(len(swiss_uf))

# with open("/mnt/sdb1/leslie_results/data/user-follower2.csv", 'a+') as f:
#     swiss_uf.to_csv(f, header=False, index=False)

uf2 = pd.read_csv("/mnt/sdb1/leslie_results/data/user-follower2.csv")
print(len(uf2))
uf2.drop_duplicates(["user", "follower"], inplace=True)
print(len(uf2))
uf2.to_csv("/mnt/sdb1/leslie_results/data/user-follower2.csv")

# for i in range(10):
	# print(type(swiss_seeds[i]))
# print(swiss_seeds)

# for i in range(len(swiss_seeds)):
# 	# swiss[i] = str(swiss[i])
# 	print(USER[USER["id_str"]==swiss_seeds[i]]["location"])


# print(len(swiss_seeds))
# swiss_crawled = crawled.merge(swiss_seeds, left_on="crawled", right_on="user")
# print(swiss_crawled.shape)


# USER_INFO = pd.read_csv("/mnt/sdb1/leslie_results/data/user.csv")
# # first identify swiss users
# seeds = np.array(pd.read_csv("/mnt/sdb1/leslie_results/data/second_level_seeds.csv").iloc[:,0])
# print(len(seeds))

# empty = np.array([1])

# swiss_seeds = return_swiss_ids(input_ids = seeds, old_ids = empty, user_df = USER_INFO)
# print(len(swiss_seeds))

# swiss_seeds = pd.DataFrame({"user": swiss_seeds})
# swiss_seeds.to_csv("/mnt/sdb1/leslie_results/data/second_level_seeds_switzerland.csv", index=False)


# uf = pd.read_csv("/mnt/sdb1/leslie_results/data/user-follower.csv")
# print(len(swiss_seeds))
# print(len(uf))

# swiss_uf = uf[uf["follower"].isin(swiss_seeds)]
