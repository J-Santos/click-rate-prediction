import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import os
import gc # We're gonna be clearing memory a lot
import matplotlib.pyplot as plt
import seaborn as sns
%matplotlib inline

p = sns.color_palette()

print('# File sizes')
for f in os.listdir('C:/Users/shishira_aitha/Anaconda3/'):
    if 'zip' not in f:
        print(f.ljust(30) + str(round(os.path.getsize('C:/Users/shishira_aitha/Anaconda3/' + f) / 1000000, 2)) + 'MB')
events = pd.read_csv("C:/Users/shishira_aitha/Anaconda3/data/events.csv", dtype=np.str, index_col=0, usecols=[0,1])
events.head(10)

test = pd.merge(pd.read_csv("C:/Users/shishira_aitha/Anaconda3/data/clicks_test.csv", dtype=np.int32, index_col=0).sample(frac=0.1),
                 events, left_index=True, right_index=True)
uuid_counts = events.groupby('uuid')['uuid'].count().sort_values()

print(uuid_counts.tail(10))

for i in [2, 5, 10]:
    print('Users that appear less than {} times: {}%'.format(i, round((uuid_counts < i).mean() * 100, 2)))


plt.figure(figsize=(12, 4))
plt.hist(uuid_counts.values, bins=50, log=True)
plt.xlabel('Number of times user appeared in set', fontsize=13)
plt.ylabel('No of Users', fontsize=13)
plt.show()