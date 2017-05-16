import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
%matplotlib inline

events = pd.read_csv("../Anaconda3/data/events.csv", dtype=np.int32, index_col=0, usecols=[0,3])
events.head(25)
train = pd.merge(pd.read_csv("../Anaconda3/data/clicks_train.csv", dtype=np.int32, index_col=0).sample(frac=0.1),
                 events, left_index=True, right_index=True)
train.head(20)
train["hour"] = (train.timestamp // (3600 * 1000)) % 24
train["day"] = train.timestamp // (3600 * 24 * 1000)
train.head(30)
train["hour"] = (train.timestamp // (3600 * 1000)) % 24
train["day"] = train.timestamp // (3600 * 24 * 1000)
plt.figure(figsize=(12,4))
train.hour.hist(bins=np.linspace(-0.5, 23.5, 25), label="train", facecolor='blue',alpha=0.7, normed=True)
plt.xlim(-0.5, 23.5)
plt.legend(loc="best")
plt.xlabel("Hour of Day")
plt.ylabel("Fraction of Events")
plt.figure(figsize=(12,4))
train.day.hist(bins=np.linspace(-.5, 14.5, 16), label="train", facecolor='blue',alpha=0.7, normed=True)
plt.xlim(-0.5, 14.5)
plt.legend(loc="best")
plt.xlabel("Days since June 14")
plt.ylabel("Fraction of Events")
plt.figure(figsize=(12,6))
hour_day_counts = train.groupby(["hour", "day"]).count().ad_id.values.reshape(24,-1)
# plot 2d hist in days and hours, with each day normalised to 1 
plt.imshow((hour_day_counts / hour_day_counts.sum(axis=0)).T,
           interpolation="none", cmap="jet")
plt.colorbar()
plt.xlabel("Hour of Day")
plt.ylabel("Days since June 14")