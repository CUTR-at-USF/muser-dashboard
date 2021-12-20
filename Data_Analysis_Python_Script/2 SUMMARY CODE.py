#!/usr/bin/env python
# coding: utf-8

# In[1]:


#importing the dependencies
import pandas as pd
import numpy as np
import time
import datetime
import os
from datetime import datetime as dt
from glob import glob


# Set display to max for rows and columns
pd.set_option('display.max_rows', None)
pd.set_option("display.max_columns", None)


# In[27]:


user_behavior_data = pd.read_csv(r'D:\Shyam\Documents\MUSER\Analysis\Cleaned Data\muser-user-master.csv',encoding='utf-8-sig')


# In[28]:


# 1.0
# Create a dataframe to Group By features
# This one is grouped by User_ID then Song Features
user_song_group = user_behavior_data.groupby(["user_id","song_artist_name","song_album_name", "song_name"])


# In[29]:


# Function to calculate feature metrics
def my_agg(x):
    names = {
        'Elapsed Time Sum': x['elapsed_time_in_min'].sum(),
        'Half-Song Listening Time': x['player_half_song'].sum(),
        'Count of Songs': x['song_name'].count(),#nunique()
        'Count of Half-Songs': x['player_half_song'].nunique(),
        'Start Date': x['date'].min(),
        'End Date': x['date'].max(),
        'Date Unique Count': x['date'].nunique()}

    return pd.Series(names, index=['Elapsed Time Sum', 'Half-Song Listening Time', 'Count of Songs','Count of Half-Songs','Start Date', 'End Date', 'Date Unique Count'])

user_song_group.apply(my_agg)


# In[30]:


# Save the grouped by results in a dataframe
user_song_group = user_song_group.apply(my_agg)

# Sort values by user_id ascending
user_behavior_data.sort_values('user_id', ascending=True, inplace=True, ignore_index = True)


# In[31]:


# 2.0
# creating a second group by dataframe
# This one is grouped by User_Id and then Date followed by Songs Features
user_date_group = user_behavior_data.groupby(["user_id","date","song_name","song_artist_name","song_album_name"])


# In[32]:


# Function to calculate feature metrics
def my_agg(x):
    names = {
        'Elapsed Time Sum': x['elapsed_time_in_min'].sum(),
        'Half-Song Listening Time': x['player_half_song'].sum(),
        'Count of Songs': x['song_name'].count(),#nunique()
        'Count of Half-Songs': x['player_half_song'].nunique()}
    
    return pd.Series(names, index=['Elapsed Time Sum', 'Half-Song Listening Time', 'Count of Songs','Count of Half-Songs'])

user_date_group.apply(my_agg)


# In[34]:


# Save the grouped by results in a dataframe
user_date_group = user_date_group.apply(my_agg)

# Sort values by user_id ascending
user_behavior_data.sort_values('user_id', ascending=True, inplace=True, ignore_index = True)


# In[35]:


# CSV EXPORT
path = r'D:\Shyam\Documents\MUSER\Analysis\Cleaned Data'

# This is to export the first dataframe as a CSV
user_song_group.to_csv(os.path.join(path,'user_song_group_summary.csv'), index=True, encoding='utf-8-sig')

# This is to export the second dataframe as a separate CSV because it is formatted differently
user_date_group.to_csv(os.path.join(path,'user_date_group_summary.csv'), index=True, encoding='utf-8-sig')


# In[ ]:




