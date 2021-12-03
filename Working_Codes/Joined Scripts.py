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


# In[2]:


# Set display to max for rows and columns
pd.set_option('display.max_rows', None)
pd.set_option("display.max_columns", None)


# In[4]:


df = pd.read_csv(r'D:\Shyam\Documents\MUSER\Extraction\music-analysis.csv',encoding='latin-1')
df.head(5)


# In[5]:


#sorting the current timestamp in ascending order
df.sort_values('event_current_time_in_milliseconds', ascending=True, inplace=True, ignore_index = True)


# In[6]:


# Removing rows that were used for internal testing on user participant devices
df = df.drop(df.loc[26323:26353].index) # userid = 68737007
df = df.drop(df.loc[26380:26455].index) # userid = 68737004 that has two additional different muser user ids 
df = df.drop(df.loc[25660:25665].index) # userid = 68737003 
df = df.drop(df.loc[26354:26379].index) # userid = 68737003


# In[7]:


# changing date type from utc iso to different date type
df['event_current_time_other'] = pd.to_datetime(df['event_current_time_in_iso_utc'])


# In[8]:


#adding a new column event_current_time_ms_to_date to the csv 
df['event_current_time_ms_to_date'] = pd.to_datetime(df['event_current_time_in_milliseconds'], unit='ms')


# In[9]:


#creating column- elapsed_time_in_min 
df['elapsed_time_in_min'] = round(df['event_seek_position_in_milliseconds']/60000,4)
#df.dtypes


# In[10]:


#converting song_last_played to current timestamp
df['song_last_played_date'] = pd.to_datetime(df['song_last_played'], unit='ms')


# In[11]:


#remove the empty valued audio_volume_data column
df = df.dropna(axis=0, subset=['audio_volume_data'])
df.shape


# In[12]:


#function to extract minimum volume from an array of audio_volume_data
def pull_vol_min(i):
    a,b,c,d= i.split(',')
    return c


# In[13]:


df['min_volume'] =df['audio_volume_data'].apply(pull_vol_min)
df['min_volume'] = df['min_volume'].str.split('=').str[1]


# In[14]:


#function to extract current volume from an array of audio_volume_data
def pull_vol_curr(i):
    a,b,c,d,e= i.split()
    start = b.find('[')
    end = b.find(',')
    substring = b[start+1:end]
    return substring


# In[15]:


# apply the function and split the column by '='
df['curent_volume'] = df.audio_volume_data.apply(pull_vol_curr)
df['curent_volume'] = df['curent_volume'].str.split('=').str[1]


# In[16]:


#filtering out the records that are greater than the minimum volume value
df= df[ df['curent_volume'] > df['min_volume'] ]


# In[17]:


# Create new variable curr_time to change the current time into milliseconds
curr_time = pd.to_datetime(df['event_current_time_in_iso_utc'])

# Create new column curr_time_in_ms
df['curr_time_in_ms'] = pd.to_datetime(curr_time, unit='ms')


# In[18]:


# convert the new column to display milliseconds
df['curr_time_in_ms'] = df['curr_time_in_ms'].astype(np.int64) / int(1e6)


# In[19]:


#creating column- current time in minutes 
df['curr_time_in_min'] = round(df['curr_time_in_ms']/60000,4)


# In[20]:


#reset the indexes of the dataset
df.reset_index(inplace=True)


# In[21]:


# shorten the final file by only keeping required columns in the final csv file
user_behavior_data = df[["user_id", "record_id", "song_id", "song_name", "player_event_type", "ui_event_type",  "event_current_time_in_milliseconds", "event_seek_position_in_milliseconds", "song_album_name", "song_artist_name", "song_duration", "song_name", "audio_volume_data","event_current_time_ms_to_date", "event_current_time_in_iso_utc", "event_current_time_other", "min_volume", "curent_volume", "elapsed_time_in_min","song_last_played_date", "song_play_count"]]


# In[26]:


user_behavior_data.head(25)


# In[27]:


# LISTENING TIME

# Original Code
for idx in range(1, len(user_behavior_data)):
  user = user_behavior_data.loc[idx, "user_id"]
  song = user_behavior_data.loc[idx, "song_id"]
  event_seek_position= user_behavior_data.loc[idx, "event_seek_position_in_milliseconds"]
  event_type = user_behavior_data.loc[idx, "player_event_type"]
  trigger_time = user_behavior_data.loc[idx, "event_current_time_in_milliseconds"]
  listening_time = 0
  if user != user_behavior_data.loc[idx-1, "user_id"]:
    continue
  elif song != user_behavior_data.loc[idx-1, "song_id"]:
    continue
  else:
    if event_type != "PLAY" and user_behavior_data.loc[idx-1, "player_event_type"] == "PLAY": #this is covering same song pause-play
        listening_time = trigger_time - user_behavior_data.loc[idx-1, "event_current_time_in_milliseconds"]
        user_behavior_data.loc[idx, "listening_time"] = listening_time/(1000*60)
    if (event_type == "PAUSE" or event_type == "NEXT" or event_type == "PREV") and (user_behavior_data.loc[idx-1, "player_event_type"] == "SEEK" or user_behavior_data.loc[idx-1, "player_event_type"] == "PLAY"):#when idx-1 is play and idx is either play or next or prev
        listening_time = trigger_time - user_behavior_data.loc[idx-1, "event_current_time_in_milliseconds"]
        user_behavior_data.loc[idx, "listening_time"] = listening_time/(1000*60)


# In[ ]:


# USER EVENT

# Duration for half song duration
for idx in range(1, len(user_behavior_data)):
  user = user_behavior_data.loc[idx, "user_id"]
  song = user_behavior_data.loc[idx, "song_id"]
  event_seek_position= user_behavior_data.loc[idx, "event_seek_position_in_milliseconds"]
  event_type = user_behavior_data.loc[idx, "player_event_type"]
  ui_event = user_behavior_data.loc[idx, "ui_event_type"]
  trigger_time = user_behavior_data.loc[idx, "event_current_time_in_milliseconds"]
  song_duration_time = user_behavior_data.loc[idx, "song_duration"]
  event_seek = user_behavior_data.loc[idx, "event_seek_position_in_milliseconds"]
  user_half_song = 0  
  if user != user_behavior_data.loc[idx-1, "user_id"]:
    continue
  elif song != user_behavior_data.loc[idx-1, "song_id"]:
    continue
  else:
    if ui_event == "PLAY" and user_behavior_data.loc[idx, "ui_event_type"] == "PLAY":
        if user_behavior_data.loc[idx, "event_seek_position_in_milliseconds"] >= user_behavior_data.loc[idx, "song_duration"]/2:
            user_half_song = user_behavior_data.loc[idx, "event_seek_position_in_milliseconds"]
            user_behavior_data.loc[idx, "user_half_song"] = user_half_song/(1000*60)
    if (ui_event == "PAUSE" or ui_event == "NEXT" or ui_event == "PREV") and (user_behavior_data.loc[idx-1, "ui_event_type"] == "SEEK" or user_behavior_data.loc[idx-1, "ui_event_type"] == "PLAY"):
        if user_behavior_data.loc[idx, "event_seek_position_in_milliseconds"] >= user_behavior_data.loc[idx, "song_duration"]/2:
            user_half_song = user_behavior_data.loc[idx, "event_seek_position_in_milliseconds"]
            user_behavior_data.loc[idx, "user_half_song"] = user_half_song/(1000*60)


# In[28]:


# PLAYER EVENT TYPE

# Duration for half song duration
for idx in range(1, len(user_behavior_data)):
  user = user_behavior_data.loc[idx, "user_id"]
  song = user_behavior_data.loc[idx, "song_id"]
  event_seek_position= user_behavior_data.loc[idx, "event_seek_position_in_milliseconds"]
  event_type = user_behavior_data.loc[idx, "player_event_type"]
  ui_event = user_behavior_data.loc[idx, "ui_event_type"]
  trigger_time = user_behavior_data.loc[idx, "event_current_time_in_milliseconds"]
  song_duration_time = user_behavior_data.loc[idx, "song_duration"]
  event_seek = user_behavior_data.loc[idx, "event_seek_position_in_milliseconds"]
  player_half_song = 0  
  if user != user_behavior_data.loc[idx-1, "user_id"]:
    continue
  elif song != user_behavior_data.loc[idx-1, "song_id"]:
    continue
  else:
    if event_type != "PLAY" and user_behavior_data.loc[idx-1, "player_event_type"] == "PLAY":
        if user_behavior_data.loc[idx, "event_seek_position_in_milliseconds"] >= user_behavior_data.loc[idx, "song_duration"]/2:
            player_half_song = user_behavior_data.loc[idx, "event_seek_position_in_milliseconds"]
            user_behavior_data.loc[idx, "player_half_song"] = player_half_song/(1000*60)
    if (event_type == "PAUSE" or event_type == "NEXT" or event_type == "PREV") and (user_behavior_data.loc[idx-1, "player_event_type"] == "SEEK" or user_behavior_data.loc[idx-1, "player_event_type"] == "PLAY"):
        if user_behavior_data.loc[idx, "event_seek_position_in_milliseconds"] >= user_behavior_data.loc[idx, "song_duration"]/2:
            player_half_song = user_behavior_data.loc[idx, "event_seek_position_in_milliseconds"]
            user_behavior_data.loc[idx, "player_half_song"] = player_half_song/(1000*60)


# In[ ]:


# TIME SPENT IN BETWEEN PAUSE AND PLAY EVENTS

# CODE IN PROGRESS


# In[29]:


#path = r'A:\MUSER\data\cleaned_data\mergedFiles\dummy\analysis_Code'
path = r'D:\Shyam\Documents\MUSER\Analysis\User Cleaned Data'

user_behavior_data.to_csv(os.path.join(path,'muser_master.csv'), index=False, encoding='utf-8-sig')


# In[ ]:





# In[ ]:




