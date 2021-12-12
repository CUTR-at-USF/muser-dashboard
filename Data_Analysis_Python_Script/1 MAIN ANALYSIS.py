#!/usr/bin/env python
# coding: utf-8

# In[26]:


#importing the dependencies
import pandas as pd
import numpy as np
import time
import datetime
import os
from datetime import datetime as dt
from glob import glob


# In[27]:


# Set display to max for rows and columns
pd.set_option('display.max_rows', None)
pd.set_option("display.max_columns", None)


# In[28]:


df = pd.read_csv(r'D:\Shyam\Documents\MUSER\Extraction\music-analysis.csv',encoding='latin-1')
df.head(5)


# In[29]:


#sorting the current timestamp in ascending order
df.sort_values('event_current_time_in_milliseconds', ascending=True, inplace=True, ignore_index = True)


# In[30]:


# Removing rows that were used for internal testing on user participant devices
df = df.drop(df.loc[26323:26353].index) # userid = 68737007 | EPQKvHaHiJgLKa5W2REgB1dWRss1
df = df.drop(df.loc[26380:26455].index) # userid = 68737004 that has two additional different muser user ids - JwhbWBRbEpcfhKGDieLk3hME0jW2 | f02Umk71rlgTLFMGEwB3z3B2pjq1
df = df.drop(df.loc[25660:25665].index) # userid = 68737003 | PUrt5cjxaQS6lAi2BdUpT3FrBE83
df = df.drop(df.loc[26354:26379].index) # userid = 68737003 | PUrt5cjxaQS6lAi2BdUpT3FrBE83
df = df.drop(df.loc[26480:26489].index) # userid = 68737003 | PUrt5cjxaQS6lAi2BdUpT3FrBE83


# In[31]:


#path = r'A:\MUSER\data\cleaned_data\mergedFiles\dummy\analysis_Code'
path = r'D:\Shyam\Documents\MUSER\Analysis\User Cleaned Data'

df.to_csv(os.path.join(path,'verify_muser_master.csv'), index=False, encoding='utf-8-sig')


# In[6]:


# changing date type from utc iso to different date type
df['event_current_time_other'] = pd.to_datetime(df['event_current_time_in_iso_utc'])


# In[7]:


#adding a new column event_current_time_ms_to_date to the csv 
df['event_current_time_ms_to_date'] = pd.to_datetime(df['event_current_time_in_milliseconds'], unit='ms')


# In[8]:


#creating column- elapsed_time_in_min 
df['elapsed_time_in_min'] = round(df['event_seek_position_in_milliseconds']/60000,4)
#df.dtypes


# In[9]:


#converting song_last_played to current timestamp
df['song_last_played_date'] = pd.to_datetime(df['song_last_played'], unit='ms')


# In[10]:


#remove the empty valued audio_volume_data column
df = df.dropna(axis=0, subset=['audio_volume_data'])
df.shape


# In[11]:


#function to extract minimum volume from an array of audio_volume_data
def pull_vol_min(i):
    a,b,c,d= i.split(',')
    return c


# In[12]:


df['min_volume'] =df['audio_volume_data'].apply(pull_vol_min)
df['min_volume'] = df['min_volume'].str.split('=').str[1]


# In[13]:


#function to extract current volume from an array of audio_volume_data
def pull_vol_curr(i):
    a,b,c,d,e= i.split()
    start = b.find('[')
    end = b.find(',')
    substring = b[start+1:end]
    return substring


# In[14]:


# apply the function and split the column by '='
df['curent_volume'] = df.audio_volume_data.apply(pull_vol_curr)
df['curent_volume'] = df['curent_volume'].str.split('=').str[1]


# In[15]:


#filtering out the records that are greater than the minimum volume value
df= df[ df['curent_volume'] > df['min_volume'] ]


# In[16]:


# Create new variable curr_time to change the current time into milliseconds
curr_time = pd.to_datetime(df['event_current_time_in_iso_utc'])

# Create new column curr_time_in_ms
df['curr_time_in_ms'] = pd.to_datetime(curr_time, unit='ms')


# In[17]:


# convert the new column to display milliseconds
df['curr_time_in_ms'] = df['curr_time_in_ms'].astype(np.int64) / int(1e6)


# In[18]:


#creating column- current time in minutes 
df['curr_time_in_min'] = round(df['curr_time_in_ms']/60000,4)


# In[19]:


#reset the indexes of the dataset
df.reset_index(inplace=True)


# In[20]:


# shorten the final file by only keeping required columns in the final csv file
user_behavior_data = df[["user_id", "record_id", "song_id", "song_name", "player_event_type", "ui_event_type",  "event_current_time_in_milliseconds", "event_seek_position_in_milliseconds", "song_album_name", "song_artist_name", "song_duration", "audio_volume_data","event_current_time_ms_to_date", "event_current_time_in_iso_utc", "event_current_time_other", "min_volume", "curent_volume", "elapsed_time_in_min","song_last_played_date", "song_play_count", "curr_time_in_ms"]]


# In[21]:


user_behavior_data.head(50)


# In[22]:


# Listening Time
# TIME SPENT BETWEEN PLAY AND PAUSE
for idx in range (1, len(user_behavior_data)):
  user = user_behavior_data.loc[idx, "user_id"]
  song = user_behavior_data.loc[idx, "song_id"]
  event_type = user_behavior_data.loc[idx, "player_event_type"]
  ui_event = user_behavior_data.loc[idx, "ui_event_type"]
  current_time = user_behavior_data.loc[idx, "curr_time_in_ms"]
  listening_time_new = 0
  pause_time = 0
  play_time = 0
  if user != user_behavior_data.loc[idx-1, "user_id"]:
    continue
  else:
    if (event_type != "PLAY" or event_type != "NEXT" or event_type != "PREV") and (user_behavior_data.loc[idx-1, "player_event_type"] == "NEXT" or user_behavior_data.loc[idx-1, "player_event_type"] == "PREV" or user_behavior_data.loc[idx-1, "player_event_type"] == "PLAY"):
        play_time = current_time - user_behavior_data.loc[idx-1, "curr_time_in_ms"]
    if (event_type != "PAUSE" or event_type != "SEEK") and (user_behavior_data.loc[idx-1, "player_event_type"] == "PAUSE" or user_behavior_data.loc[idx-1, "player_event_type"] == "SEEK"):
        pause_time = current_time - user_behavior_data.loc[idx-1, "curr_time_in_ms"]
  listening_time_new = play_time - pause_time
  user_behavior_data.loc[idx,'listening_time_new']=listening_time_new/(60000)
    
# Remove negative values and minutes that are miscalculated (usually over 1000)
user_behavior_data.listening_time_new = np.where(user_behavior_data.listening_time_new < 0, 0, user_behavior_data.listening_time_new)
user_behavior_data.listening_time_new = np.where(user_behavior_data.listening_time_new > 1000, 0, user_behavior_data.listening_time_new)

# Remove 0 and leave blanks
user_behavior_data['listening_time_new']=user_behavior_data['listening_time_new'].replace(0, np.nan)


# In[23]:


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


# In[24]:


## NEEDS WORK ###
## NOT WORKING ON ALL USERS AS EXPECTED ###
## NEED TO CONSIDER A PAUSE EVENT ON THE PLAYER_EVENT COLUMN
## THEN GO DOWN THE ROWS UNTIL PLAY EVENT APPEARS IN THE PLAYER_EVENT COLUMN
## THEN SUBTRACT THE TIME DIFFERENCE PAUSE TIME - PLAY TIME 
## THIS WILL SHOW HOW LONG THE USER WAS NOT LISTENING TO MUSIC FOR


# TIME SPENT IN BETWEEN PAUSE AND PLAY EVENTS
# When user was not listening music
for idx in range (1, len(user_behavior_data)):
  user = user_behavior_data.loc[idx, "user_id"]
  song = user_behavior_data.loc[idx, "song_id"]
  event_type = user_behavior_data.loc[idx, "player_event_type"]
  ui_event = user_behavior_data.loc[idx, "ui_event_type"]
  current_time = user_behavior_data.loc[idx, "curr_time_in_ms"]
  not_listening_time = 0
  pause_time = 0
  play_time = 0
  if user != user_behavior_data.loc[idx-1, "user_id"]:
    continue
  else:
    if (event_type != "PAUSE" or event_type != "SEEK") and (user_behavior_data.loc[idx-1, "player_event_type"] == "PAUSE" or user_behavior_data.loc[idx-1, "player_event_type"] == "SEEK"):
        pause_time = current_time - user_behavior_data.loc[idx-1, "curr_time_in_ms"]
    if (event_type != "PLAY" or event_type != "NEXT" or event_type != "PREV") and (user_behavior_data.loc[idx-1, "player_event_type"] == "NEXT" or user_behavior_data.loc[idx-1, "player_event_type"] == "PREV" or user_behavior_data.loc[idx-1, "player_event_type"] == "PLAY"):
        play_time = current_time - user_behavior_data.loc[idx-1, "curr_time_in_ms"]
            
  not_listening_time = pause_time - play_time
  user_behavior_data.loc[idx-1,'not_listening_time']=not_listening_time/(60000)
    
# Remove the negative values form the 'not_listening_time' column
user_behavior_data.not_listening_time = np.where(user_behavior_data.not_listening_time < 0, 0, user_behavior_data.not_listening_time)

# Remove 0 and replace them with blanks
user_behavior_data['not_listening_time']=user_behavior_data['not_listening_time'].replace(0, np.nan)


# In[25]:


#path = r'A:\MUSER\data\cleaned_data\mergedFiles\dummy\analysis_Code'
path = r'D:\Shyam\Documents\MUSER\Analysis\User Cleaned Data'

user_behavior_data.to_csv(os.path.join(path,'muser_master.csv'), index=False, encoding='utf-8-sig')


# In[ ]:





# In[ ]:



