#this checks for the first song of the session where player_type(idx)= PLAY and playertype(idx-1)=PLAY with evet_seek_position =0 for both cases.

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
    #this checks for the first song of the session where player_type(idx)= PLAY and playertype(idx-1)=PLAY with evet_seek_position =0 for both cases.
    if event_type == "PLAY" and user_behavior_data.loc[idx-1, "player_event_type"] == "PLAY" and event_seek_position == 0 and user_behavior_data.loc[idx-1, "event_seek_position_in_milliseconds"] == 0:
        listening_time = trigger_time - user_behavior_data.loc[idx-1, "event_current_time_in_milliseconds"]
        user_behavior_data.loc[idx-1, "listening_time"] = listening_time/(1000*60)
