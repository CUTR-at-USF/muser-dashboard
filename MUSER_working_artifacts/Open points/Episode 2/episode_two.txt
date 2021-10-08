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
  else: # this case works for idx-1 as PLAY and idx as PAUSE,PREV
    if event_type != "PLAY" and user_behavior_data.loc[idx-1, "player_event_type"] == "PLAY": #this is covering same song pause-play
        listening_time = trigger_time - user_behavior_data.loc[idx-1, "event_current_time_in_milliseconds"]
        user_behavior_data.loc[idx, "listening_time"] = listening_time/(1000*60)