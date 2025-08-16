import sys
from collections import defaultdict, Counter
from datetime import datetime
from map_reduce_lib import *
import re

SMALL_INPUT_NAME = "_small"

# Split the lines and only get the required data
def mapper_filter_data(input):
    inputs = input.split(",")
    
    if(len(inputs) == 7 and inputs[0].isnumeric()): # people line
        user_id, first_name, lastname, _, _, _, _, = inputs
        return[(user_id, (first_name, lastname))]
    elif(len(inputs) == 3 and inputs[0] != 'track_id'): # playhistory line
        track_id, user_id, _ = inputs
        return[(track_id, user_id)]
    elif(len(inputs) >= 4 and 'id' not in inputs[0]): # tracks line
        # Some column values have a comma in them, breaking the split
        # The data set solves this by putting these specific values in quotes
        # But some values also have a quote in them...
        # This regex makes sure a value is always seen as one
        track_id_match, artist_match, _, _ = re.findall(r'"([^"]*(?:""[^"]*)*)"|([^,]+)', input)
        track_id = track_id_match[1]
        artist = artist_match[1]
        return[(track_id, artist)]

# For tracks, return its artist with the users that listened to it
def reducer_artist_user_listens(key_values):
    if key_values[0].isnumeric(): # user_id
        return (key_values[0], key_values[1][0])
    else: # track_id
        artist = ''
        # We swap users with artist to artist with users to decrease the documents to 1
        # And to decrease the amount of strings (artist name)
        user_ids = []
        for value in key_values[1]:
            if value.isnumeric():
                user_ids.append(value)
            else:
                artist = value

        return(artist, user_ids)

# For artists, switch back to user with artist as the reducer needs it by user
def mapper_user_artist(input):
    if(input[0].isnumeric()): # user_id
        return [input]
    else: # artist
        listened_artist_by_user = []
        for user in input[1]:
            listened_artist_by_user.append((user, input[0]))
        return listened_artist_by_user
        
# Calcs the most listened artist by user
def reducer_user_most_listened_artist(key_values):
    first_name = ''
    last_name = ''
    artists = {}
    for value in key_values[1]:
        if(isinstance(value, tuple)):
            first_name = value[0]
            last_name = value[1]
        else:
            if value not in artists:
                artists[value] = 0
            artists[value] += 1
    
    most_listened_artist, listenCount = max(artists.items(), key=lambda kv: kv[1])
            
    return (first_name, last_name, most_listened_artist, listenCount)

if __name__ == '__main__':
    useSmallInput = True
    if len(sys.argv) > 1 and "false" in sys.argv[1].lower():
        useSmallInput = False
    
    files = ['playhistory', 'tracks', 'people']
    inputs = read_files([f'data/{file}{SMALL_INPUT_NAME if useSmallInput else ""}.csv' for file in files])
    
    inputs = [(line) for (_, line) in inputs]
    
    map_reduce_1 = MapReduce(mapper_filter_data, reducer_artist_user_listens, num_workers=8)
    intermediate_results = map_reduce_1(inputs, chunksize=16, debug=True)
    
    map_reduce_2 = MapReduce(mapper_user_artist, reducer_user_most_listened_artist, num_workers=8)
    results = map_reduce_2(intermediate_results, chunksize=16, debug=True)
    
    for result in results:
        print(result)
