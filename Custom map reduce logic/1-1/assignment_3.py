import sys
from datetime import datetime
from map_reduce_lib import *
import re

SMALL_INPUT_NAME = "_small"

def mapper(input):
    inputs = input[0].split(",")

    # Case 1: Process play history data
    if len(inputs) == 3 and inputs[0] != "track_id":
        track_id, _, date = inputs
        datetime_obj = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        hour = input[1]
        if datetime_obj.hour != hour:
            return
    
        # Return a key-value pair where the track_id is the key, and the value is 1 (indicating a play)
        return [(track_id, 1)]
    
    # Case 2: Process track data (4+ columns: track_id, artist, title, and other details)
    elif len(inputs) >= 4 and inputs[0] != "track_id":
        # Use regular expression to extract quoted fields and non-quoted fields (to handle quotes in the CSV)
        track_id, artist, title, _ = re.findall(r'"([^"]*(?:""[^"]*)*)"|([^,]+)', input[0])
        track_id = track_id[1]
        artist = artist[1]
        title = title[1]

        # Return a key-value pair where the track_id is the key and the value is a list containing artist and title
        return [(track_id, [artist, title])]
    
def reducer(key_values):    
    track_id, vals = key_values
    plays = 0
    song_name, artist = '', ''

    for val in vals:
        if type(val) is int: # If the value is an integer (representing a play)
            plays += val
        elif type(val) is list: # If the value is a list (representing artist and song name)
            artist = val[0]
            song_name = val[1]

    # Create the result tuple containing the total number of plays, track_id, song name, and artist
    result = (plays, track_id, song_name, artist)
    return result

if __name__ == '__main__':
    useSmallInput = True
    if len(sys.argv) > 1 and "false" in sys.argv[1].lower():
        useSmallInput = False
    
    files = ['playhistory', 'tracks']
    inputs = read_files([f'data/{file}{SMALL_INPUT_NAME if useSmallInput else ""}.csv' for file in files])

    hour = 8
    inputs = [(line, hour) for (_, line) in inputs]

    map_reduce = MapReduce(mapper, reducer, 8)
    
    results = map_reduce(inputs, chunksize=16, debug=True)

    results = results[-5:]

    for result in results:
        print(result)
