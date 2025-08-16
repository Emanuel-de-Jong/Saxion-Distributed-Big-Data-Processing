import sys
from datetime import datetime
from map_reduce_lib import *
from collections import Counter

SMALL_INPUT_NAME = "_small"

def mapper(line):
    inputs = line.split(",")

    # Case 1: Process play history data
    if len(inputs) == 3 and inputs[0] != "track_id":
        _, user_id, date = inputs
        datetime_obj = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        # Return the user_id and the hour extracted from the datetime as a key-value pair
        return [(user_id, datetime_obj.hour)]
    # Case 2: Process people data
    elif len(inputs) == 7 and inputs[0] != "id":
        id = inputs[0]
        first_name = inputs[1]
        last_name = inputs[2]
        # Return a key-value pair where the ID is the key and the names are the values
        return [(id, (first_name, last_name))]

def reducer(key_values):
    user_id, vals = key_values
    hours = []
    first_name, last_name = "", ""

    # Loop through all values for a given user_id
    for val in vals:
        if type(val) is int: # If the value is an integer (representing hour)
            hours.append(val)
        else: # Otherwise, it's the user's first and last name
            first_name = val[0]
            last_name = val[1]

    counter = Counter(hours)
    # Get the most common hour (the one that appears the most)
    occurances = counter.most_common(1)[0]

    # Return a tuple with the user's first name, last name, the most common hour, and its count
    return (first_name, last_name, occurances[0], occurances[1])

if __name__ == '__main__':
    useSmallInput = True
    if len(sys.argv) > 1 and "false" in sys.argv[1].lower():
        useSmallInput = False
    
    files = ['playhistory', 'people']
    inputs = read_files([f'data/{file}{SMALL_INPUT_NAME if useSmallInput else ""}.csv' for file in files])
    
    inputs = [line for (_, line) in inputs]

    map_reduce = MapReduce(mapper, reducer, 8)
    
    results = map_reduce(inputs, chunksize=16, debug=True)

    for result in results:
        print(result)
