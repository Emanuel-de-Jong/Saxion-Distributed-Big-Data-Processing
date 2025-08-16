import sys
from datetime import datetime
from map_reduce_lib import *

SMALL_INPUT_NAME = "_small"

def mapper(input):
    # Unpack the input tuple
    line, month, year = input

    # Skip the header line in the CSV file
    if "track_id,user,datetime" in line:
        return

    # Split the line by commas to extract the values
    track_id, _, date = line.split(",")

    # Convert the date string into a datetime object
    datetime_obj = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")

    # If the record is not from the desired month and year, skip it
    if datetime_obj.month != month or datetime_obj.year != year:
        return
    
    # Return a key-value pair where track_id is the key, and 1 is the value (count of occurrences)
    return [(track_id, 1)]

def reducer(key_values):
    track_id, occurrences = key_values
    # Return the track_id with the total count of occurrences
    return (track_id, sum(occurrences))

if __name__ == '__main__':
    # Flag to control whether to use small input files or not
    useSmallInput = True
    if len(sys.argv) > 1 and "false" in sys.argv[1].lower():
        # If the first argument is 'false', set useSmallInput to False
        useSmallInput = False
    
    files = ['playhistory']
    inputs = read_files([f'data/{file}{SMALL_INPUT_NAME if useSmallInput else ""}.csv' for file in files])

    # Define the month (August) and year (2017) for filtering the data
    month = 8
    year = 2017
    inputs = [(line, month, year) for (_, line) in inputs]

    map_reduce = MapReduce(mapper, reducer, 8)
    
    results = map_reduce(inputs, chunksize=16, debug=True)

    for result in results:
        print(result)
