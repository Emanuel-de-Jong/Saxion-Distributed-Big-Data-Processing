import re
import os
import string
from map_reduce_lib import *

def mapper(input):
    line_number, text= input[1].split('\t', 1)
    text = text.translate(str.maketrans('', '', string.punctuation)).replace('\t', ' ')
    
    if not text: return
    
    text = re.sub(r'\s+', ' ', text)
    results = text.lower().split(' ')
    return [(result, f"{input[0].replace('data/', '')}@{line_number}") for result in results if result]

def reducer(key_values):
    result = ','.join(key_values[1])    
    return (key_values[0], result)

if __name__ == '__main__':
  
    files = os.listdir('data')
    files = ['data/' + file for file in files]
    
    input = read_files(files)
    
    map_reduce = MapReduce(mapper, reducer, 8)
    
    results = map_reduce(input, chunksize=16, debug=True)

    with open('result.txt', 'w') as f:
        for result in results:
            f.write(f"{result[0]}   {result[1]}\n")
