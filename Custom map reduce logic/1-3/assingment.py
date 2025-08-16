from map_reduce_lib import *
from datetime import datetime
import re

regex_pattern = re.compile(r'(\d+\.\d+\.\d+\.\d+) - - \[\d+/(\w+)/')

def mapper(input):
    match = regex_pattern.search(input)
    if not match:
        return
    
    ip_address = match.group(1)
    month_str = match.group(2)
    month_int = datetime.strptime(month_str, "%b").month

    # print(f'{ip_address},{month_int}')
    return [(month_int, ip_address)]
    
def reducer(key_values):
    ips_by_count = {}
    for ip in key_values[1]:
        if ip not in ips_by_count:
            ips_by_count[ip] = 0
        ips_by_count[ip] += 1
    
    month = key_values[0]
    return (month, ips_by_count)

if __name__ == '__main__':
    inputs = read_files(["data/access_log"])
    inputs = [line for (_, line) in inputs]

    map_reduce = MapReduce(mapper, reducer, 16)
    
    results = map_reduce(inputs, chunksize=16, debug=True)

    with open('result.txt', 'w') as f:
        for result in results:
            month = result[0]
            f.write(f'Month {month}\n')
            
            ips_by_count = result[1]
            for ip in ips_by_count:
                f.write(f'\t{ip}: {ips_by_count[ip]}\n')
