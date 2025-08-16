import collections
from termcolor import cprint, colored
import itertools, random
import multiprocessing
import operator

class MapReduce(object):
    """MapReduce Framework v1.1"""
    
    def __init__(self, map_func, reduce_func, num_workers=None):
        """
        map_func

          Function to map inputs to intermediate data. Takes as
          argument one input value and returns a tuple with the key
          and a value to be reduced.
        
        reduce_func

          Function to reduce partitioned version of intermediate data
          to final output. Takes as argument a key as produced by
          map_func and a sequence of the values associated with that
          key.
         
        num_workers

          The number of workers to create in the pool. Defaults to the
          number of CPUs available on the current host.
        """
        self.map_func = map_func
        self.reduce_func = reduce_func
        self.pool = multiprocessing.Pool(num_workers)
        self.num_workers = num_workers
    
    def partition(self, mapped_values):
        """Organize the mapped values by their key.
        Returns an unsorted sequence of tuples with a key and a sequence of values.
        """
        partitioned_data = collections.defaultdict(list)
        for key, value in mapped_values:
            partitioned_data[key].append(value)
        return partitioned_data.items()
    
    def __call__(self, inputs, chunksize=1, debug=False):
        """Process the inputs through the map and reduce functions given.
        
        inputs
          An iterable containing the input data to be processed.
        
        chunksize=1
          The portion of the input data to hand to each worker.  This
          can be used to tune performance during the mapping phase.
        """
        if debug:
          cprint('=== Mapping to %d mappers with chunk size %d... ===' % (self.num_workers, chunksize), 'red', attrs=['bold'])
        
        # Map and partition
        map_responses = self.pool.map(self.map_func, inputs, chunksize=chunksize)
        random.shuffle(map_responses)
        map_responses = filter(None, map_responses)
        partitioned_data = self.partition(itertools.chain(*map_responses))
        
        if debug:
          cprint('=== Mapper returned %d keys ===' % len(partitioned_data), 'red', attrs=['bold'])
          cprint('=== Reducing using %d reducers... ===' % self.num_workers, 'red', attrs=['bold'])

        # Reduce
        reduced_values = self.pool.map(self.reduce_func, partitioned_data)

        if debug:
          cprint('=== Reducer finished ===', 'red', attrs=['bold'])

        reduced_values.sort(key=operator.itemgetter(0))
        return reduced_values

def process_print(*args):
  """Print debug information annotated with the name of the current node"""
  print(colored('[' + multiprocessing.current_process().name + ']', 'yellow'), *args)

def read_files(filenames):
  """Read data from multiple files and return the lines in key/value format"""
  file_contents = []

  for filename in filenames:
    with open(filename, 'r', encoding='utf-8') as input_file:
      lines = input_file.read().splitlines()
      for line in lines:
        file_contents.append((filename, line))
  
  return file_contents
