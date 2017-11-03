import MapReduce
import sys

"""
Map reduce Program to remove the last 10 characters from each 
string of nucleotides, then remove any duplicates generated.
"""

mr = MapReduce.MapReduce()

def mapper(record):
    # key: sequence_id
    # value: friend_name
    nucleotides = record[1]
    nucleotides_trimmed = nucleotides[:-10]
    mr.emit_intermediate(nucleotides_trimmed, nucleotides_trimmed)

def reducer(key, list_of_values):
    # key: unique_trims
    mr.emit((key))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
