import MapReduce
import sys

"""
Map reduce that prints the number of friends that 
each person has. Friendship is considered a one way
relationship in this case.
"""

mr = MapReduce.MapReduce()

def mapper(record):
    # key: person_name
    # value: friend_name
    person = record[0]
    friend = record[1]
    mr.emit_intermediate(person, 1)

def reducer(key, list_of_values):
    # key: person_name
    # value: list of 1's for every friend that
    # the person has
    total_friends = 0
    for v in list_of_values:
      total_friends += v
    mr.emit((key, total_friends))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
