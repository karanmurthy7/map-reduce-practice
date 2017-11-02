import MapReduce
import sys

"""
Map reduce that prints the list of 
of all non-symmetric friend relationships.
The output should be all pairs (friend, person) 
such that (person, friend) appears in the dataset 
but (friend, person) does not.
"""

mr = MapReduce.MapReduce()

def mapper(record):
    # key: person_name
    # value: friend_name
    person = record[0]
    friend = record[1]
    mr.emit_intermediate(person, friend)
    mr.emit_intermediate(friend, person)

def reducer(person, list_of_friends):
    # key: person_name
    # value: list containing friends
    # the person has

    for friend in list_of_friends:
      if (list_of_friends.count(friend) < 2):
          mr.emit((person, friend))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
