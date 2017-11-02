import MapReduce
import sys

"""
Map Reduce program to give the document indexes for each word.
In other words, for each word, print all the documents that contain
the word.
"""

mr = MapReduce.MapReduce()


def mapper(record):
    # document_id: document identifier
    # value: document contents
    document_id = record[0]
    value = record[1]
    words = value.split()
    for w in words:
      mr.emit_intermediate(w, document_id)

def reducer(key, list_of_values):
    # key: word
    # value: list of document identifiers
    document_ids = []
    for v in list_of_values:
        if v not in document_ids:
            document_ids.append(v)
    mr.emit((key, document_ids))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
