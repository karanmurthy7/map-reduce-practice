import MapReduce
import sys

"""
Map Reduce program to join two tables Orders and LineItem
based on the criteria Order.order_id = LineItem.order_id
"""

mr = MapReduce.MapReduce()


def mapper(record):
    # order_id: primary key of the order table
    # record: record contents
    order_id = record[1]
    mr.emit_intermediate(order_id, record)

def reducer(key, list_of_values):
    # key: order_id
    # value: list of records with the same key in Orders and LineItem table
    order = []
    line_item = []
    for value in list_of_values:
        if value[0] == "order":
            order = value
        else:
            line_item = value
        if order != [] and line_item != []:
            # Emiting joined records.
            mr.emit((order + line_item))
        
    

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
