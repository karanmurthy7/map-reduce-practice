import MapReduce
import sys

"""
Map reduce algorithm to compute the matrix multiplication A x B 
"""

mr = MapReduce.MapReduce()

matrix_size = 5

def mapper(record):
    matrix, row, column, value = record
    if matrix == "a":
        for n in range(matrix_size):
            mr.emit_intermediate((row, n), record)
    else:
        # In case matrix is b
        for n in range(matrix_size):
            mr.emit_intermediate((n, column), record)

def reducer(key, list_of_values):
    # key: tuple containing (rowId, columnId)
    # value: records from both matrix A and B as per the 
    # condition in the mapper
    matrix_A = []
    matrix_B = []
    for v in list_of_values:
        if "a" in v:
            matrix_A.append(v)
        else:
            matrix_B.append(v)
            
    output = 0
    row_index = 2
    column_index = 1
    value_index = 3
    
    for a in matrix_A:
        for b in matrix_B:
            if a[row_index] == b[column_index]:
                output += a[value_index] * b[value_index]
                    
    mr.emit((key[0], key[1], output))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
