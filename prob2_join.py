import MapReduce
import sys

"""
Matrix Join Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
	# key: Matrix row identifier
	# value: matrix content
	key = record[1]
	value = record
	mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
	# key: matrix row identifier
	# value: list of matrix contents
	table1 = []
	table2 = []
	for v in list_of_values:
		if (v[0] == "order"):
			table1.append(v)
		else:
			table2.append(v)
	for rec1 in table1:
		for rec2 in table2:
			mr.emit(rec1+rec2)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
