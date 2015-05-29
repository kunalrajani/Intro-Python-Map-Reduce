import MapReduce
import sys

"""
Matrix Multiplication Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
	# key: element identifier
	# value: value of matrix element
	mat = record[0]
	i = record[1]
	j = record[2]
	val = record[3]
	for k in range(11):
		if (mat=="a"):
			mr.emit_intermediate((i,k),(j,val))
		else:
			mr.emit_intermediate((k,j),(i,val))	# I would have had a separate for loop for this if the no. of rows in A was different than no. of cols in B
	
def reducer(key, list_of_values):
	# key: element identifier
	# value: list of element values
	idx = []
	num = []
	for val in list_of_values:
		idx.append(val[0])
		num.append(val[1])
	sum = 0;
	for i in range(11):
		val1 = 0
		val2 = 0
		if (idx.count(i)>0):
			val1 = num.pop(idx.index(i))
			idx.pop(idx.index(i))
		if (idx.count(i)>0):
			val2 = num.pop(idx.index(i))
			idx.pop(idx.index(i))
		sum = sum + val1*val2
	mr.emit((key[0],key[1],sum))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
