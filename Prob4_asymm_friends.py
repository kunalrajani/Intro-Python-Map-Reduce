import MapReduce
import sys

"""
Asymmetric Friend Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
	# key: Person id
	# value: Person's friend id
	key = record[0]
	value = record[1]
#	pair = []
	if(key<value):
#		pair.append(key)
#		pair.append(value)
		pair = key + "," + value
	else:
#		pair.append(value)
#		pair.append(key)
		pair=value + "," + key
#	print pair
	mr.emit_intermediate(pair, 1)

def reducer(key, list_of_values):
	# key: person,friend pair
	# value: list of relationship counts
	total = 0
	for v in list_of_values:
	  total += v
	key = key.split(",")
#	print key
	if(total==1):
		mr.emit((key[0],key[1]))
		mr.emit((key[1],key[0]))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
