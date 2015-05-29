import MapReduce
import sys

"""
Friends Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
	# key: A person id
	# value: Another person id who is a friend of the person in "key"
	key = record[0]
	value = record[1]
	if(key<value):
		pair = [key,value]
	else:
		pair = [value,key]
	mr.emit_intermediate(pair, 1)

def reducer(key, list_of_values):
    # key: One person id
    # value: Number of friends of that person
    total = 0
    for v in list_of_values:
      total += v
	if(v==1):
		mr.emit(key[0],key[1])
		mr.emit(key[1],key[0])

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
