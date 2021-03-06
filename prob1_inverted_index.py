import MapReduce
import sys

"""
Inverted index Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
	# key: document identifier
	# value: document contents
	key = record[0]
	value = record[1]
	words = value.split()
	words = set(words)
	for w in words:
	  mr.emit_intermediate(w, key)

def reducer(key, list_of_values):
	# key: word
	# value: list of document identifiers
	docs = []
	for v in list_of_values:
	  docs.append(v)
	mr.emit((key, docs))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
