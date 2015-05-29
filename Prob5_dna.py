import MapReduce
import sys

"""
DNA Sequence Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
	# key: Sequence id
	# value: Nucleotide string
	key = record[1]
	mr.emit_intermediate(key[0:(len(key)-10)], 1)

def reducer(key, list_of_values):
	# key: word
	# value: list of occurrence counts
	mr.emit((key))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
