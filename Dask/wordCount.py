import dask.bag as db
import dask.dataframe as dd
from collections import defaultdict

def word_counts(line):
	word_count = defaultdict(int)
	words = line.split()
	for word in words:
		word_count[word] += 1
	return word_count

def merger(seq):
	word_count = defaultdict(int)
	for x in seq:
		for k,v in x.items():
			word_coount[k] += v
	return word_count

def topk(d):
	items = sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:10]
	return items

if _name_ == '_main_':
	# change file to "Acts" to see parallel execution
	mybag = db.read_text("~/DASK/data/Acts")
	# mybag = db.read_text("Acts")
	print(mybag.map(word_counts).map(topk).compute())
	# mybag3 = mybag.map(word_counts).map(topk).compute()
	# print(topk(mybag.map(word_counts).reduction(merger,merger).compute()))
	# mydf = mybag.map(word_counts).to_dataframe()
	# print(topk(mybag3))