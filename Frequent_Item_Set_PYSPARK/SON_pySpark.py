import pyspark
from pyspark import SparkContext
import os
import sys
from collections import defaultdict
import time

def get_Key_Val_Tuple(line, fID):
	line = line.split('::')
	if fID == 0:
		return int(line[0]), line[1]
	else:
		return int(line[0]), int(line[1])

def count_freqOLD(candidates,baskets,support):
	item_counts = defaultdict(lambda: 0)
	freqItems = set()
	for itemset in candidates:
		for basket in baskets:
			if itemset.issubset(basket):
				item_counts[itemset] += 1
	for item, count in item_counts.iteritems():
		if count > support:
			freqItems.update(frozenset([item]))
	return freqItems


def aPrioriold(partition, tot_baskets_count,support):

	num_items_in_partition = 0
	item_counts = defaultdict(lambda: 0)
	freq_singletons = set()
	baskets = set()
	for transaction in partition:
		baskets.update({frozenset(transaction)})
		num_items_in_partition +=1
		for item in set(transaction):
			item_counts[item] += 1

	support_partition = support*num_items_in_partition/tot_baskets_count
	for item, count in item_counts.iteritems():
		if count > support_partition:
			freq_singletons.update({item})

	setSize = 2
	freq_Item_Sets = set([frozenset([item]) for item in freq_singletons])
	actual_freq_items = set({1})
	x = True
	#while actual_freq_items != set([]):
	#	candidates = [set_a.union(set_b) for set_a in freq_Item_Sets for set_b in freq_Item_Sets if len(set_a.union(set_b)) == setSize]
	#	actual_freq_items = count_freq(candidates,baskets,support_partition)
	#	freq_Item_Sets = actual_freq_items
	#	setSize += 1
	#	x = True

	
	yield support_partition

#def aPriori(partition, tot_baskets_count,support):

def aPriori(partition, support):
	baskets = list()
	freqSingletons = set()
	item_counts = defaultdict(lambda: 0)
	for basket in partition:
		baskets.append(set(basket))
		for item in basket:
			item_counts[item] += 1
	for item, count in  item_counts.iteritems():
		if count >= support:
			freqSingletons.add(frozenset([item]))
	frequent_item_sets = get_freq_k_sets(baskets, freqSingletons, support)
	return frequent_item_sets

def get_freq_k_sets(baskets, freqSingletons, support):

	k = 1
	freqitemDict = {k: freqSingletons}
	
	loop = True
	tmpFrq = freqSingletons
	while tmpFrq:
		k += 1
		tmpFrq = set()
		actualCount = defaultdict(int)

		candidates = set([setA.union(setB) for setA in freqitemDict[k-1] for setB in freqitemDict[k-1] if len(setA.union(setB)) == k])
		for basket in baskets:
			for subset in candidates:
				if subset.issubset(basket):
					actualCount[subset] += 1
		for item, count in actualCount.items():
			if count >= support:
				tmpFrq.add(item)
		if tmpFrq != set([]):
			freqitemDict[k] = tmpFrq


	return [item for subset in freqitemDict.values() for item in subset] 

def count_candidates(basket, candidates):
	item_counts = defaultdict(int)
	for candidate in candidates:
		if candidate.issubset(basket):
			item_counts[candidate] += 1

	return item_counts.items()




def main():
	caseNum = int(sys.argv[1])
	ratingsFileLoc = sys.argv[2]
	usersFileLoc = sys.argv[3]
	support = int(sys.argv[4])

	sc = SparkContext('local','SON')

	if os.path.exists(ratingsFileLoc):
		ratingsFile = sc.textFile(ratingsFileLoc)
	if os.path.exists(usersFileLoc):
		usersFile = sc.textFile(usersFileLoc)

	uIDmvIDpair = ratingsFile.map(lambda line: get_Key_Val_Tuple(line,1))
	uIDgenderpair = usersFile.map(lambda line: get_Key_Val_Tuple(line,0))
	joined = uIDmvIDpair.join(uIDgenderpair)

	if caseNum == 1:
		baskets = joined.filter(lambda e: e[1][1] == 'M').map(lambda e: (e[0], e[1][0])).distinct().groupByKey().map(lambda e: list(e[1]))
	if caseNum == 2:
		baskets = joined.filter(lambda e: e[1][1] == 'F').map(lambda e: (e[1][0], e[0])).distinct().groupByKey().map(lambda e: list(e[1]))

	numPartition = baskets.getNumPartitions()
	candidatefreqItemSets = baskets.mapPartitions(lambda partition: aPriori(partition,support/numPartition)).map(lambda e: (e,1)).reduceByKey(lambda acc, e: acc).map(lambda e: e[0]).collect()
	freqItemSets = baskets.flatMap(lambda basket: count_candidates(basket, candidatefreqItemSets)).reduceByKey(lambda acc,e: acc + e).filter(lambda e: e[1] >= support).map(lambda e: list(e[0])).collect()
	
	listDict = defaultdict(list)
	for lst in freqItemSets:
		listDict[len(lst)] += [sorted(lst)]
		results = open('case'+str(caseNum)+'_'+str(support)+'.txt', 'w')
	for i, lst in listDict.items():
		outputline = ""
		for l in sorted(lst):
			string = "("
			for item in l:
				string += str(item) + ","
			string = string[:-1] + ")"
			#print(string)
			outputline += string + ','

		results.write(outputline[:-1] + '\n')

	results.close()









if __name__ == '__main__':
	start_time = time.time()
	main()
	print("Total time passed in seconds: %s seconds" % (time.time() - start_time))



