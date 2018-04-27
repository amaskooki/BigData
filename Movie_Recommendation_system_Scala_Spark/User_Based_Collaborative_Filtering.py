import pyspark
from pyspark import SparkContext
import time, sys, os, collections
import numpy as np


def get_userID_movieID_rate(entry, fID):
	entry = entry.split(',')
	if entry[0] == 'userId':
		pass
	elif fID == 1:
		return ((int(entry[0]), int(entry[1])), float(entry[2]))
	elif fID == 2:
		return ((int(entry[0]), int(entry[1])), None)

def get_items(line):
	return line[0][1]

def normalize(a):
	avg = float(sum([el for el in a if el != 0]))/sum([1 for el in a if el != 0])
	return [el-avg if el != 0 else 0 for el in a]
	
def get_corr(a,b):
	if a == None or b == None:
		return 0

	num = float(sum([a[i]*b[i] for i in range(len(a))]))
	denom = np.sqrt(sum([el**2 for el in a]))*np.sqrt(sum([el**2 for el in b]))
	if denom != 0:
		return num/denom
	else:
		return 0



def dict_corr(dict1,dict2):

	shared_keys = list( set(dict1.keys()) & set(dict2.keys()) )
	a = [dict1[k] for k in shared_keys]
	b = [dict2[k] for k in shared_keys]
	if a == None or b == None:
		return 0
	else:
		a = normalize(a)
		b = normalize(b)

		return get_corr(a,b)
	 

def get_correlation( partition, trainingDict, trainingDict_userKey, NN):
	
	#NN_dict = collections.defaultdict()
	#corrl_dict = collections.defaultdict(float)
	NN_list = list()
	test = []
	for entry in partition:
		cr_list = []
		predict_user = entry[0][0]
		predict_movie = entry[0][1]
		predict_item_vector = trainingDict[predict_movie]

		for item in trainingDict_userKey[predict_user]:
			if item != predict_movie:
				cr = dict_corr(predict_item_vector,trainingDict[item])
				if cr > 0.85:
					#corrl_dict[item] = cr
					cr_list.append((item,cr))


		#NN_dict[item] = dict(sorted(corrl_dict.iteritems(), key=lambda (k,v): (v,k), reverse = True)[:NN])
		cr_list = sorted( cr_list, key = lambda tup: tup[1] )[:NN]
		NN_list.append( (predict_movie, cr_list) )
		#corrl_dict.clear()		
	#test.append(NN_dict) 
	return NN_list


def predict_rating(partition, global_mean_rating, user_mean_rating_difference, item_mean_rating_difference, trainingDict_itemKey, NN_dict):
	default = 0
	prediction = []
	outlier_map = {'0-':0, '5+':5}
	for entry in partition:
		predict_user = entry[0][0]
		predict_movie = entry[0][1]
		#test.append((item_mean_rating_difference.get(predict_movie, default),user_mean_rating_difference.get(predict_user, default)))
		predicted_rating = global_mean_rating + item_mean_rating_difference.get(predict_movie, 
			default) + user_mean_rating_difference.get(predict_user, default)

		num = 0
		denom = 0
		for NN_movie, corr in NN_dict[predict_movie]:
			NN_rating = trainingDict_itemKey[NN_movie].get(predict_user, None)
		 	if corr and NN_rating:
		 		num += corr*(NN_rating - global_mean_rating + item_mean_rating_difference.get(NN_movie, 
		 			default) + user_mean_rating_difference.get(predict_user, default))
		 		denom += corr
		if denom != 0:
			predicted_rating += num/denom
		if predicted_rating > 5 :
			predicted_rating = 5
		elif predicted_rating < 0 :
			predicted_rating = 0

		prediction.append( ((predict_user, predict_movie), predicted_rating) )
	return prediction

	


def main():
	sc = SparkContext('local','CF')
	trainingSetFileLoc = sys.argv[-2]
	if os.path.exists(trainingSetFileLoc):
		trainingSet = sc.textFile(trainingSetFileLoc)
	testingSetFileLoc = sys.argv[-1]
	if os.path.exists(testingSetFileLoc):
		testingSet = sc.textFile(testingSetFileLoc)
	
	

	trainingSet = trainingSet.map(lambda line: get_userID_movieID_rate(line, 1)).filter(lambda entry: entry != None)
	testingSet = testingSet.map(lambda line: get_userID_movieID_rate(line, 2)).filter(lambda entry: entry != None)
	
	all_movie_list = list(set(trainingSet.map(lambda entry: entry[0][1]).collect()))
	all_user_list = list(set(trainingSet.map(lambda entry: entry[0][0]).collect()))

	prediction_movie_list = list(set(testingSet.map(lambda entry: entry[0][1]).collect()))
	
	trainingSet = trainingSet.subtract(testingSet)
	
	global_mean_rating = trainingSet.values().mean()

	user_mean_rating_difference = trainingSet.map(lambda ((userId,movieID), rate): (userId,rate-global_mean_rating)).combineByKey(lambda rate: (rate, 1),
	 	lambda sum, value: (sum[0] + value, sum[1] + 1),
	 	lambda x, y: (x[0] + y[0], x[1] + y[1])).map(lambda (key, (acc,count)): (key, acc/count)).collectAsMap()

	item_mean_rating_difference = trainingSet.map(lambda ((userId,movieID), rate): (movieID,rate-global_mean_rating)).combineByKey(lambda rate: (rate, 1),
	 	lambda sum, value: (sum[0] + value, sum[1] + 1),
	 	lambda x, y: (x[0] + y[0], x[1] + y[1])).map(lambda (key, (acc,count)): (key, acc/count)).collectAsMap()


	#user_as_key = trainingSet.map(lambda ((userId,movieID), rate): (userId,rate))

	trainingDict_itemKey = trainingSet.map(lambda ((userId,movieID), rate): 
		(movieID,(userId,rate)) ).filter(lambda (movieID,(userId,rate)): rate != None).groupByKey().map(lambda x: (x[0], dict(x[1]) )).collectAsMap()
	trainingDict_userKey = trainingSet.filter(lambda ((u,m), rate): 
		rate != None).map(lambda ((userId,movieID), rate): (userId,movieID) ).groupByKey().map(lambda x: (x[0], list(x[1]) )).collectAsMap()
	#print(trainingDict_itemKey.items())
	#print(user_as_key.take(10))
	#print(item_as_key.take(10))
	NN = 10
	NN_dict = testingSet.mapPartitions(lambda partition: get_correlation(partition, trainingDict_itemKey, 
		trainingDict_userKey, NN)).collectAsMap()
	
	predicted_rating = testingSet.mapPartitions(lambda partition: predict_rating(partition, global_mean_rating, 
		user_mean_rating_difference, item_mean_rating_difference, trainingDict_itemKey, NN_dict)).collectAsMap()

	testing_user_movie = testingSet.keys().collect()

	err = trainingSet.filter(lambda entry: entry[0] in testing_user_movie).map(lambda entry: (entry[0], 
		(entry[1]-predicted_rating[entry[0]]) ))
	

	rmse = err.map(lambda x: x[1]**2).mean()
	
	#print(np.sqrt(rmse))
	
	abserr = err.map(lambda x: (int(abs(x[1])), 1) ).reduceByKey(lambda x,y: x+y).collectAsMap()

	try:
		for i in range(0,4):
			if i < 4:
				print(">=" + str(i) + " and <"+ str(i+1) + ": " + str(abserr[i]) + '\n')
		sum = 0
		for i in range(4,len(abserr)):
			sum += abserr[i]

		print(">=" + str(4) + ": " + str(sum))
	except:
		pass
	
	print('RMSE= {}'.format(rmse))
	
	x = [(k[0], k[1], v) for k, v in predicted_rating.items()]
	x = sorted(x, key= lambda k: k[0])

	f = open('results.txt', 'w')
	f.write("UserID,MovieID,Pred_rating\n")
	[f.write('{},{},{}\n'.format(st[0],st[1],st[2])) for st in x]  # python will convert \n to os.linesep
	f.close()

	




	

























	

if __name__ == '__main__':
	start_time = time.time()
	main()
	print("\n Total time passed in seconds: %s seconds" % (time.time() - start_time))