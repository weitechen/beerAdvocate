#!/usr/bin/env python
from pyspark import SparkContext, RDD
from pyspark.sql  import SparkSession
from readCSVFile import readCSVFile

def personalPreference(beerRDD, preferenceList):
	"""Pick the highest rate according to the average score of preference
	@type beerRDD:	RDD
	@type preferenceList:	list of float
	@rtype:	list of tuple
	"""
	preferenceBeer = beerRDD \
		.map(lambda x: (x.beer_style, (x.review_aroma, x.review_appearance,x.review_palate,x.review_taste))) \
		.mapValues(lambda x: (sum(( x[idx]*preferenceList[idx] for idx in xrange(4) )), 1)) \
		.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
		.mapValues(lambda x: ((x[0] + 0.0) / x[1])) \
		.top(1, key = lambda x: x[1])

	return preferenceBeer

def personalPreferenceByReviewer(beerRDD, preferenceList):
	"""
	@type beerRDD:	RDD
	@type preferenceList:	list of float
	@rtype:	list of tuple
	"""
	numOfEpoch = 10
	learningRate = 0.01
	reviewWeight = beerRDD.map(lambda x: (x.review_profilename, 1)).reduceByKey(sum).mapValues(lambda x: (0.25, 0.25, 0.25, 0.25, x))
	for epoch in xrange(numOfEpoch):
		deltaRDD = beerRDD \
			.map(lambda x: (x.review_profilename, (x.review_overall, x.review_aroma, x.review_appearance,x.review_palate,x.review_taste))) \
			.join(reviewWeight) \
			.mapValues(lambda x: (x[0][0], sum(( x[0][idx+1]*x[1][idx] for idx in xrange(4))), x[1][:4] )) \
			.mapValues(lambda x: (x[0] - x[1], x[2])) \
			.mapValues(lambda x: tuple([ x[0] * x[1][idx] for idx in xrange(4) ])) \
			.reduceByKey(lambda x, y: tuple([ x[idx] + y[idx] for idx in xrange(4)])) \
			.join(reviewWeight) \
			.mapValues(lambda x: tuple([ x[0][idx] / x[1][4] for idx in xrange(4)]))

		reviewWeight = reviewWeight \
			.join(deltaRDD) \
			.mapValues(lambda x: tuple([x[0][idx] + learningRate*x[1][idx] for idx in xrange(4)]))
	
	print reviewWeight.collect()

if __name__ == "__main__":
	fileName = "/Users/wtchen/Research/beerAdvocate/beer_reviews/beer_reviews.csv"
	sc = SparkContext()
	spark = SparkSession(sc)
	df = readCSVFile(fileName, spark, numOfPartition = 4)
	rdd = df.rdd

	#perferenceStyle = personalPreference(rdd, [1.0,1.0,0.0,0.0])
	#print u"My Recommend style: {0}".format(perferenceStyle[0][0])

	personalPreferenceByReviewer(rdd, [1.0,1.0,0.0,0.0])
