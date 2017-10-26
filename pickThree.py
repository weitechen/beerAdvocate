#!/usr/bin/env python
from pyspark import SparkContext, RDD
from pyspark.sql  import SparkSession
from readCSVFile import readCSVFile

def pickThree(beerRDD):
	""" Pick three beers to recommand
	@type beerRDD:	RDD
	@rtype:	list
	"""
	topThreeBeer = beerRDD \
		.map(lambda x: (x.beer_name, (x.review_overall, 1))) \
		.reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1])) \
		.filter(lambda x: x[1][1] > 1) \
		.mapValues(lambda x: ((x[0] + 0.0) / x[1])) \
		.top(3, key = lambda x: x[1])
	return topThreeBeer

if __name__ == "__main__":
	fileName = "/Users/wtchen/Research/beerAdvocate/beer_reviews/beer_reviews.csv"
	sc = SparkContext()
	spark = SparkSession(sc)
	df = readCSVFile(fileName, spark, numOfPartition = 4)
	rdd = df.rdd

	topThreeBeer = pickThree(rdd)
	print "The three beers I recommend are: ",
	print topThreeBeer
