#!/usr/bin/env python
from pyspark import SparkContext, RDD
from pyspark.sql  import SparkSession
from readCSVFile import readCSVFile

def getStongest(beerRDD):
	"""Extract the brewery which produces the stongest beer product
	@type beerRDD:	RDD
	"""
	maxAbv = beerRDD.max(lambda x: x.beer_abv)
	return maxAbv


if __name__ == "__main__":
	fileName = "/Users/wtchen/Research/beerAdvocate/beer_reviews/beer_reviews.csv"
	sc = SparkContext()
	spark = SparkSession(sc)
	df = readCSVFile(fileName, spark, numOfPartition = 4)
	rdd = df.rdd

	maxAbv = getStongest(rdd)
	print "The brewery provides the strongest beer: %s" % maxAbv.brewery_name.encode('utf-8').strip()
