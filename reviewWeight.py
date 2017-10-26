#!/usr/bin/env python
from pyspark import SparkContext, RDD
from pyspark.sql  import SparkSession
from readCSVFile import readCSVFile

def reviewWeight(beerRDD, sc):
	""" Determine the factor for each review parameters
	@type beerRDD:	RDD
	@type sc:		SparkContext
	@rtype: list
	"""
	#rWeight = [sc.accumulator(0.25) for _ in xrange(4)]
	rWeight = [0.25 for _ in xrange(4)]
	totalError = sc.accumulator(0.0)
	"""
	y^{bar} = w0 * r0 + w1 * r1 + w2 * r2 + w3 * r3
	y^{gold}

	J(theta) = (y^{bar} - y^{gold})^2
	J(theta) / d(w0) = 2*(y^{bar} - y^{gold})*-1*w0
	"""
	learningRate = 0.01
	numOfEpoch = 10
	instanceCnt = beerRDD.count()

	# partite RDD
	partitionRDD = beerRDD \
		.map(lambda x:  (x.review_overall, x.review_aroma, x.review_appearance, x.review_palate, x.review_taste)) \
		.repartition(4).cache()

	def computingLoss(batchPartition):
		""" Compute loss and gradients for each instance
		@type: batchPartition:	
		"""
		error = 0.0
		gradient = [0.0 for _ in xrange(4)]
		batchSize = 0
		for x in batchPartition:
			y_bar = sum([x[idx+1]*(rWeight[idx]) for idx in xrange(4)])
			loss = (x[0] - y_bar)
			error = loss**2
			gradient = [loss*x[idx] for idx in xrange(1,5)]
			yield tuple(gradient) + (error, )

	for epoch in xrange(numOfEpoch):
		lossRDD = partitionRDD \
			.mapPartitions(computingLoss, preservesPartitioning = True)

		# sum over all errors
		totalError = lossRDD \
			.mapPartitions(lambda xList: ( x[4] for x in xList)).sum() / (2*instanceCnt)

		# get delta gradients for each weight parameters
		deltaGradient = lossRDD \
			.mapPartitions(lambda xList: ( (x[0], x[1], x[2], x[3]) for x in xList)) \
			.reduce(lambda x, y: tuple([x[idx] + y[idx] for idx in xrange(4)]))

		# update weights
		for wIdx in xrange(4):
			rWeight[wIdx] += learningRate*deltaGradient[wIdx]/instanceCnt

		print "Epoch: %d, errors = %f" % (epoch, totalError)
		print "new rWeight: ",
		print rWeight
	
	return rWeight

if __name__ == "__main__":
	fileName = "/Users/wtchen/Research/beerAdvocate/beer_reviews/beer_reviews.csv"
	sc = SparkContext()
	spark = SparkSession(sc)
	df = readCSVFile(fileName, spark, numOfPartition = 4)
	rdd = df.rdd

	weight = reviewWeight(rdd, sc)
	print "The weights for the four review rates are: ",
	print weight
