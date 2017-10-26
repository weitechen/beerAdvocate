#!/usr/bin/env python
from pyspark import SparkContext, RDD
from pyspark.sql  import DataFrame, SparkSession
from itertools import izip, count
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import FloatType, LongType, StringType, TimestampType


def readCSVFile(fileName, spark, numOfPartition = 1):
	""" Read the beerAdvocateReview CSV file to DataFrame
	@type fileName:	str
	@type spark:	SparkSession
	@type numOfPartition:	int
	@rtype:	DataFrame
	"""
	schema = StructType([
		StructField("brewery_id", LongType()), StructField("brewery_name", StringType()), StructField("review_time", LongType()), StructField("review_overall", FloatType()), StructField("review_aroma", FloatType()), StructField("review_appearance", FloatType()), StructField("review_profilename", StringType()), StructField("beer_style", StringType()), StructField("review_palate", FloatType()), StructField("review_taste", FloatType()), StructField("beer_name", StringType()), StructField("beer_abv", FloatType()), StructField("beer_beerid", LongType()) ])

	with open(fileName, "r") as fhd:
		#trainInstance = self.sc.pdrallelize( [ (trainIdx, (amr, depTree, DepPathContainer(depTree))) for (trainIdx, (amr, depTree)) in enumerate(zip(self.amrReader.amrRepository, self.depReader.depTreeRepository))], self.numOfPartition)
		df = spark.read.csv(fileName, header=True, schema=schema)

	return df
