#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Starter Pyspark Script for students to complete for their Lab 3 Assignment.
Usage:
    $ spark-submit lab_3_starter_code.py <student_netID>
'''

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession
from pyspark import SparkContext,  SparkConf
from pyspark.sql.types import *

def main(spark, sc):
    counts = spark.read.load("examples/src/main/resources/people.csv",
                     format="csv", sep=";", inferSchema="true", header="true")
    
    
    ratings = sc.textFile('kaggle.txt') \
            .map(lambda x:x.split("\t"))\
            .map(lambda x:(str(x[0]), str(x[1]), int(x[2])))

    schemaString = "user_id count track_id"

    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)

    # Apply the schema to the RDD.
    schemaRatings = spark.createDataFrame(ratings, schema)
    
    schemaRatings.createOrReplaceTempView("ratings")

    # SQL can be run over DataFrames that have been registered as a table.
    results = spark.sql("SELECT * FROM ratings")

    results.show()
    
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('Test').getOrCreate()
    sc = spark.sparkContext
    # Call our main routine
    main(spark, sc)