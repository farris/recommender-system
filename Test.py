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
from pyspark.ml.feature import StringIndexer

def main(spark, sc):
    sc.setLogLevel("ERROR")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    ratings = sc.textFile('kaggle.txt') \
            .map(lambda x:x.split("\t"))\
            .map(lambda x:(str(x[0]), str(x[1]), int(x[2])))

    schemaString = "user_str track_id count"

    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)

    # Apply the schema to the RDD.
    schemaRatings = spark.createDataFrame(ratings, schema)
    
    schemaRatings.createOrReplaceTempView("ratings")

    indexer_user = StringIndexer(inputCol="user_str", outputCol="user_id")
    indexed = indexer_user.fit(schemaRatings).transform(schemaRatings)
    
    indexer_track = StringIndexer(inputCol="track_id", outputCol="trackId")
    indexed = indexer_track.fit(indexed).transform(indexed) 
    
    print("Indexed-----------------------------------------------------------------------------------")
    indexed.show()
    
    
    indexed.createOrReplaceTempView("ratings_idx")
    # SQL can be run over DataFrames that have been registered as a table.
    results = spark.sql("SELECT user_str, track_id, count, CAST(user_id AS INT) AS userId , CAST(trackId AS INT) AS trackId FROM ratings_idx")
    print("Results-----------------------------------------------------------------------------------")
    results.show()  

    results.createOrReplaceTempView("final")
    cleaned = spark.sql("SELECT userId, trackId ,count FROM final")
    print("Cleaned-----------------------------------------------------------------------------------")
    cleaned.show() 
    
    train_rdd = cleaned.rdd.map(tuple)
    print("Train_RDD-----------------------------------------------------------------------------------")
    print(train_rdd.take(10))
    
    from pyspark.mllib.recommendation import ALS
    model=ALS.trainImplicit(train_rdd, rank=5, iterations=3, alpha=0.99)

#     print(model)
    
    
    
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('Test').getOrCreate()
    sc = spark.sparkContext
    # Call our main routine
    main(spark, sc)